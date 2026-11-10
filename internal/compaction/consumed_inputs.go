package compaction

import (
	"bytes"
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/apache/arrow-go/v18/parquet/metadata"
	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/klauspost/compress/zstd"
)

// consumedInputsKey is the Parquet footer key of an hourly output that lists
// the raw files whose rows it holds: every raw input its job read, plus the
// lists carried by the outputs it re-merged. The hourly scan uses it to delete
// a raw file an output already holds (an ingest stager re-PUTs a key whose
// first PUT committed while the client saw an error) instead of merging it
// with whichever output has room, which removes the copy only when that is the
// output holding the rows. The daily tier does not carry the list: nothing
// reads it there.
const consumedInputsKey = "arc:consumed_inputs"

// consumedInputsVersion prefixes the value: base64 of a zstd frame holding one
// "key\tsize\thash\n" line per input, sorted.
const consumedInputsVersion = "v1:"

// maxConsumedInputs bounds the list an output carries. It costs about 48 bytes
// of footer per input (73 KB for an hour of 1,500 stager files, about 970 KB
// at this bound); past it the output carries no list and the hour falls back
// to merging every raw file.
const maxConsumedInputs = 20000

// consumedInputHashBytes is how much of the SHA-256 of an input the list keeps.
// The key and size already match for the same stager upload; the hash proves
// the bytes are the same.
const consumedInputHashBytes = 8

// consumedInputMaxVerifyBytes bounds the raw file the scan downloads to check
// its hash. A larger one is merged as before.
const consumedInputMaxVerifyBytes = 256 << 20

// footerTailRead is the first range read of an output's footer; a footer that
// does not fit takes a second read of exactly its size.
const footerTailRead = 256 << 10

var errBadParquetFooter = errors.New("not a readable parquet footer")

type consumedInput struct {
	Key  string
	Size int64
	Hash string
}

// isHourlyOutput reports whether key is an hourly tier output (or a DELETE
// rewrite of one).
func isHourlyOutput(key string) bool {
	return strings.HasSuffix(storage.StripRewriteSuffix(key), "_compacted.parquet")
}

func encodeConsumedInputs(in []consumedInput) (string, error) {
	var b strings.Builder
	for _, c := range in {
		b.WriteString(c.Key)
		b.WriteByte('\t')
		b.WriteString(strconv.FormatInt(c.Size, 10))
		b.WriteByte('\t')
		b.WriteString(c.Hash)
		b.WriteByte('\n')
	}
	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedBetterCompression), zstd.WithEncoderConcurrency(1))
	if err != nil {
		return "", err
	}
	defer enc.Close()
	return consumedInputsVersion + base64.StdEncoding.EncodeToString(enc.EncodeAll([]byte(b.String()), nil)), nil
}

func decodeConsumedInputs(v string) ([]consumedInput, error) {
	payload, ok := strings.CutPrefix(v, consumedInputsVersion)
	if !ok {
		return nil, fmt.Errorf("%s: unknown version", consumedInputsKey)
	}
	frame, err := base64.StdEncoding.DecodeString(payload)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", consumedInputsKey, err)
	}
	dec, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1), zstd.WithDecoderMaxMemory(64<<20))
	if err != nil {
		return nil, err
	}
	defer dec.Close()
	text, err := dec.DecodeAll(frame, nil)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", consumedInputsKey, err)
	}
	var out []consumedInput
	for line := range strings.SplitSeq(strings.TrimSuffix(string(text), "\n"), "\n") {
		if line == "" {
			continue
		}
		key, rest, ok1 := strings.Cut(line, "\t")
		size, hash, ok2 := strings.Cut(rest, "\t")
		n, err := strconv.ParseInt(size, 10, 64)
		if !ok1 || !ok2 || err != nil || key == "" || hash == "" {
			return nil, fmt.Errorf("%s: malformed entry %q", consumedInputsKey, line)
		}
		out = append(out, consumedInput{Key: key, Size: n, Hash: hash})
	}
	return out, nil
}

// normalizeConsumedInputs sorts the list and removes repeats; a key named
// twice with the same size and hash (an output and the re-PUT it was merged
// with) is one entry.
func normalizeConsumedInputs(in []consumedInput) []consumedInput {
	slices.SortFunc(in, func(a, b consumedInput) int {
		return cmp.Or(strings.Compare(a.Key, b.Key), cmp.Compare(a.Size, b.Size), strings.Compare(a.Hash, b.Hash))
	})
	return slices.Compact(in)
}

func hashPrefix(r io.Reader) (string, error) {
	h := sha256.New()
	if _, err := io.Copy(h, r); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)[:consumedInputHashBytes]), nil
}

func hashLocalFile(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	return hashPrefix(f)
}

// parquetFooterValue returns the footer key-value entry key of a Parquet file
// of size bytes. readFrom returns the file's bytes from an offset to its end.
func parquetFooterValue(size int64, readFrom func(offset int64) ([]byte, error), key string) (value string, found bool, err error) {
	if size < 12 {
		return "", false, errBadParquetFooter
	}
	n := min(size, footerTailRead)
	tail, err := readFrom(size - n)
	if err != nil {
		return "", false, err
	}
	if int64(len(tail)) != n || !bytes.Equal(tail[n-4:], []byte("PAR1")) {
		return "", false, errBadParquetFooter
	}
	footerLen := int64(binary.LittleEndian.Uint32(tail[n-8 : n-4]))
	if footerLen+12 > size {
		return "", false, errBadParquetFooter
	}
	if footerLen+8 > n {
		if tail, err = readFrom(size - footerLen - 8); err != nil {
			return "", false, err
		}
		if int64(len(tail)) != footerLen+8 {
			return "", false, errBadParquetFooter
		}
	}
	footer := tail[int64(len(tail))-footerLen-8 : int64(len(tail))-8]
	return footerKeyValue(footer, key)
}

func footerKeyValue(footer []byte, key string) (value string, found bool, err error) {
	defer func() {
		if r := recover(); r != nil {
			value, found, err = "", false, fmt.Errorf("%w: %v", errBadParquetFooter, r)
		}
	}()
	md, err := metadata.NewFileMetaData(footer, nil)
	if err != nil {
		return "", false, fmt.Errorf("%w: %v", errBadParquetFooter, err)
	}
	v := md.KeyValueMetadata().FindValue(key)
	if v == nil {
		return "", false, nil
	}
	return *v, true, nil
}

func localParquetFooterValue(path, key string) (string, bool, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", false, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return "", false, err
	}
	return parquetFooterValue(info.Size(), func(off int64) ([]byte, error) {
		buf := make([]byte, info.Size()-off)
		_, err := f.ReadAt(buf, off)
		return buf, err
	}, key)
}

// consumedInputsMetadata is the arc:consumed_inputs value of an hourly output
// made from files: each raw input with its size and hash, and the lists of the
// outputs among the inputs. "" means no list (other tiers, too many entries,
// an encoding error); the output then reads like one from an older build.
// An input that cannot be hashed, or an output whose list cannot be read, is
// left out, which only means its rows are merged rather than dropped later.
func (j *Job) consumedInputsMetadata(files []downloadedFile) string {
	if j.Tier != "hourly" {
		return ""
	}
	var in []consumedInput
	for _, f := range files {
		if isHourlyOutput(f.storageKey) {
			v, ok, err := localParquetFooterValue(f.localPath, consumedInputsKey)
			if err == nil && ok {
				var list []consumedInput
				if list, err = decodeConsumedInputs(v); err == nil {
					in = append(in, list...)
				}
			}
			if err != nil {
				j.logger.Warn().Err(err).Str("file", f.storageKey).Msg("Could not read the consumed-input list of a re-merged output; the new output will not list its inputs")
			}
			continue
		}
		if strings.ContainsAny(f.storageKey, "\t\n") {
			continue
		}
		h, err := hashLocalFile(f.localPath)
		if err != nil {
			j.logger.Warn().Err(err).Str("file", f.storageKey).Msg("Could not hash a compaction input; the output will not list it")
			continue
		}
		in = append(in, consumedInput{Key: f.storageKey, Size: f.size, Hash: h})
	}
	in = normalizeConsumedInputs(in)
	if len(in) == 0 {
		return ""
	}
	if len(in) > maxConsumedInputs {
		j.logger.Warn().Int("inputs", len(in)).Int("max", maxConsumedInputs).Msg("Too many consumed inputs to list in the output footer; re-PUT raw files of this hour will be merged, not dropped")
		return ""
	}
	v, err := encodeConsumedInputs(in)
	if err != nil {
		j.logger.Warn().Err(err).Msg("Could not encode the consumed-input list; the output will not carry it")
		return ""
	}
	return v
}

// consumedInputsCache holds the arc:consumed_inputs value of outputs by key
// ("" for an output without one). Outputs are immutable, so an entry never
// goes stale; the oldest entries go first past consumedInputsCacheEntries or
// consumedInputsCacheBytes.
type consumedInputsCache struct {
	mu    sync.Mutex
	byKey map[string]string
	order []string
	bytes int
}

const (
	consumedInputsCacheEntries = 4096
	consumedInputsCacheBytes   = 32 << 20
)

func (c *consumedInputsCache) get(key string) (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	v, ok := c.byKey[key]
	return v, ok
}

func (c *consumedInputsCache) put(key, v string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.byKey == nil {
		c.byKey = make(map[string]string)
	}
	if _, ok := c.byKey[key]; ok {
		return
	}
	c.byKey[key] = v
	c.order = append(c.order, key)
	c.bytes += len(key) + len(v)
	for len(c.order) > consumedInputsCacheEntries || c.bytes > consumedInputsCacheBytes {
		old := c.order[0]
		c.bytes -= len(old) + len(c.byKey[old])
		delete(c.byKey, old)
		c.order = c.order[1:]
	}
}

// consumedInputDropper is a tier whose outputs list their consumed inputs and
// whose scan may drop a raw file an output already holds.
type consumedInputDropper interface {
	dropsConsumedInputs() bool
}

// consumedInputsOf returns the list output carries, reading its footer with
// range reads on a cache miss. nil, nil = no list (an older build's output).
func (m *Manager) consumedInputsOf(ctx context.Context, output string, size int64) ([]consumedInput, error) {
	v, ok := m.consumedInputs.get(output)
	if !ok {
		if size < 0 {
			var err error
			if size, err = m.StorageBackend.StatFile(ctx, output); err != nil {
				return nil, err
			}
			if size < 0 {
				return nil, nil
			}
		}
		val, found, err := parquetFooterValue(size, func(off int64) ([]byte, error) {
			var buf bytes.Buffer
			err := m.StorageBackend.ReadToAt(ctx, output, &buf, off)
			return buf.Bytes(), err
		}, consumedInputsKey)
		switch {
		case errors.Is(err, errBadParquetFooter):
			// Permanent for an immutable object: never look again.
			m.logger.Debug().Err(err).Str("output", output).Msg("Output footer unreadable; its raw files are merged, not dropped")
			val = ""
		case err != nil:
			return nil, err
		case !found:
			val = ""
		}
		m.consumedInputs.put(output, val)
		v = val
	}
	if v == "" {
		return nil, nil
	}
	return decodeConsumedInputs(v)
}

// dropConsumedInputs deletes the raw files of an hourly candidate that an
// output of the same candidate already holds: the output lists the key with
// the same size, and the file's bytes hash to the listed value. Such a file is
// a re-PUT of an input a job already merged and deleted; merging it again
// removes the copy only if it lands in the batch of the output holding its
// rows, which with several outputs per hour it usually does not.
//
// It never deletes a file no output provably holds: an output without a list
// (older builds), an unreadable footer, a size or hash that differs, or any
// error leaves the file to be merged as before. It runs after the manifest,
// pending-registration and sync filters, so neither the file nor the output is
// part of a compaction in flight.
//
// The second result is false when the candidate should not run this tick: a
// delete failed (the next tick lists the hour again), or what remains no longer
// needs compacting.
func (m *Manager) dropConsumedInputs(ctx context.Context, tier Tier, c Candidate) (Candidate, bool) {
	if d, ok := tier.(consumedInputDropper); !ok || !d.dropsConsumedInputs() {
		return c, true
	}
	sizeOf := func(i int) int64 {
		if len(c.FileSizes) == len(c.Files) {
			return c.FileSizes[i]
		}
		return -1
	}
	var outputs, raws []int
	for i, f := range c.Files {
		if tier.IsCompactedFile(f) {
			outputs = append(outputs, i)
		} else {
			raws = append(raws, i)
		}
	}
	if len(outputs) == 0 || len(raws) == 0 {
		return c, true
	}

	listed := make(map[string][]consumedInput)
	heldBy := make(map[consumedInput]string)
	for _, i := range outputs {
		list, err := m.consumedInputsOf(ctx, c.Files[i], sizeOf(i))
		if err != nil {
			if ctx.Err() == nil {
				m.logger.Warn().Err(err).Str("output", c.Files[i]).Msg("Could not read an output's consumed-input list; its raw files are merged, not dropped")
			}
			continue
		}
		for _, in := range list {
			listed[in.Key] = append(listed[in.Key], in)
			heldBy[in] = c.Files[i]
		}
	}
	if len(listed) == 0 {
		return c, true
	}

	type drop struct {
		in     consumedInput
		output string
	}
	var drops []drop
	for _, i := range raws {
		key := c.Files[i]
		entries := listed[key]
		if len(entries) == 0 {
			continue
		}
		size := sizeOf(i)
		if size < 0 {
			var err error
			if size, err = m.StorageBackend.StatFile(ctx, key); err != nil || size < 0 {
				continue
			}
		}
		if !slices.ContainsFunc(entries, func(e consumedInput) bool { return e.Size == size }) {
			m.logger.Info().Str("partition", c.PartitionPath).Str("file", key).Int64("size", size).
				Msg("Raw file has the key of an input an output holds but another size; merging it")
			continue
		}
		if size > consumedInputMaxVerifyBytes {
			continue
		}
		var buf bytes.Buffer
		if err := m.StorageBackend.ReadTo(ctx, key, &buf); err != nil {
			if ctx.Err() == nil {
				m.logger.Warn().Err(err).Str("file", key).Msg("Could not read a raw file an output lists; merging it")
			}
			continue
		}
		if int64(buf.Len()) != size {
			continue
		}
		h, _ := hashPrefix(&buf)
		in := consumedInput{Key: key, Size: size, Hash: h}
		output, ok := heldBy[in]
		if !ok {
			m.logger.Info().Str("partition", c.PartitionPath).Str("file", key).
				Msg("Raw file has the key and size of an input an output holds but other bytes; merging it")
			continue
		}
		drops = append(drops, drop{in: in, output: output})
	}
	if len(drops) == 0 {
		return c, true
	}

	keys := make([]string, len(drops))
	for i, d := range drops {
		keys[i] = d.in.Key
	}
	// The edge-sync hub marks the receipts first, as for any consumed input;
	// a failed mark keeps the files, which are then merged as before.
	if err := m.notifyConsumedInputs(keys); err != nil {
		m.logger.Warn().Err(err).Str("partition", c.PartitionPath).Strs("files", keys).
			Msg("Could not mark raw files an output holds as consumed; merging them instead")
		return c, true
	}
	dropped := make(map[string]bool, len(drops))
	for _, d := range drops {
		if err := m.StorageBackend.Delete(ctx, d.in.Key); err != nil {
			m.logger.Warn().Err(err).Str("partition", c.PartitionPath).Str("file", d.in.Key).
				Msg("Could not delete a raw file an output holds; the hour waits for the next tick")
			return c, false
		}
		dropped[d.in.Key] = true
		metrics.Get().IncCompactionConsumedInputsDropped(1)
		m.logger.Info().
			Str("partition", c.PartitionPath).
			Str("file", d.in.Key).
			Int64("size", d.in.Size).
			Str("output", d.output).
			Msg("Deleted a raw file whose rows an hourly output already holds (same key, size and hash): a re-uploaded input")
	}
	m.mu.Lock()
	onComplete := m.onCompactionComplete
	m.mu.Unlock()
	if onComplete != nil {
		onComplete()
	}

	// The drop is this pass's progress: it ends a forced re-merge back-off,
	// and what remains is judged afresh on the next tick.
	reportForcedRemerge(tier, c, true)
	sized := len(c.FileSizes) == len(c.Files)
	kept := c
	kept.ForcedRemerge = false
	kept.Files = make([]string, 0, len(c.Files)-len(dropped))
	kept.FileSizes = nil
	for i, f := range c.Files {
		if dropped[f] {
			continue
		}
		kept.Files = append(kept.Files, f)
		if sized {
			kept.FileSizes = append(kept.FileSizes, c.FileSizes[i])
		}
	}
	kept.FileCount = len(kept.Files)
	return kept, tier.ShouldCompact(kept.Files, kept.PartitionTime)
}
