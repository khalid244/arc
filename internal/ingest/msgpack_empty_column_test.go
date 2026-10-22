package ingest

import (
	"context"
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

// A MessagePack write with an empty column name (hammel-posthog-api sends one
// for a PostHog property keyed "$") goes through decode, buffer and flush the
// way the write handler sends it: the rows are stored, the empty-named column
// is not. Before unstoredColumn, the flush indexed name[0] and the panic took
// the ingest process down. Two requests give the empty-named column different
// types, so the flush merges batches that disagree on it.
func TestMsgPackEmptyColumnNameIsSkippedAtFlush(t *testing.T) {
	for _, typed := range []bool{true, false} {
		dec := NewMessagePackDecoder(zerolog.Nop())
		dec.SetTypedDecodeEnabled(typed)
		r := newFlushRig(t, nil)
		for i, empty := range []interface{}{"x", int64(1)} {
			raw, err := msgpack.Marshal(map[string]interface{}{
				"m": "events",
				"columns": map[string]interface{}{
					"time":  []interface{}{int64(1700000000000 + i)},
					"event": []interface{}{string(rune('a' + i))},
					"":      []interface{}{empty},
				},
			})
			require.NoError(t, err)
			records, err := dec.Decode(raw)
			require.NoError(t, err)
			require.NoError(t, r.buf.Write(context.Background(), "posthog", records))
		}
		files := r.files(t)
		require.Len(t, files, 1, "typed=%v", typed)
		_, has := files[0].types[""]
		require.False(t, has, "typed=%v: the empty-named column was stored", typed)
		require.Equal(t, []interface{}{"a", "b"}, files[0].cols["event"], "typed=%v", typed)
	}
}
