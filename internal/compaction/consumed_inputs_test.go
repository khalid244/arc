package compaction

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestConsumedInputs_EncodeDecode(t *testing.T) {
	in := normalizeConsumedInputs([]consumedInput{
		{Key: "db/m/2026/11/10/05/b.parquet", Size: 2, Hash: "bb"},
		{Key: "db/m/2026/11/10/05/a.parquet", Size: 1, Hash: "aa"},
		{Key: "db/m/2026/11/10/05/a.parquet", Size: 1, Hash: "aa"},
	})
	if len(in) != 2 || in[0].Key != "db/m/2026/11/10/05/a.parquet" {
		t.Fatalf("normalized = %v, want two entries sorted by key", in)
	}
	v, err := encodeConsumedInputs(in)
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodeConsumedInputs(v)
	if err != nil || fmt.Sprint(got) != fmt.Sprint(in) {
		t.Fatalf("round trip = %v, %v; want %v", got, err, in)
	}
	for _, bad := range []string{"", "v2:" + strings.TrimPrefix(v, consumedInputsVersion), "v1:!!", "v1:" + "AAAA"} {
		if _, err := decodeConsumedInputs(bad); err == nil {
			t.Errorf("decode(%q) accepted a malformed value", bad)
		}
	}
}

func TestConsumedInputs_FooterOfNonParquet(t *testing.T) {
	for _, data := range [][]byte{
		[]byte("short"),
		[]byte(strings.Repeat("x", 100)),
		append([]byte(strings.Repeat("x", 100)), 0xff, 0xff, 0xff, 0x7f, 'P', 'A', 'R', '1'),
		append([]byte(strings.Repeat("x", 100)), 20, 0, 0, 0, 'P', 'A', 'R', '1'),
	} {
		_, found, err := parquetFooterValue(int64(len(data)), func(off int64) ([]byte, error) { return data[off:], nil }, consumedInputsKey)
		if found || !errors.Is(err, errBadParquetFooter) {
			t.Errorf("%q: found=%v err=%v, want errBadParquetFooter", data, found, err)
		}
	}
}

func TestConsumedInputs_CacheBounds(t *testing.T) {
	var c consumedInputsCache
	big := strings.Repeat("v", consumedInputsCacheBytes/4)
	for i := range 5 {
		c.put(fmt.Sprintf("o%d", i), big)
	}
	if _, ok := c.get("o0"); ok {
		t.Fatal("oldest entry kept past the byte bound")
	}
	if _, ok := c.get("o4"); !ok {
		t.Fatal("newest entry evicted")
	}
	if c.bytes > consumedInputsCacheBytes {
		t.Fatalf("cache holds %d bytes, bound %d", c.bytes, consumedInputsCacheBytes)
	}
	for i := range consumedInputsCacheEntries + 10 {
		c.put(fmt.Sprintf("s%d", i), "")
	}
	if len(c.byKey) != consumedInputsCacheEntries || len(c.order) != consumedInputsCacheEntries {
		t.Fatalf("cache holds %d/%d entries, bound %d", len(c.byKey), len(c.order), consumedInputsCacheEntries)
	}
}
