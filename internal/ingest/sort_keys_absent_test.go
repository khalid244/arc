package ingest

import (
	"reflect"
	"testing"
)

// TestSortTypedColumnBatchByKeys_AbsentKeyStillSortsByTime is the regression for
// a measurement-level sort key ("events:event,time") meeting a flush that has no
// "event" column (another database's "events" table, or schema drift). The
// multi-key sort used to error on the missing column and sortTypedColumnBatchByKeys
// returned the batch untouched — so the file was not even time-ordered. The
// absent key must be skipped and the batch ordered by the remaining keys.
func TestSortTypedColumnBatchByKeys_AbsentKeyStillSortsByTime(t *testing.T) {
	batch := &TypedColumnBatch{
		Data: map[string]interface{}{
			"time":        []int64{30, 10, 20},
			"distinct_id": []string{"c", "a", "b"},
		},
		Signature: "distinct_id:s,time:i64",
	}
	sorted := sortTypedColumnBatchByKeys(batch, []string{"event", "time"})
	if got, want := sorted.Data["time"].([]int64), []int64{10, 20, 30}; !reflect.DeepEqual(got, want) {
		t.Fatalf("time = %v, want %v", got, want)
	}
	if got, want := sorted.Data["distinct_id"].([]string), []string{"a", "b", "c"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("distinct_id = %v, want %v (rows must move together)", got, want)
	}
}

// TestSortTypedColumnBatchByKeys_NullEventKeepsRowsAligned: a NULL event is ""
// in the data slice with a false validity bit. Sorting by (event, time) must move
// the validity bit with its row, so a NULL never turns into a real "" event (or
// the reverse) and every column stays row-aligned.
func TestSortTypedColumnBatchByKeys_NullEventKeepsRowsAligned(t *testing.T) {
	batch := &TypedColumnBatch{
		Data: map[string]interface{}{
			"time":        []int64{4, 3, 2, 1, 5},
			"event":       []string{"b", "", "a", "", "a"},
			"distinct_id": []string{"u4", "u3", "u2", "u1", "u5"},
		},
		Validity: map[string][]bool{
			"event": {true, false, true, false, true},
		},
		Signature: "distinct_id:s,event:s,time:i64",
	}
	sorted := sortTypedColumnBatchByKeys(batch, []string{"event", "time"})

	// NULLs ("" with validity false) sort first, ordered by time, then a, b.
	if got, want := sorted.Data["distinct_id"].([]string), []string{"u1", "u3", "u2", "u5", "u4"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("distinct_id = %v, want %v", got, want)
	}
	if got, want := sorted.Validity["event"], []bool{false, false, true, true, true}; !reflect.DeepEqual(got, want) {
		t.Fatalf("event validity = %v, want %v", got, want)
	}
	if got, want := sorted.Data["time"].([]int64), []int64{1, 3, 2, 5, 4}; !reflect.DeepEqual(got, want) {
		t.Fatalf("time = %v, want %v", got, want)
	}
}

func TestPresentSortKeys(t *testing.T) {
	cols := map[string]interface{}{"time": []int64{1}, "os_name": []string{"x"}}
	tests := []struct {
		keys []string
		want []string
	}{
		{[]string{"os_name", "time"}, []string{"os_name", "time"}},
		{[]string{"event", "time"}, []string{"time"}},
		{[]string{"os_name", "event", "time"}, []string{"os_name", "time"}},
		{[]string{"event"}, []string{}},
	}
	for _, tt := range tests {
		if got := presentSortKeys(cols, tt.keys); !reflect.DeepEqual(got, tt.want) {
			t.Errorf("presentSortKeys(%v) = %v, want %v", tt.keys, got, tt.want)
		}
	}
}
