package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

func zerologNopForTest() zerolog.Logger { return zerolog.Nop() }

// ── unit tests for the small helpers ──────────────────────────────────────

func TestNormalizeRollupMode(t *testing.T) {
	cases := []struct {
		in      string
		want    string
		wantErr bool
	}{
		{"", "auto", false},
		{"auto", "auto", false},
		{"AUTO", "auto", false},
		{"  auto  ", "auto", false},
		{"off", "off", false},
		{"OFF", "off", false},
		{"disable", "off", false},
		{"disabled", "off", false},
		{"skip", "off", false},
		{"required", "required", false},
		{"REQUIRE", "required", false},
		{"on", "", true},           // unknown
		{"true", "", true},         // unknown
		{"sometimes", "", true},    // unknown
		{"rollup=off", "", true},   // hint-style not accepted
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			got, err := normalizeRollupMode(tc.in)
			if (err != nil) != tc.wantErr {
				t.Fatalf("err=%v wantErr=%v", err, tc.wantErr)
			}
			if got != tc.want {
				t.Fatalf("got %q want %q", got, tc.want)
			}
		})
	}
}

// ── builder + context plumbing ────────────────────────────────────────────

func TestRollupBuilder_RecordAcceptedAndRefused(t *testing.T) {
	b := &rollupOutcomeBuilder{}
	if b.written {
		t.Fatal("new builder must not be written")
	}
	b.recordAccepted()
	if !b.accepted || !b.written {
		t.Fatalf("after accept: accepted=%v written=%v", b.accepted, b.written)
	}
	// Re-record as refused overrides.
	b.recordRefused("nope")
	if b.accepted || b.reason != "nope" {
		t.Fatalf("after refuse: accepted=%v reason=%q", b.accepted, b.reason)
	}
}

func TestRollupBuilder_NilSafe(t *testing.T) {
	var b *rollupOutcomeBuilder
	// Both methods are no-ops on nil — guards in tryTieredRewrite call sites.
	b.recordAccepted()
	b.recordRefused("x")
}

func TestRollupCtx_DefaultsToAuto(t *testing.T) {
	if got := rollupModeFromCtx(context.Background()); got != "auto" {
		t.Fatalf("default mode = %q, want auto", got)
	}
}

func TestRollupCtx_RoundTrip(t *testing.T) {
	for _, mode := range []string{"off", "required", "auto"} {
		ctx := withRollupMode(context.Background(), mode)
		if got := rollupModeFromCtx(ctx); got != mode {
			t.Fatalf("mode roundtrip: got %q want %q", got, mode)
		}
	}
}

func TestRollupBuilderCtx_RoundTrip(t *testing.T) {
	b := &rollupOutcomeBuilder{}
	ctx := withRollupOutcomeBuilder(context.Background(), b)
	got := rollupOutcomeBuilderFromCtx(ctx)
	if got != b {
		t.Fatalf("builder roundtrip failed")
	}
}

func TestRollupBuilderCtx_AbsentReturnsNil(t *testing.T) {
	if got := rollupOutcomeBuilderFromCtx(context.Background()); got != nil {
		t.Fatalf("expected nil builder when absent, got %v", got)
	}
}

// ── attachRollup + setRollupHeaders ──────────────────────────────────────

func TestAttachRollup_NoBuilderNoChange(t *testing.T) {
	resp := QueryResponse{Success: true}
	out := attachRollup(resp, nil, "auto")
	if out.Rollup != nil {
		t.Fatalf("expected no Rollup when builder is nil")
	}
}

func TestAttachRollup_UnwrittenBuilderNoChange(t *testing.T) {
	resp := QueryResponse{Success: true}
	out := attachRollup(resp, &rollupOutcomeBuilder{}, "auto")
	if out.Rollup != nil {
		t.Fatalf("expected no Rollup when builder is unwritten")
	}
}

func TestAttachRollup_PopulatesAcceptedAndMode(t *testing.T) {
	b := &rollupOutcomeBuilder{}
	b.recordAccepted()
	out := attachRollup(QueryResponse{Success: true}, b, "auto")
	if out.Rollup == nil {
		t.Fatal("expected Rollup populated")
	}
	if !out.Rollup.Accepted || out.Rollup.Mode != "auto" {
		t.Fatalf("got %+v", out.Rollup)
	}
}

func TestAttachRollup_PopulatesRefusedReason(t *testing.T) {
	b := &rollupOutcomeBuilder{}
	b.recordRefused("sub-hour bucket")
	out := attachRollup(QueryResponse{}, b, "required")
	if out.Rollup == nil || out.Rollup.Accepted || out.Rollup.Reason != "sub-hour bucket" || out.Rollup.Mode != "required" {
		t.Fatalf("got %+v", out.Rollup)
	}
}

// ── QueryRequest JSON parsing ─────────────────────────────────────────────

func TestQueryRequest_JSONOmitsOptionalFields(t *testing.T) {
	// Existing clients only send {"sql": "..."} — they must still parse.
	body := `{"sql": "SELECT 1"}`
	var req QueryRequest
	if err := json.Unmarshal([]byte(body), &req); err != nil {
		t.Fatal(err)
	}
	if req.SQL != "SELECT 1" {
		t.Errorf("SQL=%q", req.SQL)
	}
	if req.Rollup != "" {
		t.Errorf("Rollup should default to empty string, got %q", req.Rollup)
	}
}

func TestQueryRequest_JSONParsesRollupField(t *testing.T) {
	body := `{"sql": "SELECT 1", "rollup": "required"}`
	var req QueryRequest
	if err := json.Unmarshal([]byte(body), &req); err != nil {
		t.Fatal(err)
	}
	if req.Rollup != "required" {
		t.Errorf("Rollup=%q", req.Rollup)
	}
}

// ── headers ───────────────────────────────────────────────────────────────

func TestSetRollupHeaders_NoBuilderNoOp(t *testing.T) {
	resp := runHeaderRoute(t, func(c *fiber.Ctx) error {
		setRollupHeaders(c, nil, "auto")
		return c.SendStatus(200)
	})
	if got := resp.Header.Get("X-Arc-Rollup-Accepted"); got != "" {
		t.Fatalf("unexpected header: %q", got)
	}
}

func TestSetRollupHeaders_AcceptedSetsTrue(t *testing.T) {
	resp := runHeaderRoute(t, func(c *fiber.Ctx) error {
		b := &rollupOutcomeBuilder{}
		b.recordAccepted()
		setRollupHeaders(c, b, "auto")
		return c.SendStatus(200)
	})
	if got := resp.Header.Get("X-Arc-Rollup-Accepted"); got != "true" {
		t.Errorf("X-Arc-Rollup-Accepted=%q want true", got)
	}
	if got := resp.Header.Get("X-Arc-Rollup-Mode"); got != "auto" {
		t.Errorf("X-Arc-Rollup-Mode=%q", got)
	}
}

func TestSetRollupHeaders_RefusedIncludesReason(t *testing.T) {
	resp := runHeaderRoute(t, func(c *fiber.Ctx) error {
		b := &rollupOutcomeBuilder{}
		b.recordRefused("unsupported bucket")
		setRollupHeaders(c, b, "required")
		return c.SendStatus(200)
	})
	if got := resp.Header.Get("X-Arc-Rollup-Accepted"); got != "false" {
		t.Errorf("Accepted=%q want false", got)
	}
	if !strings.Contains(resp.Header.Get("X-Arc-Rollup-Reason"), "unsupported bucket") {
		t.Errorf("Reason header missing: %q", resp.Header.Get("X-Arc-Rollup-Reason"))
	}
}

// ── tryTieredRewrite respects rollupModeFromCtx + builder ────────────────

// rollup=off via context must short-circuit before any table lookup.
func TestTryTieredRewrite_OffSkipsRouter(t *testing.T) {
	h := newRollupCtrlHandler(t)
	b := &rollupOutcomeBuilder{}
	ctx := withRollupOutcomeBuilder(withRollupMode(context.Background(), "off"), b)
	got, ok := h.tryTieredRewrite(ctx, "SELECT * FROM events", "default")
	if ok {
		t.Errorf("rollup=off must refuse rewrite; got ok=true (%q)", got)
	}
	if got != "SELECT * FROM events" {
		t.Errorf("rollup=off must return SQL unchanged; got %q", got)
	}
	if !b.written || b.accepted || !strings.Contains(b.reason, "rollup=off") {
		t.Errorf("builder should record refused with reason 'rollup=off …', got %+v", b)
	}
}

// rollup=auto + no deps → builder records the no-deps reason (so explain can surface it).
func TestTryTieredRewrite_AutoNoDepsRecordsReason(t *testing.T) {
	h := newRollupCtrlHandler(t)
	b := &rollupOutcomeBuilder{}
	ctx := withRollupOutcomeBuilder(context.Background(), b) // mode defaults to "auto"
	_, ok := h.tryTieredRewrite(ctx, "SELECT * FROM events", "default")
	if ok {
		t.Errorf("no deps registered → expected refuse")
	}
	if !b.written || b.accepted {
		t.Errorf("builder should record refused; got %+v", b)
	}
	if !strings.Contains(b.reason, "no tiered deps") {
		t.Errorf("expected no-deps reason; got %q", b.reason)
	}
}

// Without a builder on context, the function still works (no panic, no record).
func TestTryTieredRewrite_NoBuilderNoCrash(t *testing.T) {
	h := newRollupCtrlHandler(t)
	ctx := withRollupMode(context.Background(), "off")
	_, ok := h.tryTieredRewrite(ctx, "SELECT * FROM events", "default")
	if ok {
		t.Errorf("rollup=off must refuse")
	}
	// No assertion on the builder — there isn't one. If the function panics
	// (e.g., dereferences a nil builder unguarded), this test crashes.
}

// Same code path, mode=required, no deps — the builder records refusal so
// the caller (executeQuery) can return the 400 with a reason.
func TestTryTieredRewrite_RequiredRecordsRefusalReason(t *testing.T) {
	h := newRollupCtrlHandler(t)
	b := &rollupOutcomeBuilder{}
	ctx := withRollupOutcomeBuilder(withRollupMode(context.Background(), "required"), b)
	_, ok := h.tryTieredRewrite(ctx, "SELECT * FROM events", "default")
	if ok {
		t.Errorf("required+no-deps must refuse")
	}
	if !b.written || b.accepted {
		t.Errorf("builder should record refused; got %+v", b)
	}
}

func newRollupCtrlHandler(t *testing.T) *QueryHandler {
	t.Helper()
	return &QueryHandler{logger: zerologNopForTest()}
}

// ── end-to-end: executeQuery returns 400 for invalid rollup mode ─────────

func TestExecuteQuery_InvalidRollupMode_400(t *testing.T) {
	h := newRollupCtrlHandler(t)
	app := fiber.New()
	app.Post("/api/v1/query", h.executeQuery)

	body := `{"sql": "SELECT 1", "rollup": "yes-please"}`
	req := httptest.NewRequest("POST", "/api/v1/query", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := app.Test(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 400 {
		t.Fatalf("status=%d want 400", resp.StatusCode)
	}
	var got QueryResponse
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatal(err)
	}
	if got.Success {
		t.Errorf("expected success=false")
	}
	if !strings.Contains(got.Error, "invalid rollup mode") {
		t.Errorf("expected error to mention invalid rollup mode; got %q", got.Error)
	}
}

// rollup=required + table that has no tieredDeps → 400 with explicit reason.
// This is the killer feature: dashboards that MUST hit rollup turn timeouts
// into immediate, debuggable errors.
func TestExecuteQuery_Required_NoDeps_Returns400WithReason(t *testing.T) {
	h := newRollupCtrlHandler(t) // no SetTieredDeps → no deps for any table
	app := fiber.New()
	app.Post("/api/v1/query", h.executeQuery)

	body := `{"sql": "SELECT date_trunc('hour', time) FROM events GROUP BY 1", "rollup": "required"}`
	req := httptest.NewRequest("POST", "/api/v1/query", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := app.Test(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 400 {
		t.Fatalf("status=%d want 400", resp.StatusCode)
	}
	var got QueryResponse
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(got.Error, "rollup required but router refused") {
		t.Errorf("expected refusal error; got %q", got.Error)
	}
	// Response should include Rollup outcome so the caller knows why.
	if got.Rollup == nil {
		t.Fatal("expected Rollup field on refused-required response")
	}
	if got.Rollup.Accepted {
		t.Errorf("Rollup.Accepted should be false")
	}
	if got.Rollup.Mode != "required" {
		t.Errorf("Rollup.Mode=%q want required", got.Rollup.Mode)
	}
	if !strings.Contains(got.Rollup.Reason, "no tiered deps") {
		t.Errorf("Reason should explain why; got %q", got.Rollup.Reason)
	}
}

// runHeaderRoute wires up a single GET / route that runs `h`, then returns
// the response so the test can assert on headers.
func runHeaderRoute(t *testing.T, h fiber.Handler) *http.Response {
	t.Helper()
	app := fiber.New()
	app.Get("/", h)
	r := httptest.NewRequest("GET", "/", nil)
	resp, err := app.Test(r)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}
