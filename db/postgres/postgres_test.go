package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"log-tailer-go/model"
)

func pgErr(code string) error {
	return fmt.Errorf("failed to connect: %w", &pgconn.PgError{Severity: "FATAL", Code: code, Message: "refused"})
}

func TestConnectFailure(t *testing.T) {
	cases := []struct {
		name   string
		err    error
		wantUp bool
	}{
		{"peer auth rejected", pgErr("28000"), true},
		{"too many connections", pgErr(codeTooManyConnections), true},
		{"database missing", pgErr("3D000"), true},
		{"starting up or shutting down", pgErr(codeCannotConnectNow), false},
		{"no socket", errors.New("dial unix /var/run/postgresql/.s.PGSQL.5432: connect: no such file or directory"), false},
		{"timed out", fmt.Errorf("connect: %w", context.DeadlineExceeded), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			up, text := ConnectFailure(tc.err)
			if up != tc.wantUp {
				t.Fatalf("expected up=%v, got %v", tc.wantUp, up)
			}
			if text == "" {
				t.Fatal("expected an error text")
			}
		})
	}
}

func TestIsTooManyConnections(t *testing.T) {
	if !IsTooManyConnections(pgErr(codeTooManyConnections)) {
		t.Fatal("expected 53300 to be too many connections")
	}
	if IsTooManyConnections(pgErr("28000")) || IsTooManyConnections(errors.New("dial error")) {
		t.Fatal("expected other errors not to be too many connections")
	}
}

func TestErrorText(t *testing.T) {
	if got := ErrorText(fmt.Errorf("query: %w", context.DeadlineExceeded)); got != errTimedOut {
		t.Fatalf("expected %q for the round's deadline, got %q", errTimedOut, got)
	}
	err := &pgconn.PgError{Severity: "ERROR", Code: "57014", Message: "canceling statement due to statement timeout"}
	if got := ErrorText(err); !strings.Contains(got, "statement timeout") {
		t.Fatalf("expected Postgres's own message, got %q", got)
	}
}

func i64(v int64) *int64 { return &v }

func TestParseCounters(t *testing.T) {
	reset := 1.7e9
	row := DatabaseRow{
		Commits: i64(10), Rollbacks: i64(2), Deadlocks: i64(1), TempFiles: i64(3),
		TempBytes: i64(4096), BlocksHit: i64(900), BlocksRead: i64(100), StatsReset: &reset,
	}
	got, ok := ParseCounters(row)
	want := Counters{Commits: 10, Rollbacks: 2, Deadlocks: 1, TempFiles: 3, TempBytes: 4096, BlocksHit: 900, BlocksRead: 100, StatsReset: reset}
	if !ok || got != want {
		t.Fatalf("expected %+v, got %+v (ok=%v)", want, got, ok)
	}

	row.StatsReset = nil
	if got, ok := ParseCounters(row); !ok || got.StatsReset != 0 {
		t.Fatalf("expected never-reset stats to read StatsReset 0, got %+v", got)
	}
}

func TestParseCounters_NoStatsEntry(t *testing.T) {
	if _, ok := ParseCounters(DatabaseRow{Name: "fresh"}); ok {
		t.Fatal("expected a database with no stats entry to have no counters")
	}
}

func sample(at time.Time, c Counters) Sample {
	return Sample{Counters: c, At: at, StartedAt: 1000}
}

func TestDiff(t *testing.T) {
	t0 := time.Unix(2000, 0)
	prev := sample(t0, Counters{Commits: 100, Rollbacks: 5, Deadlocks: 1, TempFiles: 2, TempBytes: 1000, BlocksHit: 900, BlocksRead: 100})
	cur := sample(t0.Add(time.Minute), Counters{Commits: 160, Rollbacks: 7, Deadlocks: 1, TempFiles: 3, TempBytes: 5000, BlocksHit: 1890, BlocksRead: 110})

	got, ok := Diff(prev, cur)
	want := Activity{Seconds: 60, Commits: 60, Rollbacks: 2, Deadlocks: 0, TempFiles: 1, TempBytes: 4000}
	if !ok || got != want {
		t.Fatalf("expected %+v, got %+v (ok=%v)", want, got, ok)
	}
}

func TestDiff_RefusesAcrossARestartOrReset(t *testing.T) {
	t0 := time.Unix(2000, 0)
	prev := sample(t0, Counters{Commits: 100})
	later := t0.Add(time.Minute)

	restarted := sample(later, Counters{Commits: 150})
	restarted.StartedAt = 1500
	if _, ok := Diff(prev, restarted); ok {
		t.Fatal("expected no activity across a Postgres restart")
	}

	reset := sample(later, Counters{Commits: 150, StatsReset: 1900})
	if _, ok := Diff(prev, reset); ok {
		t.Fatal("expected no activity across a stats reset")
	}

	backwards := sample(later, Counters{Commits: 40})
	if _, ok := Diff(prev, backwards); ok {
		t.Fatal("expected no activity when a counter went backwards")
	}

	if _, ok := Diff(prev, sample(t0, Counters{Commits: 150})); ok {
		t.Fatal("expected no activity over a zero-length window")
	}
}

func TestCacheHitPercent(t *testing.T) {
	got, ok := CacheHitPercent(Counters{BlocksHit: 900, BlocksRead: 100}, Counters{BlocksHit: 1890, BlocksRead: 110})
	if !ok || got != 99 {
		t.Fatalf("expected 99%% (990 hits of 1000 reads), got %f (ok=%v)", got, ok)
	}
	if _, ok := CacheHitPercent(Counters{BlocksHit: 5}, Counters{BlocksHit: 5}); ok {
		t.Fatal("expected no ratio when nothing was read")
	}
}

func TestUptimeSeconds(t *testing.T) {
	now := time.Unix(10_000, 0)
	if got := UptimeSeconds(6_399.75, now); got != 3600 {
		t.Fatalf("expected 3600 whole seconds, got %d", got)
	}
	if got := UptimeSeconds(20_000, now); got != 0 {
		t.Fatalf("expected a start time in the future to read 0, got %d", got)
	}
}

func TestFormatEpoch(t *testing.T) {
	if got := FormatEpoch(1_790_000_000.5); got != "2026-09-21T14:13:20Z" {
		t.Fatalf("expected RFC 3339 UTC, got %q", got)
	}
}

func TestQuote(t *testing.T) {
	if got := quote(`/run/it's\here`); got != `'/run/it\'s\\here'` {
		t.Fatalf("expected quotes and backslashes escaped, got %s", got)
	}
}

var writeWord = regexp.MustCompile(`(?i)\b(INSERT|UPDATE|DELETE|CREATE|ALTER|DROP|TRUNCATE|GRANT|REVOKE|VACUUM|ANALYZE|COPY|SET)\b`)

// Every query must only read; a write keyword in one is a bug even though
// the session is read-only anyway
func TestQueriesOnlyRead(t *testing.T) {
	queries := []string{qServer, qMaxConnections, qConnections, qLongRunning, qBlocked,
		qReplicas, qReplay, qDatabases, qSizes, qLargestTables, qDeadTuples}
	for _, q := range queries {
		first := strings.Fields(q)[0]
		if first != "SELECT" {
			t.Fatalf("expected every query to start with SELECT, got %q in:\n%s", first, q)
		}
		if w := writeWord.FindString(q); w != "" {
			t.Fatalf("expected no %s in a monitoring query:\n%s", w, q)
		}
	}
}

// pg_stat_activity's query column holds the raw SQL of every session, with
// its values; it must never be selected
func TestQueriesNeverReadQueryText(t *testing.T) {
	for _, q := range []string{qConnections, qLongRunning, qBlocked} {
		for _, field := range strings.FieldsFunc(q, func(r rune) bool {
			return r == ' ' || r == ',' || r == '\n' || r == '(' || r == ')'
		}) {
			if field == "query" {
				t.Fatalf("expected no query column selected in:\n%s", q)
			}
		}
	}
}

// A missing socket is Postgres down: the event still publishes, with up
// false, the error, and every list present but empty
func TestCollect_NoSocketIsDown(t *testing.T) {
	e := New("server-1", t.TempDir())
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	event := e.Collect(ctx).(*model.PostgresEvent)
	if event.Up {
		t.Fatal("expected up=false with no socket")
	}
	if !strings.Contains(event.Error, "no such file or directory") {
		t.Fatalf("expected the dial error, got %q", event.Error)
	}

	payload, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	var raw map[string]any
	if err := json.Unmarshal(payload, &raw); err != nil {
		t.Fatal(err)
	}
	if raw["serverId"] != "server-1" || raw["timestamp"] == nil || raw["up"] != false {
		t.Fatalf("expected serverId, timestamp and up always present, got %s", payload)
	}
	for _, list := range []string{"longRunning", "blocked", "replicas", "databases"} {
		if v, ok := raw[list].([]any); !ok || len(v) != 0 {
			t.Fatalf("expected %s present as [], got %s", list, payload)
		}
	}
	for _, absent := range []string{"errors", "connections", "uptimeSeconds", "role", "startedAt"} {
		if _, ok := raw[absent]; ok {
			t.Fatalf("expected %s absent when down, got %s", absent, payload)
		}
	}
}

// A failure is logged once when it starts, and forgotten when the part is not
// attempted in a round, so its next failure logs again
func TestRound_LogsOnceAndPrunes(t *testing.T) {
	e := New("server-1", t.TempDir())
	boom := errors.New("boom")

	r := &round{ctx: context.Background(), engine: e, seen: make(map[string]bool)}
	errs := make(map[string]string)
	if r.ok(errs, "sizes", "sizes", boom) {
		t.Fatal("expected a failure")
	}
	if errs["sizes"] != "boom" || !e.failing["sizes"] {
		t.Fatalf("expected the error published and remembered, got %v / %v", errs, e.failing)
	}
	r.prune()
	if !e.failing["sizes"] {
		t.Fatal("expected a part attempted this round to stay remembered")
	}

	r = &round{ctx: context.Background(), engine: e, seen: make(map[string]bool)}
	r.prune()
	if e.failing["sizes"] {
		t.Fatal("expected a part not attempted this round to be forgotten")
	}

	r = &round{ctx: context.Background(), engine: e, seen: make(map[string]bool)}
	r.ok(nil, "sizes", "sizes", boom)
	if !r.ok(nil, "sizes", "sizes", nil) || e.failing["sizes"] {
		t.Fatal("expected a success to clear the failure")
	}
}

func TestRound_AfterTheDeadlineEveryFailureIsTimedOut(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r := &round{ctx: ctx, engine: New("server-1", t.TempDir()), seen: make(map[string]bool)}
	errs := make(map[string]string)
	r.ok(errs, "deadTuples", "db/app/deadTuples", errors.New("conn closed"))
	if errs["deadTuples"] != errTimedOut {
		t.Fatalf("expected %q once the round is over, got %q", errTimedOut, errs["deadTuples"])
	}
}
