package tailer_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"log-tailer-go/model"
	"log-tailer-go/tailer"
)

// Timeouts are generous because the tests run against the tailer's real
// timing constants (200 ms poll, 1 s retries, ~5 s rotation check) — no
// production code is modified for testability.
const (
	fastWait = 5 * time.Second  // paths detected on the 200 ms poll
	slowWait = 15 * time.Second // paths detected on the ~5 s rotation check
)

// fakePublisher records everything the tailer publishes. No Redis involved.
type fakePublisher struct {
	mu         sync.Mutex
	events     []model.LogEvent
	channels   []string
	batchSizes []int
}

func (p *fakePublisher) PublishBatch(_ context.Context, channel string, payloads [][]byte) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, payload := range payloads {
		var ev model.LogEvent
		if err := json.Unmarshal(payload, &ev); err != nil {
			ev = model.LogEvent{Message: "UNMARSHAL-ERROR: " + err.Error()}
		}
		p.events = append(p.events, ev)
		p.channels = append(p.channels, channel)
	}
	p.batchSizes = append(p.batchSizes, len(payloads))
	return len(payloads)
}

func (p *fakePublisher) messages() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]string, len(p.events))
	for i, ev := range p.events {
		out[i] = ev.Message
	}
	return out
}

func (p *fakePublisher) allEvents() []model.LogEvent {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]model.LogEvent(nil), p.events...)
}

// waitForMessages polls until at least n messages arrived or the timeout hits.
func (p *fakePublisher) waitForMessages(t *testing.T, n int, timeout time.Duration) []string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if msgs := p.messages(); len(msgs) >= n {
			return msgs
		}
		time.Sleep(20 * time.Millisecond)
	}
	msgs := p.messages()
	t.Fatalf("timed out waiting for %d messages, got %d: %q", n, len(msgs), msgs)
	return nil
}

// startTailer runs a tailer against path in the background and registers
// cleanup that cancels it and waits for Run to return.
func startTailer(t *testing.T, path string, pub *fakePublisher) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		tailer.New(path, "test-channel", "test-server", pub).Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(fastWait):
			t.Error("tailer did not stop after context cancel")
		}
	})
}

func createFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func appendToFile(t *testing.T, path, content string) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.WriteString(content); err != nil {
		t.Fatal(err)
	}
}

// waitForOpen gives the tailer time to open the file and seek to the end,
// so content appended afterwards is unambiguously "new". The first open
// happens on the first loop iteration, i.e. within milliseconds of Run.
func waitForOpen() {
	time.Sleep(500 * time.Millisecond)
}

func expectMessages(t *testing.T, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %d messages %q, want %d %q", len(got), got, len(want), want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("message %d = %q, want %q (all: %q)", i, got[i], want[i], got)
		}
	}
}

func TestSkipsExistingContentShipsNewLines(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "app.log")
	createFile(t, path, "old-1\nold-2\n")

	pub := &fakePublisher{}
	startTailer(t, path, pub)
	waitForOpen()

	appendToFile(t, path, "new-1\nnew-2\n")

	got := pub.waitForMessages(t, 2, fastWait)
	expectMessages(t, got, []string{"new-1", "new-2"})
}

func TestEventFields(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "app.log")
	createFile(t, path, "")

	pub := &fakePublisher{}
	startTailer(t, path, pub)
	waitForOpen()

	appendToFile(t, path, "hello\n")
	pub.waitForMessages(t, 1, fastWait)

	ev := pub.allEvents()[0]
	if ev.ServerName != "test-server" {
		t.Errorf("ServerName = %q, want %q", ev.ServerName, "test-server")
	}
	if ev.Path != path {
		t.Errorf("Path = %q, want %q", ev.Path, path)
	}
	if ev.Channel != "test-channel" {
		t.Errorf("Channel = %q, want %q", ev.Channel, "test-channel")
	}
	if _, err := time.Parse(time.RFC3339, ev.Timestamp); err != nil {
		t.Errorf("Timestamp %q is not RFC3339: %v", ev.Timestamp, err)
	}

	pub.mu.Lock()
	channel := pub.channels[0]
	pub.mu.Unlock()
	if channel != "test-channel" {
		t.Errorf("published to channel %q, want %q", channel, "test-channel")
	}
}

func TestPartialLineAssembledAcrossWrites(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "app.log")
	createFile(t, path, "")

	pub := &fakePublisher{}
	startTailer(t, path, pub)
	waitForOpen()

	appendToFile(t, path, "hel")
	// Let the tailer read the incomplete fragment before completing it
	time.Sleep(600 * time.Millisecond)
	appendToFile(t, path, "lo\n")

	got := pub.waitForMessages(t, 1, fastWait)
	expectMessages(t, got, []string{"hello"})
}

func TestLineHygiene(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "app.log")
	createFile(t, path, "")

	pub := &fakePublisher{}
	startTailer(t, path, pub)
	waitForOpen()

	// CRLF endings, BOM, blank lines, and intentional leading/trailing
	// whitespace (e.g. indented stack traces) that must be preserved
	appendToFile(t, path, "plain\r\n  indented \r\n\ufeffbom-prefixed\n\n\r\nlast\n")

	got := pub.waitForMessages(t, 4, fastWait)
	expectMessages(t, got, []string{"plain", "  indented ", "bom-prefixed", "last"})
}

func TestTruncationReopensFromStart(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "app.log")
	createFile(t, path, "")

	pub := &fakePublisher{}
	startTailer(t, path, pub)
	waitForOpen()

	appendToFile(t, path, "before-truncate-1\nbefore-truncate-2\n")
	pub.waitForMessages(t, 2, fastWait)

	if err := os.Truncate(path, 0); err != nil {
		t.Fatal(err)
	}
	appendToFile(t, path, "after-truncate\n")

	got := pub.waitForMessages(t, 3, fastWait)
	expectMessages(t, got, []string{"before-truncate-1", "before-truncate-2", "after-truncate"})
}

func TestRotationPicksUpNewFile(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "app.log")
	createFile(t, path, "")

	pub := &fakePublisher{}
	startTailer(t, path, pub)
	waitForOpen()

	appendToFile(t, path, "before-rotate\n")
	pub.waitForMessages(t, 1, fastWait)

	// Classic rename rotation: old file moved aside, fresh file at same path
	if err := os.Rename(path, path+".1"); err != nil {
		t.Fatal(err)
	}
	createFile(t, path, "after-rotate\n")

	// Detection happens on the ~5 s inode check
	got := pub.waitForMessages(t, 2, slowWait)
	expectMessages(t, got, []string{"before-rotate", "after-rotate"})
}

func TestRotationDropsBufferedPartialLine(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "app.log")
	createFile(t, path, "")

	pub := &fakePublisher{}
	startTailer(t, path, pub)
	waitForOpen()

	// An unterminated fragment sits in the line buffer when rotation hits;
	// the buffer must be discarded, never glued onto the new file's lines
	appendToFile(t, path, "orphan-fragment")
	time.Sleep(600 * time.Millisecond)

	if err := os.Rename(path, path+".1"); err != nil {
		t.Fatal(err)
	}
	createFile(t, path, "fresh-line\n")

	got := pub.waitForMessages(t, 1, slowWait)
	expectMessages(t, got, []string{"fresh-line"})
	for _, msg := range got {
		if strings.Contains(msg, "orphan") {
			t.Fatalf("buffered partial line leaked across rotation: %q", msg)
		}
	}
}

func TestFileDeletedThenRecreated(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "app.log")
	createFile(t, path, "")

	pub := &fakePublisher{}
	startTailer(t, path, pub)
	waitForOpen()

	appendToFile(t, path, "before-delete\n")
	pub.waitForMessages(t, 1, fastWait)

	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	// Disappearance is noticed on the ~5 s check; wait it out, then recreate
	time.Sleep(6 * time.Second)
	createFile(t, path, "after-recreate\n")

	got := pub.waitForMessages(t, 2, slowWait)
	expectMessages(t, got, []string{"before-delete", "after-recreate"})
}

func TestWaitsForFileToAppear(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "app.log")

	pub := &fakePublisher{}
	startTailer(t, path, pub) // file does not exist yet

	time.Sleep(1500 * time.Millisecond)
	createFile(t, path, "")
	// Give the 1 s wait-for-file loop time to open the new file
	time.Sleep(2500 * time.Millisecond)

	appendToFile(t, path, "first-line\n")

	got := pub.waitForMessages(t, 1, fastWait)
	expectMessages(t, got, []string{"first-line"})
}

func TestBurstArrivesInOrderAndBatched(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "app.log")
	createFile(t, path, "")

	pub := &fakePublisher{}
	startTailer(t, path, pub)
	waitForOpen()

	// ~200 KB in one write — spans multiple 64 KB read chunks
	const lines = 5000
	var b strings.Builder
	for i := 0; i < lines; i++ {
		fmt.Fprintf(&b, "burst-line-%06d-padding-padding\n", i)
	}
	appendToFile(t, path, b.String())

	got := pub.waitForMessages(t, lines, fastWait)
	if len(got) != lines {
		t.Fatalf("got %d lines, want %d", len(got), lines)
	}
	for i, msg := range got {
		want := fmt.Sprintf("burst-line-%06d-padding-padding", i)
		if msg != want {
			t.Fatalf("line %d = %q, want %q", i, msg, want)
		}
	}

	pub.mu.Lock()
	maxBatch := 0
	for _, size := range pub.batchSizes {
		if size > maxBatch {
			maxBatch = size
		}
	}
	pub.mu.Unlock()
	if maxBatch < 2 {
		t.Errorf("expected pipelined batches during burst, largest batch was %d", maxBatch)
	}
}

func TestOversizedLineIsForceFlushed(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "app.log")
	createFile(t, path, "")

	pub := &fakePublisher{}
	startTailer(t, path, pub)
	waitForOpen()

	// 1.2 MB with no newline — exceeds the 1 MB cap, must be force-flushed
	// in chunks instead of growing the buffer unboundedly
	const total = 1200 * 1024
	oversized := strings.Repeat("a", total)
	appendToFile(t, path, oversized)
	pub.waitForMessages(t, 1, fastWait)

	// Terminate the remainder so everything ships
	appendToFile(t, path, "\n")

	deadline := time.Now().Add(fastWait)
	for time.Now().Before(deadline) {
		if totalLen(pub.messages()) >= total {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	got := pub.messages()
	if len(got) < 2 {
		t.Fatalf("expected oversized line split into multiple events, got %d", len(got))
	}
	reassembled := strings.Join(got, "")
	if reassembled != oversized {
		t.Fatalf("reassembled content differs: got %d bytes, want %d", len(reassembled), total)
	}
}

func totalLen(msgs []string) int {
	n := 0
	for _, m := range msgs {
		n += len(m)
	}
	return n
}

func TestShutdownStopsPromptly(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "app.log")
	createFile(t, path, "")

	pub := &fakePublisher{}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		tailer.New(path, "test-channel", "test-server", pub).Run(ctx)
	}()
	waitForOpen()

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return within 2 s of context cancel")
	}
}
