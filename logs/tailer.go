package logs

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"log-tailer-go/config"
	"log-tailer-go/model"
)

const (
	maxLineBytes       = 1024 * 1024 // 1 MB — oversized lines are force-flushed
	readBufSize        = 64 * 1024   // 64 KB read buffer, allocated once
	pollInterval       = 200 * time.Millisecond
	rotationCheckEvery = 25 // cycles (~5 s at 200 ms poll)
	retryDelay         = time.Second
	waitForFileDelay   = time.Second
	heartbeatInterval  = 5 * time.Minute
)

// Publisher ships a batch of serialized log events to a pub/sub channel in
// one pipelined round trip, returning how many were accepted.
type Publisher interface {
	PublishBatch(ctx context.Context, channel string, payloads [][]byte) int
}

type Tailer struct {
	pattern   string // configured path; a glob when it holds * ? or [
	glob      bool
	path      string          // file currently followed — the pattern itself for an exact path
	left      map[uint64]bool // inodes of glob matches switched away from, never reopened
	channel   string
	identity  config.IdentityConfig
	publisher Publisher
	buf       [readBufSize]byte // reused per read, no per-poll allocation
	batch     [][]byte          // payloads pending publish; reused, bounded by lines per read chunk
	shipped   int64             // lines shipped since last heartbeat; touched only by the Run goroutine
}

func New(path, channel string, identity config.IdentityConfig, publisher Publisher) *Tailer {
	return &Tailer{
		pattern:   path,
		glob:      strings.ContainsAny(path, "*?["),
		path:      path,
		left:      make(map[uint64]bool),
		channel:   channel,
		identity:  identity,
		publisher: publisher,
	}
}

// Run tails the file and ships each complete line to Redis Pub/Sub.
// Returns when ctx is cancelled (graceful shutdown).
func (t *Tailer) Run(ctx context.Context) {
	slog.Info("Starting log tailer", "path", t.pattern, "channel", t.channel)

	var (
		f             *os.File
		fileIno       uint64
		offset        int64
		lineBuffer    bytes.Buffer
		startAtEnd    = true // first open skips existing content
		cycle         int
		switchPending bool // a newer glob match exists; switch once the current file is drained
		lastHeartbeat = time.Now()
	)

	defer func() {
		if f != nil {
			f.Close()
		}
	}()

	// Periodic liveness report — fires even while waiting for the file,
	// so silent zero-shipping is visible in the journal
	heartbeat := func() {
		if time.Since(lastHeartbeat) >= heartbeatInterval {
			slog.Info("Tailer heartbeat", "path", t.path, "channel", t.channel, "lines_shipped", t.shipped)
			t.shipped = 0
			lastHeartbeat = time.Now()
		}
	}

	for {
		if ctx.Err() != nil {
			return
		}

		heartbeat()

		// Open file if not already open
		if f == nil {
			switchPending = false
			if t.glob {
				newest, _ := t.newestMatch()
				if newest == "" {
					slog.Warn("Waiting for a log file matching the pattern", "pattern", t.pattern)
					sleep(ctx, waitForFileDelay)
					continue
				}
				t.path = newest
			} else if _, err := os.Stat(t.path); os.IsNotExist(err) {
				slog.Warn("Waiting for log file to appear", "path", t.path)
				sleep(ctx, waitForFileDelay)
				continue
			}

			var err error
			f, fileIno, offset, err = t.openFile(startAtEnd)
			if err != nil {
				slog.Error("Failed to open log file, retrying", "path", t.path, "error", err)
				sleep(ctx, retryDelay)
				continue
			}
			startAtEnd = false
			if t.glob {
				slog.Info("Following log file", "pattern", t.pattern, "path", t.path)
			}
		}

		cycle++

		// Rotation and disappearance check — off the hot path, runs ~every 5 s
		if cycle%rotationCheckEvery == 0 {
			if _, err := os.Stat(t.path); os.IsNotExist(err) {
				slog.Warn("Log file disappeared", "path", t.path)
				f.Close()
				f = nil
				lineBuffer.Reset()
				continue
			}
			if replaced, _ := t.hasBeenReplaced(fileIno); replaced {
				slog.Info("Log file replaced or rotated, reopening", "path", t.path)
				f.Close()
				f = nil
				lineBuffer.Reset()
				continue
			}
			if t.glob && !switchPending {
				switchPending = t.hasNewerMatch(f)
			}
		}

		info, err := f.Stat()
		if err != nil {
			slog.Error("Recoverable error stating file, retrying", "path", t.path, "error", err)
			f.Close()
			f = nil
			lineBuffer.Reset()
			sleep(ctx, retryDelay)
			continue
		}

		size := info.Size()

		if size < offset {
			slog.Info("Log file truncated, reopening", "path", t.path)
			f.Close()
			f = nil
			lineBuffer.Reset()
			continue
		}

		if size > offset {
			// Drain all available data before sleeping — one 64 KB chunk per loop
			// iteration so a burst of megabytes is processed without a 200 ms gap.
			drainErr := false
			for size > offset {
				n, err := f.ReadAt(t.buf[:], offset)
				if n > 0 {
					offset += int64(n)
					lineBuffer.Write(t.buf[:n])
					t.flushCompleteLines(ctx, &lineBuffer)
				}
				if err != nil && err != io.EOF {
					slog.Error("Recoverable read error, retrying", "path", t.path, "error", err)
					f.Close()
					f = nil
					lineBuffer.Reset()
					sleep(ctx, retryDelay)
					drainErr = true
					break
				}
				// Refresh size so we keep reading if more data arrived during this drain
				if info, err = f.Stat(); err != nil {
					break
				}
				size = info.Size()

				if ctx.Err() != nil {
					return
				}

				// A sustained burst can keep us in this drain loop past the
				// heartbeat interval — report from here too so a busy tailer
				// doesn't go silent
				heartbeat()
			}
			if drainErr {
				continue
			}
		}

		// The current file is drained — move to the newer match, read from its
		// start. Lines written to the old file after this point are dropped.
		if switchPending {
			slog.Info("Newer log file matches the pattern, switching", "pattern", t.pattern, "from", t.path)
			t.left[fileIno] = true
			f.Close()
			f = nil
			lineBuffer.Reset()
			continue
		}

		sleep(ctx, pollInterval)
	}
}

func (t *Tailer) openFile(startAtEnd bool) (*os.File, uint64, int64, error) {
	ino, err := fileInode(t.path)
	if err != nil {
		return nil, 0, 0, err
	}

	f, err := os.Open(t.path)
	if err != nil {
		return nil, 0, 0, err
	}

	var offset int64
	if startAtEnd {
		offset, err = f.Seek(0, io.SeekEnd)
		if err != nil {
			f.Close()
			return nil, 0, 0, err
		}
	}

	return f, ino, offset, nil
}

func (t *Tailer) hasBeenReplaced(knownIno uint64) (bool, error) {
	currentIno, err := fileInode(t.path)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return currentIno != knownIno, nil
}

// newestMatch returns the most recently modified file matching the pattern,
// skipping files already switched away from so a late write to yesterday's
// file cannot pull the tailer back to re-read it from the start. Ties go to
// the later name. Returns "" when nothing matches.
func (t *Tailer) newestMatch() (string, time.Time) {
	matches, _ := filepath.Glob(t.pattern) // the pattern is validated at config load
	var (
		newest  string
		newestM time.Time
		present = make(map[uint64]bool, len(matches))
	)
	for _, m := range matches {
		info, err := os.Stat(m)
		if err != nil || !info.Mode().IsRegular() {
			continue
		}
		stat, ok := info.Sys().(*syscall.Stat_t)
		if !ok {
			continue
		}
		present[stat.Ino] = true
		if t.left[stat.Ino] {
			continue
		}
		if newest == "" || info.ModTime().After(newestM) || (info.ModTime().Equal(newestM) && m > newest) {
			newest, newestM = m, info.ModTime()
		}
	}
	// Forget deleted files so a reused inode number is not mistaken for one
	for ino := range t.left {
		if !present[ino] {
			delete(t.left, ino)
		}
	}
	return newest, newestM
}

// hasNewerMatch reports whether another file matching the pattern was
// modified after the one currently open.
func (t *Tailer) hasNewerMatch(f *os.File) bool {
	info, err := f.Stat()
	if err != nil {
		return false
	}
	newest, mtime := t.newestMatch()
	return newest != "" && newest != t.path && mtime.After(info.ModTime())
}

func fileInode(path string) (uint64, error) {
	var stat syscall.Stat_t
	if err := syscall.Stat(path, &stat); err != nil {
		return 0, err
	}
	return stat.Ino, nil
}

// flushCompleteLines extracts complete newline-terminated lines from buf and
// publishes them as one pipelined batch — a burst pays one network round trip
// per read chunk, not per line. Incomplete trailing data stays in the buffer
// for the next read.
func (t *Tailer) flushCompleteLines(ctx context.Context, buf *bytes.Buffer) {
	data := buf.Bytes()

	// Guard against unbounded buffer growth when no newline appears for a long time
	if len(data) > maxLineBytes {
		slog.Warn("Line exceeds max length, flushing oversized chunk", "path", t.path, "max_bytes", maxLineBytes)
		t.appendEvent(string(data[:maxLineBytes]))
		buf.Next(maxLineBytes)
		t.publishBatch(ctx)
		return
	}

	consumed := 0
	for {
		idx := bytes.IndexByte(data[consumed:], '\n')
		if idx < 0 {
			break
		}
		line := string(data[consumed : consumed+idx])
		consumed += idx + 1

		// Strip BOM and \r — trim() is NOT used to preserve intentional
		// leading/trailing whitespace (e.g. indented stack traces)
		line = strings.ReplaceAll(line, "\ufeff", "")
		line = strings.ReplaceAll(line, "\r", "")

		if line == "" {
			continue
		}

		t.appendEvent(line)
	}

	if consumed > 0 {
		buf.Next(consumed)
	}
	t.publishBatch(ctx)
}

// appendEvent serializes line into a LogEvent and adds it to the pending batch.
func (t *Tailer) appendEvent(line string) {
	event := model.LogEvent{
		ServerID:  t.identity.ServerID,
		Path:      t.path,
		Channel:   t.channel,
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Message:   line,
	}

	payload, err := json.Marshal(event)
	if err != nil {
		slog.Error("Failed to serialize log event", "path", t.path, "error", err)
		return
	}
	t.batch = append(t.batch, payload)
}

// publishBatch ships the pending batch and resets it. The publisher logs
// delivery failures (throttled); failed lines are dropped — loss is accepted
// over buffering. ctx aborts a publish against a stalled Redis during shutdown.
func (t *Tailer) publishBatch(ctx context.Context) {
	if len(t.batch) == 0 {
		return
	}
	t.shipped += int64(t.publisher.PublishBatch(ctx, t.channel, t.batch))
	t.batch = t.batch[:0]
}

func sleep(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}
