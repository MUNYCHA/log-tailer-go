package tailer

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"strings"
	"syscall"
	"time"

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

// Publisher ships a serialized log event to a pub/sub channel.
type Publisher interface {
	Publish(ctx context.Context, channel string, payload []byte) error
}

type Tailer struct {
	path       string
	channel    string
	serverName string
	publisher  Publisher
	buf        [readBufSize]byte // reused per read, no per-poll allocation
	shipped    int64             // lines shipped since last heartbeat; touched only by the Run goroutine
}

func New(path, channel, serverName string, publisher Publisher) *Tailer {
	return &Tailer{
		path:       path,
		channel:    channel,
		serverName: serverName,
		publisher:  publisher,
	}
}

// Run tails the file and ships each complete line to Redis Pub/Sub.
// Returns when ctx is cancelled (graceful shutdown).
func (t *Tailer) Run(ctx context.Context) {
	slog.Info("Starting log tailer", "path", t.path, "channel", t.channel)

	var (
		f          *os.File
		fileIno    uint64
		offset     int64
		lineBuffer bytes.Buffer
		startAtEnd    = true // first open skips existing content
		cycle         int
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
			if _, err := os.Stat(t.path); os.IsNotExist(err) {
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

func fileInode(path string) (uint64, error) {
	var stat syscall.Stat_t
	if err := syscall.Stat(path, &stat); err != nil {
		return 0, err
	}
	return stat.Ino, nil
}

// flushCompleteLines extracts complete newline-terminated lines from buf and publishes each.
// Incomplete trailing data stays in the buffer for the next read.
func (t *Tailer) flushCompleteLines(ctx context.Context, buf *bytes.Buffer) {
	data := buf.Bytes()

	// Guard against unbounded buffer growth when no newline appears for a long time
	if len(data) > maxLineBytes {
		slog.Warn("Line exceeds max length, flushing oversized chunk", "path", t.path, "max_bytes", maxLineBytes)
		t.publish(ctx, string(data[:maxLineBytes]))
		buf.Next(maxLineBytes)
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

		t.publish(ctx, line)
	}

	if consumed > 0 {
		buf.Next(consumed)
	}
}

func (t *Tailer) publish(ctx context.Context, line string) {
	event := model.LogEvent{
		ServerName: t.serverName,
		Path:       t.path,
		Channel:    t.channel,
		Timestamp:  time.Now().UTC().Format(time.RFC3339),
		Message:    line,
	}

	payload, err := json.Marshal(event)
	if err != nil {
		slog.Error("Failed to serialize log event", "path", t.path, "error", err)
		return
	}

	// The publisher logs delivery failures (throttled); the line is dropped —
	// loss is accepted over buffering. ctx aborts a publish against a stalled
	// Redis during shutdown.
	if err := t.publisher.Publish(ctx, t.channel, payload); err == nil {
		t.shipped++
	}
}

func sleep(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}
