# log-tailer-go

A lightweight log file tailer that reads log files and ships each line to Kafka as a structured JSON event. Written in Go — no JVM, no runtime dependencies, single binary.

## Features

- Tails one or more log files concurrently, starting at the end of each file — only lines written after startup are shipped
- Detects log rotation, truncation, and file disappearance — auto-recovers without manual intervention
- Ships each line as a JSON event to Kafka
- Batches messages for efficient network usage
- Drains bursts at full speed — no polling gap while behind, then back to a relaxed 200 ms poll
- Per-file tailers recover from panics and restart automatically (1 s delay so a crash loop can't spin hot)
- Heartbeat log every 5 minutes per file with lines shipped — silent zero-shipping is visible in the journal
- Waits for Kafka at startup — retries every 5 s instead of exiting, so it also self-heals when run without systemd
- Structured logging via `log/slog`
- Graceful shutdown on `SIGTERM` / `SIGINT` — flushes Kafka before exiting

## Project Structure

```
log-tailer-go/
├── main.go              — entry point, wiring, graceful shutdown
├── config/
│   ├── config.go        — config structs and validation
│   ├── loader.go        — config loading and path resolution
│   └── config.example.json
├── model/
│   └── event.go         — LogEvent JSON structure
├── kafka/
│   └── producer.go      — Kafka async producer
├── tailer/
│   └── tailer.go        — core file tailing logic
└── deploy/
    └── log-tailer-go.service — systemd unit for production
```

## Kafka Message Format

Each log line is published as a JSON object:

```json
{
  "serverName": "your-server-name",
  "path": "/var/log/app/app.log",
  "topic": "your-topic-1",
  "timestamp": "2026-05-28T10:00:00Z",
  "message": "the raw log line"
}
```

## Configuration

Copy the sample config and fill in your values:

```bash
cp config/config.example.json config/config.json
```

| Field | Description |
|---|---|
| `bootstrapServers` | Comma-separated Kafka broker addresses |
| `identity.system.id` | Unique system identifier |
| `identity.system.name` | System display name |
| `identity.server.name` | Server hostname (used as Kafka message key) |
| `identity.server.ip` | Server IP address |
| `logTailer.enabled` | Enable or disable the tailer |
| `logTailer.files` | List of `{ path, topic }` entries to tail |

## Build

```bash
go build -o log-tailer-go .
```

## Run

```bash
# uses config/config.json by default
./log-tailer-go

# specify config path explicitly
./log-tailer-go --config=/etc/log-tailer-go/config.json

# or as a positional argument
./log-tailer-go /etc/log-tailer-go/config.json

# via environment variable
LOGTAILER_CONFIG=/etc/log-tailer-go/config.json ./log-tailer-go
```

Priority: command-line argument (flag or positional) > `LOGTAILER_CONFIG` env var > default path.

## Production Deployment (systemd)

The unit file lives at [`deploy/log-tailer-go.service`](deploy/log-tailer-go.service). It expects the binary at `/opt/log-tailer-go/log-tailer-go` and the config at `/etc/log-tailer-go/config.json`; adjust `User=`/`Group=` to an account that can read your log files.

```bash
sudo mkdir -p /opt/log-tailer-go /etc/log-tailer-go
sudo cp log-tailer-go /opt/log-tailer-go/
sudo cp config/config.json /etc/log-tailer-go/
sudo cp deploy/log-tailer-go.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now log-tailer-go
```

The unit fences the service hard:

- `MemoryMax=64M` + `MemorySwapMax=0` — hard memory ceiling (includes page cache), no swap
- `CPUQuota=25%` + `Nice=10` — at most a quarter of one core, yields to everything else
- `ProtectSystem=strict` + `NoNewPrivileges` — entire filesystem is read-only to the process, kernel-enforced
- `Restart=on-failure` + `RestartSec=5` — self-heals indefinitely, including when Kafka is down at boot

> No JVM flags needed — Go binaries use only what they need.
