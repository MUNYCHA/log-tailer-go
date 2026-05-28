# log-tailer-go

A lightweight log file tailer that reads log files and ships each line to Kafka as a structured JSON event. Written in Go — no JVM, no runtime dependencies, single binary.

## Features

- Tails one or more log files concurrently
- Detects log rotation, truncation, and file disappearance — auto-recovers without manual intervention
- Ships each line as a JSON event to Kafka
- Batches messages for efficient network usage
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
└── tailer/
    └── tailer.go        — core file tailing logic
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
# uses config/logTailer_config.json by default
./log-tailer-go

# specify config path explicitly
./log-tailer-go --config=/etc/log-tailer/config.json

# via environment variable
LOGTAILER_CONFIG=/etc/log-tailer/config.json ./log-tailer-go
```

## Production Deployment (systemd)

```ini
[Unit]
Description=Log Tailer -> Kafka
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=logtailer
Group=logtailer
ExecStart=/opt/log-tailer-go/log-tailer-go --config=/etc/log-tailer/config.json
Restart=on-failure
RestartSec=5
TimeoutStopSec=20
MemoryMax=64M
MemorySwapMax=0
CPUQuota=25%
NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=read-only
PrivateTmp=true

[Install]
WantedBy=multi-user.target
```

> No JVM flags needed — Go binaries use only what they need.
