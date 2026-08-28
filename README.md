# log-tailer-go

A lightweight log file tailer that reads log files and publishes each line to Redis Pub/Sub as a structured JSON event. Written in Go — no JVM, no runtime dependencies, single binary.

## Features

- Tails one or more log files concurrently, starting at the end of each file — only lines written after startup are shipped
- Detects log rotation, truncation, and file disappearance — auto-recovers without manual intervention
- Publishes each line as a JSON event to a Redis Pub/Sub channel
- Drains bursts at full speed — no polling gap while behind, then back to a relaxed 200 ms poll
- Bursts are published as pipelined batches (one round trip per 64 KB read chunk), sustaining 50k+ lines/s per file while staying synchronous — no internal queues
- Every component — each per-file tailer and the metrics collector — recovers from panics and restarts automatically (1 s delay so a crash loop can't spin hot)
- Heartbeat log every 5 minutes per file with lines shipped — silent zero-shipping is visible in the journal
- Waits for Redis at startup — retries every 5 s instead of exiting, so it also self-heals when run without systemd
- Auto-reconnects if Redis goes down mid-run; publish failures are logged with throttling and memory stays flat (nothing is buffered)
- Warns (throttled) when a channel has zero subscribers, so a down consumer is visible in the journal
- Structured logging via `log/slog`
- Config file may be JSON or YAML, auto-detected by extension
- Optional metrics collector publishes mount disk usage + server uptime as a combined JSON event on a timer, independent of log tailing
- Graceful shutdown on `SIGTERM` / `SIGINT` — publishes are synchronous, so exit is immediate with nothing left in flight

## Project Structure

```
log-tailer-go/
├── main.go              — entry point, wiring, graceful shutdown
├── config/
│   ├── config.go        — config structs and validation
│   ├── loader.go        — config loading and path resolution
│   ├── config_test.go
│   ├── config.example.json
│   └── config.example.yaml
├── model/
│   └── event.go         — LogEvent and MetricsEvent JSON structures
├── redis/
│   └── publisher.go     — Redis Pub/Sub publisher
├── tailer/
│   ├── tailer.go        — core file tailing logic
│   └── tailer_test.go
├── metrics/
│   ├── metrics.go       — mount usage + server uptime collector
│   └── metrics_test.go
└── deploy/
    └── log-tailer-go.service — systemd unit for production
```

## Message Format

Each log line is published as a JSON object:

```json
{
  "serverName": "your-server-name",
  "path": "/var/log/app/app.log",
  "channel": "your-channel-1",
  "timestamp": "2026-05-28T10:00:00Z",
  "message": "the raw log line"
}
```

Consume with `SUBSCRIBE your-channel-1` (or `PSUBSCRIBE your-channel-*` for all channels). Note that Redis Pub/Sub has no persistence: messages published while no subscriber is connected are discarded.

`message` is the raw line with its trailing newline removed; a BOM and any `\r` are stripped, but leading and trailing spaces are preserved so indented stack traces survive intact. Empty lines are skipped entirely (a whitespace-only line is still published). A line longer than 1 MB is published as a 1 MB chunk and the remainder continues in the next event, so a runaway line can't grow the buffer without bound.

### Metrics

When `metrics.enabled` is `true`, a combined snapshot of server uptime and disk usage for the configured mounts is published to `metrics.channel` every `metrics.interval`:

```json
{
  "systemId": "your-system-id",
  "systemName": "your-system-name",
  "serverName": "your-server-name",
  "serverIp": "10.0.0.5",
  "timestamp": "2026-05-28T10:00:00Z",
  "uptimeSeconds": 435600,
  "mounts": [
    { "path": "/", "totalBytes": 214748364800, "usedBytes": 52428800000, "freeBytes": 151234567890, "usedPercent": 24.4 },
    { "path": "/var/log", "totalBytes": 10737418240, "usedBytes": 1073741824, "freeBytes": 9448931328, "usedPercent": 10.0 }
  ]
}
```

A mount that can't be statted (typo'd path, not mounted) is reported with an `error` field; its byte fields are present but meaningless, so treat a non-empty `error` as "no reading" rather than reading the zeros. The rest of the mounts still publish normally. If server uptime can't be read, the whole tick is skipped and no event is published for that interval — expect an occasional gap rather than an event with a zero uptime. This collector runs independently of `logTailer` — either can be enabled on its own.

## Configuration

Config is JSON or YAML — picked automatically by the file's extension (`.json`, or `.yaml`/`.yml`). Both formats use the same fields. YAML is parsed strictly: an unknown or misspelled key is a startup error. JSON is not — unknown keys there are ignored silently.

The config is validated at startup and any failure exits non-zero rather than running degraded. `redis.addr`, `identity.system.id`, `identity.system.name` and `identity.server.name` are always required; `logTailer.files` (each with a `path` and `channel`) is required when the tailer is enabled, and `metrics.channel`, a positive `metrics.interval` and a non-empty `metrics.mounts` when the collector is enabled. `identity.server.ip` is optional and publishes as an empty string if omitted. Enabling neither `logTailer` nor `metrics` is also an error — there would be nothing to do.

Copy the sample config and fill in your values:

```bash
cp config/config.example.json config/config.json
# or, for YAML:
cp config/config.example.yaml config/config.yaml
```

| Field | Description |
|---|---|
| `redis.addr` | Redis address (`host:port`) |
| `redis.password` | Redis password (empty for none) |
| `redis.db` | Redis database number (Pub/Sub ignores it; kept for client completeness) |
| `identity.system.id` | Unique system identifier (stable; included in each metrics event as `systemId`) |
| `identity.system.name` | System display name (included in each metrics event as `systemName`) |
| `identity.server.name` | Server hostname (included in each event as `serverName`) |
| `identity.server.ip` | Server IP address (included in each metrics event as `serverIp`) |
| `logTailer.enabled` | Enable or disable the tailer |
| `logTailer.files` | List of `{ path, channel }` entries to tail |
| `metrics.enabled` | Enable or disable the metrics collector |
| `metrics.channel` | Redis Pub/Sub channel for metrics events |
| `metrics.interval` | Collection interval, as a Go duration string (e.g. `"1m"`, `"30s"`) |
| `metrics.mounts` | List of mount paths to report disk usage for |

## Build

```bash
go build -o log-tailer-go .
```

## Run

```bash
# uses config/config.json by default
./log-tailer-go

# specify config path explicitly (--config=PATH or --config PATH)
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
- `ProtectHome=read-only` + `PrivateTmp` — home directories are inaccessible, `/tmp` is isolated from the rest of the system
- `ProtectKernelTunables` + `ProtectControlGroups` + `RestrictSUIDSGID` — no writing to `/proc/sys` or the cgroup hierarchy, can't create setuid/setgid files
- `Restart=on-failure` + `RestartSec=5` — self-heals indefinitely, including when Redis is down at boot
- `TimeoutStopSec=20` — bounded shutdown window before systemd force-kills the process

> No JVM flags needed — Go binaries use only what they need.
