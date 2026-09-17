# log-tailer-go

A lightweight log file tailer that reads log files and publishes each line to Redis Pub/Sub as a structured JSON event. Written in Go — no JVM, no runtime dependencies, single binary.

## Features

- Tails one or more log files concurrently, starting at the end of each file — only lines written after startup are shipped
- Detects log rotation, truncation, and file disappearance — auto-recovers without manual intervention
- Publishes each line as a JSON event to a Redis Pub/Sub channel
- Drains bursts at full speed — no polling gap while behind, then back to a relaxed 200 ms poll
- Bursts are published as pipelined batches (one round trip per 64 KB read chunk), sustaining 50k+ lines/s per file while staying synchronous — no internal queues
- Every component — each per-file tailer, the resources and storage collectors, and the heartbeat — recovers from panics and restarts automatically (1 s delay so a crash loop can't spin hot)
- Each tailer logs a liveness line to the journal every 5 minutes with its lines-shipped count — silent zero-shipping is visible without a subscriber (this is a journal log, unrelated to the `agent-heartbeat` channel below)
- Waits for Redis at startup — retries every 5 s instead of exiting, so it also self-heals when run without systemd
- Auto-reconnects if Redis goes down mid-run; publish failures are logged with throttling and memory stays flat (nothing is buffered)
- Warns (throttled) when a channel has zero subscribers, so a down consumer is visible in the journal
- Structured logging via `log/slog`
- Config file may be JSON or YAML, auto-detected by extension
- Optional resources collector publishes server uptime, load average, CPU utilisation, memory/swap and network throughput as one JSON event on its own timer
- Optional storage collector publishes disk usage for the configured mounts as a separate JSON event on its own timer, so a slow disk never delays the resources event
- Heartbeat (on by default) publishes a fixed liveness beat on its own ticker, reading nothing and sharing no state with the collectors, so a wedged storage read can't make a healthy server look down
- Graceful shutdown on `SIGTERM` / `SIGINT` — publishes are synchronous, so exit is immediate with nothing left in flight

## Requirements

- **Linux only** — metrics are read from `/proc`, `/sys` and `statfs`; the agent does not build on Windows, macOS or BSD
- **Kernel 3.14 or newer** for a complete metrics report
  - 3.2 – 3.13: runs, but the memory fields are omitted (`MemAvailable` was added to `/proc/meminfo` in 3.14)
  - RHEL/CentOS 7 (3.10) backports `MemAvailable`, so memory is reported there too
  - Below 3.2: not supported by the Go runtime
- **Tested on kernel 6.6.** Older kernels are covered by the documented, append-only `/proc` formats, not by direct testing
- **Go 1.25+** to build (`go.mod`); the resulting binary has no runtime dependencies
- **Redis** reachable from the server (Pub/Sub only, no persistence needed)

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
│   └── event.go         — LogEvent, ResourcesEvent, StorageEvent and HeartbeatEvent JSON structures
├── redis/
│   └── publisher.go     — Redis Pub/Sub publisher
├── logs/
│   ├── tailer.go        — follows one log file, handles rotation/truncation, publishes each line
│   └── tailer_test.go
├── resources/
│   ├── collector.go     — every interval: runs read → parse → calculate for each metric
│   │                      below, builds one resources event, publishes it
│   ├── collector_test.go
│   ├── uptime/          — /proc/uptime → uptimeSeconds
│   ├── load/            — /proc/loadavg → cpu.load1/5/15
│   ├── memory/          — /proc/meminfo → memory and swap in bytes
│   ├── cpu/             — /proc/stat → cpu.count; last tick vs this tick → cpu.usedPercent
│   └── network/         — /proc/net/dev, physical NICs only, last tick vs this tick → rx/tx bytes/sec
├── storage/
│   ├── collector.go     — every interval: runs read → parse → calculate for each mount,
│   │                      builds one storage event, publishes it
│   ├── collector_test.go
│   └── disk/            — statfs on each mount → total/used/free bytes, used %
├── heartbeat/
│   ├── heartbeat.go     — fixed-interval liveness beat
│   └── heartbeat_test.go
└── deploy/
    └── log-tailer-go.service — systemd unit for production
```

Every metric folder under `resources/` and `storage/` has the same four files, one job each:

| File | Job | Touches the server? |
|---|---|---|
| `read.go` | Gets raw data from Linux (`/proc`, `/sys`, `statfs`) | Yes — the only file that does |
| `parse.go` | Turns the raw data into plain numbers | No |
| `calculate.go` | Turns those numbers into the published values (bytes, %, rates) | No |
| `<name>_test.go` | Tests parsing and calculation with fake input | No |

`collector.go` never reads the server or does arithmetic itself — it calls those three steps for each metric in turn. Adding a metric means adding a folder with the same shape and one call in the matching `collector.go`.

## Message Format

Each log line is published as a JSON object:

```json
{
  "systemId": "your-system-id",
  "systemName": "your-system-name",
  "serverName": "your-server-name",
  "serverIp": "10.0.0.5",
  "path": "/var/log/app/app.log",
  "channel": "your-channel-1",
  "timestamp": "2026-05-28T10:00:00Z",
  "message": "the raw log line"
}
```

Log, resources and storage events all open with the same four identity fields in the same order, so a consumer extracts identity the same way on every channel. `systemId` is the stable key to group or join on — it never changes for a given system, while `systemName` and `serverIp` may change and are refreshed from every event. The heartbeat is the exception: it carries only `systemId` and `serverName`, the pair that identifies a server, and nothing else.

Consume with `SUBSCRIBE your-channel-1` (or `PSUBSCRIBE your-channel-*` for all channels). Note that Redis Pub/Sub has no persistence: messages published while no subscriber is connected are discarded.

`message` is the raw line with its trailing newline removed; a BOM and any `\r` are stripped, but leading and trailing spaces are preserved so indented stack traces survive intact. Empty lines are skipped entirely (a whitespace-only line is still published). A line longer than 1 MB is published as a 1 MB chunk and the remainder continues in the next event, so a runaway line can't grow the buffer without bound.

### Resources

When `resources.enabled` is `true`, a snapshot of server uptime, CPU, memory, swap and network is published to `resources.channel` every `resources.interval`:

```json
{
  "systemId": "your-system-id",
  "systemName": "your-system-name",
  "serverName": "your-server-name",
  "serverIp": "10.0.0.5",
  "timestamp": "2026-05-28T10:00:00Z",
  "uptimeSeconds": 435600,
  "cpu": {
    "count": 8,
    "usedPercent": 12.7,
    "load1": 0.52,
    "load5": 0.41,
    "load15": 0.38
  },
  "memory": {
    "totalBytes": 16466874368,
    "availableBytes": 14146666496
  },
  "swap": {
    "totalBytes": 4294967296,
    "usedBytes": 102400000
  },
  "network": {
    "rxBytesPerSec": 184320.5,
    "txBytesPerSec": 52480.25
  }
}
```

This collector runs independently of `logTailer` and `storage` — each can be enabled on its own.

Every `/proc`-sourced value is **omitted from the JSON when it can't be read**, never sent as a zero: absent means "unknown", where `0` would read as a real measurement of an idle machine. Values are omitted as a group, since a half-parsed file tells you nothing about which line survived:

| Source | Values | If the read or parse fails |
|---|---|---|
| `/proc/uptime` | `uptimeSeconds` | Omitted, the tick still publishes |
| `/proc/loadavg` | `cpu.load1`, `cpu.load5`, `cpu.load15` | All three omitted, the tick still publishes |
| `/proc/stat` | `cpu.count`, `cpu.usedPercent` | Both omitted, the tick still publishes |
| `/proc/loadavg` and `/proc/stat` | the whole `cpu` group | Omitted, the tick still publishes |
| `/proc/meminfo` | the whole `memory` and `swap` groups | Both omitted, the tick still publishes |
| `/proc/net/dev` | the whole `network` group | Omitted, the tick still publishes |

`cpu.count` is the number of online logical CPUs (the `cpu0` … `cpuN` lines of `/proc/stat`, the same figure as `nproc`). Load average is measured against it: `load1` of 4 is a full 4-CPU server but a mostly idle 64-CPU one. Unlike `cpu.usedPercent` it needs no previous sample, so it is present from the first tick.

Memory values are bytes (`/proc/meminfo` reports kB, multiplied by 1024). `memory.availableBytes` is `MemAvailable`, not `MemFree`, so it accounts for reclaimable page cache. `swap.usedBytes` is `SwapTotal - SwapFree`.

`cpu.usedPercent` is the **mean busy percentage over the whole interval**, not an instantaneous reading — it differences two `/proc/stat` samples one `resources.interval` apart:

```
busy = (total_now - total_prev) - (idle_now - idle_prev)
pct  = 100 * busy / (total_now - total_prev)
```

`idle` counts both the `idle` and `iowait` columns, matching `top`: a server blocked on a dead NFS mount is waiting, not burning CPU, and reporting it as busy would send someone hunting the wrong problem. Load average is what surfaces that case, which is why `load1/5/15` are published alongside — the two numbers disagreeing is the signal.

`total` excludes the `guest` and `guest_nice` columns. The kernel already counts time spent running VMs inside `user` and `nice`, so adding those columns again would inflate `total` and busy time by the same amount and make `cpu.usedPercent` read too high on a host running VMs (e.g. 70% real busy reported as ~79%). On an ordinary server or inside a VM both columns are `0`, and the result is unchanged.

Because it needs two samples, `cpu.usedPercent` is **omitted on the first tick after startup**, and again on the first tick after a supervised restart (the collector is rebuilt, so the previous sample is gone). It's also omitted if the counters move backwards, which is what a reboot between ticks looks like. A short spike inside a 1-minute interval is flattened into the mean; that's the intended trade, and load average is the finer-grained signal.

`network.rxBytesPerSec` (download) and `network.txBytesPerSec` (upload) are the **mean rate in bytes per second over the interval**, differencing two `/proc/net/dev` samples and dividing by the wall-clock time between them. Multiply by 8 for bits per second. They are summed over **physical interfaces only** — those with a `/sys/class/net/<iface>/device` link — so loopback, Docker bridges, veths and tunnels are excluded; counting them would double count traffic that also crosses the NIC. An interface that appears between two ticks is left out of that window, since it has no baseline.

The `network` group follows the same omission rules as `cpu.usedPercent`: absent on the first tick after startup or a supervised restart, and absent when any counter moves backwards (a driver reload). A host with no physical interface — e.g. the agent running inside a container — never reports it.

### Storage

When `storage.enabled` is `true`, disk usage for each path in `storage.mounts` is published to `storage.channel` every `storage.interval`:

```json
{
  "systemId": "your-system-id",
  "systemName": "your-system-name",
  "serverName": "your-server-name",
  "serverIp": "10.0.0.5",
  "timestamp": "2026-05-28T10:00:00Z",
  "mounts": [
    { "path": "/", "totalBytes": 214748364800, "usedBytes": 52428800000, "freeBytes": 151234567890, "usedPercent": 24.4 },
    { "path": "/var/log", "totalBytes": 10737418240, "usedBytes": 1073741824, "freeBytes": 9448931328, "usedPercent": 10.0 }
  ]
}
```

Mounts are reported in config order. A mount that can't be statted (typo'd path, not mounted) is reported with an `error` field; its byte fields are present but meaningless, so treat a non-empty `error` as "no reading" rather than reading the zeros. The rest of the mounts still publish normally. `storage.mounts` may be empty, in which case `mounts` is published as `[]`.

Storage runs as its own component, separate from resources, so a `statfs` stuck on a dead mount only delays the storage event — CPU, memory and the heartbeat keep publishing.

### Heartbeat

When `heartbeat.enabled` is `true` (the default), a beat is published to `heartbeat.channel` — `agent-heartbeat` unless overridden — every `heartbeat.interval`:

```json
{ "systemId": "your-system-id", "serverName": "your-server-name" }
```

That pair is the same identity the live metrics key is built from, so a beat maps to exactly one server. Because every beat names its own sender, one channel carries the beats of every server in a fleet and the consumer tells them apart from the payload — a per-server channel is supported but not needed for that. It is JSON rather than a bare id so a `serverName` containing a colon can't be misparsed by a consumer splitting on one, and so a field can be added later without a format break.

The heartbeat is deliberately the dumbest component in the agent: it reads no files, stats no mounts and shares no state with the resources or storage collectors, running on its own goroutine and its own ticker. If storage collection wedges on a stuck mount, the beat keeps going — a beat that can stop for any reason other than the agent being dead is worse than no beat at all. Publishes are fire and forget: a failure is logged (throttled) and dropped, never retried, never allowed to delay the next beat.

> **Changing `heartbeat.channel` is a coordinated change.** The consumer subscribes by name, and Redis discards a publish nobody is listening for. Point an agent at a channel the subscriber doesn't know and there is no error anywhere: the agent logs healthy beats, the consumer sees none, and the server reads as offline. Add the name on the subscriber side first — beats published before it subscribes are dropped, not queued.

> **Changing `heartbeat.interval` is a coordinated change.** The consumer expires a server's heartbeat key on a TTL of roughly three beats (30s for the default 10s interval). Raising the interval past that TTL makes every healthy server read as offline — silently, and looking exactly like a broken agent. Tell the API side before changing it.

## Configuration

Config is JSON or YAML — picked automatically by the file's extension (`.json`, or `.yaml`/`.yml`). Both formats use the same fields. YAML is parsed strictly: an unknown or misspelled key is a startup error. JSON is not — unknown keys there are ignored silently.

The config is validated at startup and any failure exits non-zero rather than running degraded. `redis.addr`, `identity.system.id`, `identity.system.name` and `identity.server.name` are always required; `logTailer.files` (each with a `path` and `channel`) is required when the tailer is enabled, `resources.channel` and a positive `resources.interval` when resources is enabled, and `storage.channel`, a positive `storage.interval` and non-empty `storage.mounts` entries (the list itself may be empty) when storage is enabled. `identity.server.ip` is optional and publishes as an empty string if omitted. Enabling nothing at all — no `logTailer`, no `resources`, no `storage`, and `heartbeat.enabled: false` — is also an error, since there would be nothing to do.

The whole `heartbeat` block is optional: omit it and the heartbeat runs on `agent-heartbeat` at its 10s default, so a config written before the heartbeat existed picks it up without being edited. Set `heartbeat.enabled: false` to opt out; a disabled heartbeat isn't validated, so a stale interval can't block startup.

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
| `identity.system.id` | Unique system identifier (stable; published in every event as `systemId`) |
| `identity.system.name` | System display name (published in every event as `systemName`) |
| `identity.server.name` | Server hostname (published in every event as `serverName`) |
| `identity.server.ip` | Server IP address (published in every event as `serverIp`) |
| `logTailer.enabled` | Enable or disable the tailer |
| `logTailer.files` | List of `{ path, channel }` entries to tail |
| `resources.enabled` | Enable or disable the resources collector |
| `resources.channel` | Redis Pub/Sub channel for resources events |
| `resources.interval` | Collection interval, as a Go duration string (e.g. `"30s"`) |
| `storage.enabled` | Enable or disable the storage collector |
| `storage.channel` | Redis Pub/Sub channel for storage events |
| `storage.interval` | Collection interval, as a Go duration string (e.g. `"5m"`) |
| `storage.mounts` | Optional list of mount paths to report disk usage for |
| `heartbeat.enabled` | Enable or disable the heartbeat (default `true` when the key or the whole block is omitted) |
| `heartbeat.channel` | Channel beats are published to (default `"agent-heartbeat"`; an empty value falls back to it). Must match what the consumer subscribes to |
| `heartbeat.interval` | Beat interval, as a Go duration string (default `"10s"`; must stay well under the consumer's TTL) |

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
./log-tailer-go --config=/etc/log-tailer-go/config.yaml

# or as a positional argument
./log-tailer-go /etc/log-tailer-go/config.yaml

# via environment variable
LOGTAILER_CONFIG=/etc/log-tailer-go/config.yaml ./log-tailer-go
```

Priority: command-line argument (flag or positional) > `LOGTAILER_CONFIG` env var > default path. The default is JSON only — a YAML config always needs its path given explicitly, by any of the three means above.

There is no `--help` or `--version`; an unrecognized flag is ignored rather than reported, so a typo'd flag starts the service on the default config instead of failing.

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

### Deploying with a YAML config

The format is chosen by the file extension, so a YAML deployment is the same steps with the config path changed in `ExecStart=`. Copy the YAML config instead, and point the unit at it before reloading:

```bash
sudo mkdir -p /opt/log-tailer-go /etc/log-tailer-go
sudo cp log-tailer-go /opt/log-tailer-go/
sudo cp config/config.yaml /etc/log-tailer-go/
sudo cp deploy/log-tailer-go.service /etc/systemd/system/

# point the unit at the YAML config
sudo sed -i 's|--config=/etc/log-tailer-go/config.json|--config=/etc/log-tailer-go/config.yaml|' \
  /etc/systemd/system/log-tailer-go.service

sudo systemctl daemon-reload
sudo systemctl enable --now log-tailer-go
```

Renaming the file alone is not enough — the path in `ExecStart=` is what selects the parser, and a `.yaml` file left at the `.json` path is parsed as JSON and fails at startup. Keep exactly one config in `/etc/log-tailer-go/` so there is no question which one is live. Remember that YAML is parsed strictly: a misspelled key stops the service rather than being ignored, which `journalctl -u log-tailer-go` reports as a config parse error.

The unit fences the service hard:

- `MemoryMax=64M` + `MemorySwapMax=0` — hard memory ceiling (includes page cache), no swap
- `CPUQuota=25%` + `Nice=10` — at most a quarter of one core, yields to everything else
- `ProtectSystem=strict` + `NoNewPrivileges` — entire filesystem is read-only to the process, kernel-enforced
- `ProtectHome=read-only` + `PrivateTmp` — home directories are inaccessible, `/tmp` is isolated from the rest of the system
- `ProtectKernelTunables` + `ProtectControlGroups` + `RestrictSUIDSGID` — no writing to `/proc/sys` or the cgroup hierarchy, can't create setuid/setgid files
- `Restart=on-failure` + `RestartSec=5` — self-heals indefinitely, including when Redis is down at boot
- `TimeoutStopSec=20` — bounded shutdown window before systemd force-kills the process

> No JVM flags needed — Go binaries use only what they need.
