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
- Optional storage collector publishes the server's total local storage plus disk usage for the configured mounts as a separate JSON event on its own timer, so a slow disk never delays the resources event
- Heartbeat (on by default) publishes a fixed liveness beat on its own ticker, reading nothing and sharing no state with the collectors, so a wedged storage read can't make a healthy server look down
- Graceful shutdown on `SIGTERM` / `SIGINT` — publishes are synchronous, so exit is immediate with nothing left in flight

## Requirements

- **Linux only** — metrics are read from `/proc`, `/sys` and `statfs`; the agent does not build on Windows, macOS or BSD
- **Kernel 3.14 or newer** for exact values in every field
  - 3.2 – 3.13: runs, but memory is estimated and marked `memory.estimated: true` (`MemAvailable` was added to `/proc/meminfo` in 3.14)
  - RHEL/CentOS 7 (3.10) backports `MemAvailable`, so memory is exact there too
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
│   ├── collector.go     — every interval: reads the mount table, builds the server total
│   │                      and each configured mount, publishes one storage event
│   ├── collector_test.go
│   ├── mounts/          — /proc/self/mounts → device, fsType, local or network per path; unique local filesystems
│   └── disk/            — statfs → total/used/free/reserved bytes, used %; server-wide total
├── heartbeat/
│   ├── heartbeat.go     — fixed-interval liveness beat
│   └── heartbeat_test.go
├── deploy/
│   └── log-tailer-go.service — systemd unit for production
└── docs/
    └── consumer-contract.md  — what a subscriber can rely on in every event
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
    "usedBytes": 2320207872,
    "availableBytes": 14146666496,
    "usedPercent": 14.09,
    "estimated": false
  },
  "swap": {
    "totalBytes": 4294967296,
    "usedBytes": 102400000,
    "usedPercent": 2.38
  },
  "network": {
    "rxBytesPerSec": 184320.5,
    "txBytesPerSec": 52480.25
  }
}
```

This collector runs independently of `logTailer` and `storage` — each can be enabled on its own.

Every `/proc`-sourced value is **omitted from the JSON when it can't be read**, never sent as a zero: absent means "unknown", where `0` would read as a real measurement of an idle machine. Values from one file are omitted together, since a half-parsed file tells you nothing about which line survived:

| Source | Values | If the read or parse fails |
|---|---|---|
| `/proc/uptime` | `uptimeSeconds` | Omitted, the tick still publishes |
| `/proc/loadavg` | `cpu.load1`, `cpu.load5`, `cpu.load15` | All three omitted, the tick still publishes |
| `/proc/stat` | `cpu.count`, `cpu.usedPercent` | Both omitted, the tick still publishes |
| `/proc/loadavg` and `/proc/stat` | the whole `cpu` group | Omitted, the tick still publishes |
| `/proc/meminfo` unreadable, or a value isn't a number | the whole `memory` and `swap` groups | Both omitted, the tick still publishes |
| `/proc/meminfo` without `MemTotal`, or without both `MemAvailable` and the estimate lines | the whole `memory` group | Omitted, `swap` still publishes |
| `/proc/meminfo` without `SwapTotal`/`SwapFree` | the whole `swap` group | Omitted, `memory` still publishes |
| `/proc/net/dev` | the whole `network` group | Omitted, the tick still publishes |

`cpu.count` is the number of online logical CPUs (the `cpu0` … `cpuN` lines of `/proc/stat`, the same figure as `nproc`). Load average is measured against it: `load1` of 4 is a full 4-CPU server but a mostly idle 64-CPU one. Unlike `cpu.usedPercent` it needs no previous sample, so it is present from the first tick.

Memory values are bytes (`/proc/meminfo` reports kB, multiplied by 1024). `memory.availableBytes` is `MemAvailable`, not `MemFree`, so it accounts for reclaimable page cache. `memory.usedBytes` is `MemTotal - available` and `memory.usedPercent` is `used / MemTotal × 100`, so used and available always add up to total.

`MemAvailable` only exists on kernel 3.14+ (and kernels it was backported to, such as RHEL/CentOS 7). Without it, available memory is **estimated** and `memory.estimated` is `true`:

```
available = MemFree + Buffers + Cached + SReclaimable - Shmem
```

`Shmem` (tmpfs, shared memory) is counted inside `Cached` but can't be freed, so it is taken back out. The estimate is usually within a few percent of what `MemAvailable` would report and reads slightly more available, because it ignores the kernel's small emergency reserve. `memory.totalBytes` is always exact. The agent checks whether the `MemAvailable` line exists, never the kernel version, so a backported kernel reports exact values.

`swap.usedBytes` is `SwapTotal - SwapFree` and `swap.usedPercent` is `used / SwapTotal × 100`. A server with no swap reports `swap` with all values `0`.

`cpu.usedPercent` is the **mean busy percentage over the whole interval**, not an instantaneous reading — it differences two `/proc/stat` samples one `resources.interval` apart:

```
busy = (total_now - total_prev) - (idle_now - idle_prev)
pct  = 100 * busy / (total_now - total_prev)
```

`idle` counts both the `idle` and `iowait` columns, matching `top`: a server blocked on a dead NFS mount is waiting, not burning CPU, and reporting it as busy would send someone hunting the wrong problem. Load average is what surfaces that case, which is why `load1/5/15` are published alongside — the two numbers disagreeing is the signal.

`total` excludes the `guest` and `guest_nice` columns. The kernel already counts time spent running VMs inside `user` and `nice`, so adding those columns again would inflate `total` and busy time by the same amount and make `cpu.usedPercent` read too high on a host running VMs (e.g. 70% real busy reported as ~79%). On an ordinary server or inside a VM both columns are `0`, and the result is unchanged.

Because it needs two samples, `cpu.usedPercent` is **omitted on the first tick after startup**, and again on the first tick after a supervised restart (the collector is rebuilt, so the previous sample is gone). It's also omitted if the counters move backwards, which is what a reboot between ticks looks like. A short spike inside the interval is flattened into the mean; that's the intended trade, and load average is the finer-grained signal.

`network.rxBytesPerSec` (download) and `network.txBytesPerSec` (upload) are the **mean rate in bytes per second over the interval**, differencing two `/proc/net/dev` samples and dividing by the wall-clock time between them. Multiply by 8 for bits per second. They are summed over **physical interfaces only** — those with a `/sys/class/net/<iface>/device` link — so loopback, Docker bridges, veths and tunnels are excluded; counting them would double count traffic that also crosses the NIC. An interface that appears between two ticks is left out of that window, since it has no baseline.

The `network` group follows the same omission rules as `cpu.usedPercent`: absent on the first tick after startup or a supervised restart, and absent when any counter moves backwards (a driver reload). A host with no physical interface — e.g. the agent running inside a container — never reports it.

### Storage

When `storage.enabled` is `true`, the server's total local storage and the disk usage of each path in `storage.mounts` are published to `storage.channel` every `storage.interval`:

```json
{
  "systemId": "your-system-id",
  "systemName": "your-system-name",
  "serverName": "your-server-name",
  "serverIp": "10.0.0.5",
  "timestamp": "2026-05-28T10:00:00Z",
  "server": {
    "totalBytes": 225485783040,
    "usedBytes": 53502541824,
    "freeBytes": 160709131878,
    "reservedBytes": 11274109338,
    "usedPercent": 24.98,
    "partial": false
  },
  "mounts": [
    { "path": "/", "device": "/dev/sda1", "fsType": "ext4", "totalBytes": 214748364800, "usedBytes": 52428800000, "freeBytes": 151582326374, "reservedBytes": 10737238426, "usedPercent": 25.7 },
    { "path": "/var/log", "device": "/dev/sdb1", "fsType": "xfs", "totalBytes": 10737418240, "usedBytes": 1073741824, "freeBytes": 9126805504, "reservedBytes": 536870912, "usedPercent": 10.53 },
    { "path": "/mnt/nas", "fsType": "nfs4", "error": "network filesystem not supported" },
    { "path": "/missing", "error": "no such file or directory" }
  ]
}
```

Values come from `statfs` and match `df -B1` column for column:

| Field | Calculation | `df` column |
|---|---|---|
| `totalBytes` | `f_blocks × f_frsize` | 1B-blocks |
| `usedBytes` | `(f_blocks − f_bfree) × f_frsize` | Used |
| `freeBytes` | `f_bavail × f_frsize` — what a non-root app can still write | Available |
| `reservedBytes` | `(f_bfree − f_bavail) × f_frsize` — writable by root only (ext4 keeps 5% by default) | not shown |
| `usedPercent` | `used / (used + free) × 100` | Use% (df rounds up) |

`used + free + reserved = total`. The same fields and formulas are used for `server` and for each mount. `usedPercent` uses df's formula, so `100` means apps can no longer write even though root-reserved space is left. Sizes use `f_frsize`, the unit the block counts are in; `f_bsize` is only an I/O hint. Every subtraction is guarded, so a filesystem reporting inconsistent counts gets `0` rather than an underflowed number.

#### Server total

`server` is the server's **mounted local storage**, found from `/proc/self/mounts` on every tick. It does not depend on `storage.mounts`: an admin who lists only `/` still gets every local disk in the total.

1. Keep local filesystem types only: `ext2`, `ext3`, `ext4`, `xfs`, `btrfs`, `vfat`. RAM and kernel filesystems (`tmpfs`, `proc`, …), layers (`overlay`, `squashfs`), network filesystems and any unknown type are skipped.
2. Skip `/dev/loop*` devices (snaps and images).
3. Count each device once, keeping its shortest mount point, so bind mounts, Docker bind mounts and btrfs subvolumes of one disk don't double the total.
4. `statfs` each one and sum the bytes. `usedPercent` is recalculated from the sums with df's formula, never an average of per-disk percentages.

| Case | Published |
|---|---|
| Every local filesystem read | `server` with `partial: false` |
| Some local filesystem can't be statted | `server` summed without it, `partial: true` and `missingPaths` |
| None can be read, or no mount table | `server` omitted |

`partial: true` means the total is smaller than the server really has, and `missingPaths` names the mount points left out, sorted:

```json
"server": {
  "totalBytes": 214748364800,
  "usedBytes": 52428800000,
  "freeBytes": 151582326374,
  "reservedBytes": 10737238426,
  "usedPercent": 25.7,
  "partial": true,
  "missingPaths": ["/mnt/data"]
}
```

A consumer can then say which filesystem is absent without reading the agent's log, and can tell a missing filesystem apart from storage that really shrank. `missingPaths` is omitted entirely when `partial` is false. The failure is also logged, once when it starts and once when it clears.

The total measures mounted, usable storage — not the size of the physical disks: unmounted partitions, swap partitions and unallocated LVM space are not counted. On ext4 and xfs (with or without LVM or mdadm) it equals the sum of `df` for each disk. btrfs figures are the kernel's own estimate, as in `df`. A filesystem type outside the list above is not counted. Network filesystems are never touched, so the total can't block on a dead remote server.

#### Mounts

Mounts are reported in config order. A path doesn't have to be a mount point: `/var/log` reports the filesystem it lives on. `device` and `fsType` come from `/proc/self/mounts`, read once per tick — the entry whose mount point is the longest whole-component match for the path. Two paths on the same disk device (for example both `/dev/sda1`) share that disk, so filling one fills the other.

A path is matched as written; symlinks are not resolved, since resolving would touch every component of the path and could block on a dead network mount.

When a path has no reading, it is published with only an `error`, never with zero sizes that would read as an empty disk:

| Case | Published |
|---|---|
| Path can't be statted (doesn't exist, permission denied) | `{ path, error }` |
| Path is on a network filesystem (`nfs`, `nfs4`, `cifs`, `smb3`, `ceph`, `glusterfs`, `fuse.sshfs`, `9p`) | `{ path, fsType, error: "network filesystem not supported" }` |

A failing path is logged once when it starts failing and once when it becomes readable again, not on every tick, so a typo'd path doesn't flood the journal.

Network paths are **never statted**: `statfs` on a mount whose server is down can block until the server answers, which would hold up the whole storage event. The rest of the mounts still publish normally.

Only network filesystems are refused. Any other configured path is reported, including RAM-backed ones such as a `tmpfs` `/tmp`, even though those are left out of the `server` total.

If the mount table itself can't be read, paths are statted without `device`, `fsType` or the network check, and `server` is omitted.

A path that is an empty mount point with nothing mounted on it reports the filesystem it sits on (usually `/`), exactly as `df` would.

`storage.mounts` may be empty, in which case `mounts` is published as `[]` and `server` is still reported.

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

### Field presence

A value the agent cannot read is **left out of the JSON entirely** — never sent as `null`, `0` or `""`. An absent key therefore means *unknown*, which is not the same as zero: a missing `cpu.usedPercent` means the agent could not measure it, while `0` means the CPU was idle. Two absences are routine rather than faults: `cpu.usedPercent` and `network` are differences against the previous tick, so the first event after every start or restart omits them.

[**docs/consumer-contract.md**](docs/consumer-contract.md) is the full contract for whoever writes the subscriber: every event in its normal and degraded shapes, a table per event of what can be absent and why, and what a consumer has to do about it in any language.

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
- `ProtectHome=read-only` + `PrivateTmp` — home directories can be read but not written; the service gets its own `/tmp` folder, which still lives on the host's `/tmp` filesystem, so its disk usage matches `df /tmp`
- `ProtectKernelTunables` + `ProtectControlGroups` + `RestrictSUIDSGID` — no writing to `/proc/sys` or the cgroup hierarchy, can't create setuid/setgid files
- `Restart=on-failure` + `RestartSec=5` — self-heals indefinitely, including when Redis is down at boot
- `TimeoutStopSec=20` — bounded shutdown window before systemd force-kills the process

Do **not** add these settings, they hide data the agent reads:

| Setting | What breaks |
|---|---|
| `ProcSubset=pid` | hides `/proc/uptime`, `/proc/stat`, `/proc/loadavg`, `/proc/meminfo` and `/proc/net/dev`, so uptime, cpu, memory, swap and network are omitted |
| `ProtectHome=yes` or `ProtectHome=tmpfs` | a separate `/home` disk disappears from the storage `server` total |
| `InaccessiblePaths=` or `TemporaryFileSystem=` on a disk path | that disk is hidden from storage or reported as the wrong filesystem |

> No JVM flags needed — Go binaries use only what they need.
