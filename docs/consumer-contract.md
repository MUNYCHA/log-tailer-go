# Consumer contract

What a subscriber can rely on in every event this agent publishes, and what it
must be ready to find missing. The agent is written in Go, but nothing here
assumes anything about the language on the other side.

Read this alongside the [README's Message Format section](../README.md#message-format),
which explains what each value *means*; this file is about which keys are
guaranteed to be there.

## The rules

1. **A value that cannot be read is left out of the JSON.** Never sent as
   `null`, `0` or `""`.
2. **Absent means unknown, not zero.** A missing `cpu.usedPercent` means the
   agent could not measure it. A `usedPercent` of `0` means the CPU was idle.
   A consumer that maps the first to the second will chart a busy server as an
   idle one, and an unreachable disk as an empty one.
3. **An event never fails as a whole.** One unreadable file costs that one
   group; everything else in the event still publishes.
4. **`serverId` and `timestamp` are always present** on every event — see
   [Identity](#identity).
5. **Groups are all-or-nothing.** If a group is present, every field inside it
   is present. There is no half-filled group.
6. **Lists are always present**, as `[]` when empty. Never `null`, never
   absent.
7. **The interval-driven events — `resources`, `storage`, `docker` and
   `heartbeat` — publish once a second after startup**, then on their own intervals. A
   consumer sees one event per channel right after an agent restart, out of
   step with the interval it was expecting and carrying no marker saying so.
   It is a complete event, but any rate in it is measured over that one second
   rather than a full interval, so on a long interval the first point covers a
   much shorter window than the rest. `logs` is not on a schedule: an event is
   published when a line is written.
8. **New fields get added over time.** A consumer must ignore keys it does not
   recognise rather than reject the event.

Rules 2 and 8 are the two that cause real incidents. The rest are convenience.

## Identity

Every `logs`, `resources`, `storage` and `docker` event opens with the same two fields,
in the same order, always present:

```json
{
  "serverId": "your-server-id",
  "timestamp": "2026-05-28T10:00:00Z"
}
```

`serverId` identifies exactly one server and is the only thing to join on. It
is an opaque string — treat it as such rather than parsing it, since its format
is the operator's choice and can differ between fleets. Anything else about the
server (hostname, address, which system it belongs to) is the consumer's to
hold, looked up from this id; the agent deliberately does not copy it onto
every event, where it would be a duplicate that goes stale.

`timestamp` is RFC 3339, always UTC, and is when the agent built the event, not
when it was received.

`heartbeat` carries only `serverId`.

## logs

One event per log line. Fixed shape — every field is always present.

```json
{
  "serverId": "your-server-id",
  "path": "/var/log/app/app.log",
  "channel": "your-channel-1",
  "timestamp": "2026-05-28T10:00:00Z",
  "message": "the raw log line"
}
```

There is no degraded form. A line the agent cannot read produces no event at
all rather than an event with an error in it.

`message` can be empty only for a whitespace-only source line, and is capped at
1 MB — a longer line arrives as consecutive events, so a consumer reassembling
multi-line stack traces should not assume one event is one line.

## heartbeat

```json
{ "serverId": "your-server-id" }
```

Fixed shape, the one field always present. No degraded form. The beat carries
no measurements, reads no files and shares
no state with the other collectors, so there is nothing in it that can fail
independently. If a beat arrives, the agent is alive.

Absence of beats is the signal: expire a server's liveness key on roughly three
missed intervals.

## resources

One event per interval. Everything below the identity fields is optional.

### Normal

```json
{
  "serverId": "your-server-id",
  "timestamp": "2026-09-18T04:44:50Z",
  "uptimeSeconds": 7782,
  "cpu": {
    "count": 24,
    "usedPercent": 0.2777777777777778,
    "load1": 0.07,
    "load5": 0.09,
    "load15": 0.09
  },
  "memory": {
    "totalBytes": 16466862080,
    "usedBytes": 2808463360,
    "availableBytes": 13658398720,
    "usedPercent": 17.055243107981386,
    "estimated": false
  },
  "swap": {
    "totalBytes": 4294967296,
    "usedBytes": 0,
    "usedPercent": 0
  },
  "network": {
    "rxBytesPerSec": 0,
    "txBytesPerSec": 0
  }
}
```

Percentages are unrounded floats in `[0, 100]` (the docker event's
`cpuPercent` is the exception — see [docker](#docker)). Byte counts are whole bytes and
can exceed 2³¹, so a 32-bit integer type is not enough. `rxBytesPerSec` and
`txBytesPerSec` are means over the interval just ended, not instantaneous.

### Rates missing

`cpu.usedPercent` and the `network` group are the two values measured between
two readings rather than read directly. Normally both are present from the
first event — the agent takes a baseline reading at startup and
publishes a second later. They drop out when their source file cannot be read or parsed, when the kernel
counters move backwards — what a reboot between two readings looks like — or
when there is nothing to compare: no interface common to both readings, or CPU
counters that have not advanced at all.

```json
{
  "serverId": "your-server-id",
  "timestamp": "2026-09-18T04:44:50Z",
  "uptimeSeconds": 7782,
  "cpu": {
    "count": 24,
    "load1": 0.07,
    "load5": 0.09,
    "load15": 0.09
  },
  "memory": { "totalBytes": 16466862080, "usedBytes": 2808463360, "availableBytes": 13658398720, "usedPercent": 17.055243107981386, "estimated": false },
  "swap": { "totalBytes": 4294967296, "usedBytes": 0, "usedPercent": 0 }
}
```

When that happens the keys are absent rather than zero. Do not backfill them
with `0`: a missing rate means unmeasured, and charting it as 0 reads as an
idle CPU or a silent network.

### Degraded

Each source fails independently. A server where `/proc/loadavg` cannot be read
but everything else can:

```json
{
  "serverId": "your-server-id",
  "timestamp": "2026-09-18T04:44:50Z",
  "uptimeSeconds": 7782,
  "cpu": { "count": 24, "usedPercent": 0.28 },
  "memory": { "totalBytes": 16466862080, "usedBytes": 2808463360, "availableBytes": 13658398720, "usedPercent": 17.05, "estimated": false },
  "swap": { "totalBytes": 4294967296, "usedBytes": 0, "usedPercent": 0 },
  "network": { "rxBytesPerSec": 0, "txBytesPerSec": 0 }
}
```

The worst case, where nothing at all could be read, is still a valid event:

```json
{
  "serverId": "your-server-id",
  "timestamp": "2026-09-18T04:44:50Z"
}
```

### What can be absent

| Key | Absent when |
|---|---|
| `uptimeSeconds` | `/proc/uptime` unreadable |
| `cpu` | neither `/proc/stat` nor `/proc/loadavg` readable |
| `cpu.count` | `/proc/stat` unreadable or unparseable |
| `cpu.usedPercent` | same, the counters moved backwards (a reboot between readings), or they did not advance at all between two readings |
| `cpu.load1`, `load5`, `load15` | `/proc/loadavg` unreadable — all three together, never one alone |
| `memory` | `/proc/meminfo` unreadable, or its memory lines are missing |
| `swap` | `/proc/meminfo` unreadable, or its swap lines are missing |
| `network` | `/proc/net/dev` unreadable, the counters moved backwards, or no physical interface is present in both readings |

A server with no swap reports `swap` with all three fields `0` — that is a
measurement, not an absence.

`memory.estimated` is `true` when the kernel had no `MemAvailable` and the
value was derived from reclaimable memory instead: close, not exact. `total` is
always exact.

## storage

One event per interval: a server-wide total plus one entry per configured
mount.

### Normal

```json
{
  "serverId": "your-server-id",
  "timestamp": "2026-09-18T04:24:43Z",
  "server": {
    "totalBytes": 1081233945600,
    "usedBytes": 49551924224,
    "freeBytes": 976679315456,
    "reservedBytes": 55002705920,
    "usedPercent": 4.828533990005148,
    "partial": false
  },
  "mounts": [
    {
      "path": "/",
      "device": "/dev/sda1",
      "fsType": "ext4",
      "totalBytes": 1081101176832,
      "usedBytes": 49485991936,
      "freeBytes": 976622829568,
      "reservedBytes": 54992355328,
      "usedPercent": 4.822684582661206
    }
  ]
}
```

`server` and each mount use the same five size fields, matching `df -B1`:
`usedBytes + freeBytes + reservedBytes = totalBytes`, and `usedPercent` is
`used / (used + free) × 100`, so `100` means an ordinary process can no longer
write even though root-reserved space remains.

`server` is every local filesystem on the machine, counted once each,
independent of which mounts are configured. The same filesystem can appear both
in `server` and in `mounts` — they are two views, not a partition of one.

### Incomplete total

When a local filesystem cannot be read it is dropped from the sum,
`partial` becomes `true` and `missingPaths` names what was left out:

```json
{
  "serverId": "your-server-id",
  "timestamp": "2026-09-18T04:30:00Z",
  "server": {
    "totalBytes": 214748364800,
    "usedBytes": 52428800000,
    "freeBytes": 151582326374,
    "reservedBytes": 10737238426,
    "usedPercent": 25.7,
    "partial": true,
    "missingPaths": ["/mnt/data"]
  },
  "mounts": []
}
```

`partial: true` means **the totals are real but too low** — treat them as a
floor. A drop in `totalBytes` on a partial event is a missing filesystem, not
storage that shrank, so suppress capacity alerts for that tick.

`missingPaths` is sorted and only present when `partial` is `true`. `partial`
itself is always present when `server` is, so it is the field to branch on.

### Failed mount

A configured path that cannot be read is still listed, so the consumer can see
it is down rather than silently losing the row. It carries `error` **instead
of** the five size fields:

```json
{
  "mounts": [
    { "path": "/", "device": "/dev/sda1", "fsType": "ext4", "totalBytes": 1081101176832, "usedBytes": 49485991936, "freeBytes": 976622829568, "reservedBytes": 54992355328, "usedPercent": 4.82 },
    { "path": "/mnt/nas", "fsType": "nfs4", "error": "network filesystem not supported" },
    { "path": "/mnt/data", "error": "no such file or directory" }
  ]
}
```

The sizes are omitted rather than zeroed, because a zeroed row reads as a disk
that is empty. Branch on the presence of `error`, or equivalently on the
absence of `totalBytes`.

A path on a network filesystem is reported with a fixed `error` and is never
statted, so an unreachable NFS server cannot delay the event.

### Nothing readable

```json
{
  "serverId": "your-server-id",
  "timestamp": "2026-09-18T04:30:00Z",
  "mounts": []
}
```

`server` is omitted entirely rather than published as zeros. `mounts` is still
there as an empty list — it is a list, and lists are always present.

### What can be absent

| Key | Absent when |
|---|---|
| `server` | no local filesystem could be read, or the mount table is unreadable |
| `server.missingPaths` | the total is complete (`partial` is `false`) |
| `mounts[].device`, `mounts[].fsType` | that path was not found in the mount table |
| `mounts[].totalBytes` … `usedPercent` | that path could not be read — `error` is present instead |
| `mounts[].error` | that path was read successfully |

`mounts` itself is never absent and never `null`: `[]` when no mounts are
configured. `mounts[].path` is never absent.

## docker

One event per interval listing every container on the server, running or not,
sorted by `name`. Running and paused containers carry stats; every other state
carries how it last exited.

### Normal

```json
{
  "serverId": "your-server-id",
  "timestamp": "2026-09-21T08:40:00Z",
  "cgroupVersion": 2,
  "containers": [
    {
      "id": "fa5eb4ae064f7908a3a6ed36514e5c97b1d56e05274daccbe6d84cfbb51ec338",
      "name": "mysql-mysql-1",
      "image": "mysql:8",
      "state": "exited",
      "exitCode": 255,
      "finishedAt": "2026-09-11T02:39:19Z"
    },
    {
      "id": "190a28d8ed2ac9890a44c6d69aee47fd4d5dde9b75f375d165afa3eadadc18cc",
      "name": "redis-redis-1",
      "image": "redis:8-alpine",
      "state": "running",
      "cpuPercent": 0.15313115399763685,
      "memory": { "usedBytes": 10194944, "limitBytes": 16466882560, "usedPercent": 0.0619118036632187 },
      "network": { "rxBytes": 3160697, "txBytes": 358542 },
      "blockIO": { "readBytes": 35926016, "writeBytes": 0 },
      "pids": 22
    }
  ]
}
```

`id`, `name`, `image` and `state` are always present on every container.
Branch on `state`, not on which keys happen to be there: `running` and
`paused` are the states that get stats, every other state gets `exitCode` and
`finishedAt`. `state` is Docker's own vocabulary and a future Docker could add
a value, so treat an unknown state as "not running".

Three values differ from the `resources` event and are easy to misread:

- **`cpuPercent` is not capped at 100.** It is a share of one core, like
  `docker stats`: a container busy on two cores reads `200`, and the ceiling
  is 100 × the host's CPU count. Do not chart it on a 0–100 axis next to
  `resources.cpu.usedPercent`, which is a share of the whole machine.
- **`network` and `blockIO` are totals since the container started**, not
  rates. They reset to `0` when the container restarts. To chart a rate,
  difference two events and treat a drop as a restart, not negative traffic.
- **`memory.limitBytes` is the host's memory** when the container has no
  limit, so `usedPercent` is then a share of the host.

### Block I/O and `cgroupVersion`

`cgroupVersion` says which kernel accounting Docker's counters come from. It
matters for one field. On `1`, buffered writes are flushed to disk by a kernel
thread and charged to that thread rather than the container, so
`blockIO.writeBytes` **may be undercounted**, often down to `0`. On `2` they
are charged to the container.

| `cgroupVersion` | `writeBytes: 0` means |
|---|---|
| `2` | the container wrote nothing to disk |
| `1` | it wrote nothing, **or** its writes were not attributed to it — show it as "may be undercounted", not as a confident zero |
| absent | not known yet — Docker has not answered once since the agent started |

Compare `writeBytes` only between servers with the same `cgroupVersion`.
Every other field is comparable across all servers.

### Docker unreachable

Docker not installed, its socket missing, or the daemon down:

```json
{
  "serverId": "your-server-id",
  "timestamp": "2026-09-21T08:40:00Z",
  "containers": [],
  "error": "Get \"http://docker/containers/json?all=true\": dial unix /var/run/docker.sock: connect: no such file or directory"
}
```

**Check `error` before reading an empty `containers` list.** Without `error`,
`[]` means the server really has no containers; with it, the agent could not
see them. A dashboard that skips this check shows every container on a server
vanishing whenever the daemon restarts. The event still arrives, so a missing
docker event means the agent is down, never Docker.

### One container failing

A container whose stats or details could not be read keeps its metadata and
carries `error` instead of its stats or exit fields:

```json
{ "id": "…", "name": "api", "image": "api:1.4", "state": "running", "error": "timed out" }
```

`"timed out"` means the request, or the whole round, ran out of time: a round
is cut off at the interval, capped at 10 seconds, so a daemon that answers the
list but hangs on stats still gets its event published on time. Any other
`error` is Docker's own message.

A running container with neither stats nor `error` stopped between the list
and its stats request; the next event shows its new state.

### What can be absent

| Key | Absent when |
|---|---|
| `cgroupVersion` | Docker has not answered `/info` since the agent started |
| `error` (event) | the container list was read |
| `containers[].cpuPercent` | the first round after a container starts, restarts, or is created while the agent runs — on a 3s or longer interval only when that happened in the last second before the round — or Docker reports no CPU count |
| `containers[].memory` | Docker sent no memory figures for it |
| `containers[].network` | the container uses host or no networking |
| `containers[].blockIO` | not running, or its stats failed |
| `containers[].pids` | Docker too old to report it |
| `containers[].exitCode`, `finishedAt` | running or paused, never started, started again mid-round, or its details failed — always together |
| `containers[].error` | its stats or details were read |
| every stats field above | the container is not running or paused, stopped mid-round, or has `error` |

`exitCode` `0` is a clean exit and is published; only a never-started
container omits it. `finishedAt` is RFC 3339 UTC like `timestamp`.

## Mapping this in any language

The single rule is **absent is a valid value, not a parse error**, and it must
stay distinguishable from zero. How much work that takes depends on the
language:

| Language | An absent key gives you | What to write |
|---|---|---|
| Go | the zero value, automatically | nothing; use pointers where `0` is ambiguous |
| Java, C#, Kotlin | the field's default | a nullable/boxed type — see the hazard below |
| TypeScript, JavaScript | `undefined` | `?:` in the type, `?? []` at the use site |
| Python | `KeyError` on `d["k"]` | `d.get("k")`, or a dataclass with defaults |
| Rust (serde) | a decode error | `Option<T>`, or `#[serde(default)]` |
| Ruby, PHP | `nil` / `null` | nothing |
| SQL (JSONB, etc.) | `NULL` | `coalesce(...)` for lists; keep `NULL` for scalars |

**The hazard is primitive numeric types.** In any language where a number
cannot be null, an absent key silently becomes `0`:

```
uptimeSeconds declared as a 64-bit int   → absent becomes 0  → a server up for
                                                                ten days charts
                                                                as just rebooted
usedPercent declared as a float          → absent becomes 0.0 → a busy CPU
                                                                charts as idle
```

So: **every optional scalar needs a type that can hold "no value"** — nullable,
boxed, `Option`, or a sentinel you check explicitly. Use a plain primitive only
for the fields listed as always present: `serverId`, `timestamp`,
`partial`, `path`, a container's `id`, `name`, `image` and `state`, and the
numbers *inside* a group, which are guaranteed whenever the group itself is
there.

**Ignore unknown keys.** Fields get added to these events — `missingPaths` was
added after the first release. A strict decoder that rejects unrecognised keys
will break on an agent upgrade nobody told it about. Turn that check off, or
decode into an open map.

Nothing is ever removed or renamed without a version bump, so a consumer that
ignores unknown keys and treats absent as unknown keeps working across agent
upgrades.

## Transport

Redis Pub/Sub, one channel per event type, names set in the agent's config.

Pub/Sub has **no persistence and no delivery guarantee**. A message published
while no subscriber is connected is discarded, not queued. A consumer that
reconnects has no way to fetch what it missed, so gaps are normal and the
consumer must not assume an unbroken series. Every event carries its own
`serverId` and `timestamp` for exactly that reason: they are independent
snapshots, not a stream that has to be replayed in order.

Publishes are fire-and-forget. The agent logs a failure and drops the message;
it never retries and never blocks the next tick.
