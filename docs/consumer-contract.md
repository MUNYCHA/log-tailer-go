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
7. **Every collector publishes once at startup**, then on its interval. A
   consumer sees an event immediately after an agent restart, out of step with
   the interval it was expecting. It carries no marker saying so.
8. **New fields get added over time.** A consumer must ignore keys it does not
   recognise rather than reject the event.

Rules 2 and 8 are the two that cause real incidents. The rest are convenience.

## Identity

Every `logs`, `resources` and `storage` event opens with the same two fields,
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

Percentages are unrounded floats in `[0, 100]`. Byte counts are whole bytes and
can exceed 2³¹, so a 32-bit integer type is not enough. `rxBytesPerSec` and
`txBytesPerSec` are means over the interval just ended, not instantaneous.

### First tick after start

**This is not an error and a consumer will see it several times a day** — on
every agent start, config reload and supervised restart.

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

`cpu.usedPercent` and the whole `network` group are absent. Both are
differences between two ticks, and the first tick has nothing to difference
against. The next event carries them.

The agent publishes this event immediately on startup rather than waiting out
an interval, so it arrives as soon as the agent is up and the complete one
follows a full interval later.

Do not alarm on this, and do not backfill it with `0`.

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
| `cpu.usedPercent` | same, **or the first tick after start** |
| `cpu.load1`, `load5`, `load15` | `/proc/loadavg` unreadable — all three together, never one alone |
| `memory` | `/proc/meminfo` unreadable, or its memory lines are missing |
| `swap` | `/proc/meminfo` unreadable, or its swap lines are missing |
| `network` | `/proc/net/dev` unreadable, **or the first tick after start** |

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
`partial`, `path`, and the numbers *inside* a group, which are guaranteed
whenever the group itself is there.

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
