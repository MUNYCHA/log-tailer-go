# log-tailer-go — Design

Each feature has its own section: event shape, config, logic, and what happens
when data is missing. To add a feature later, add a new section and a new row
in the progress table.

---

## Progress

Tick a box when that item is built, tested and merged to `dev`.

| # | Feature | Item | Done |
|---|---|---|---|
| 1 | Config | Replace `metrics` block with `resources` + `storage` blocks | [x] |
| 2 | Layout | Move `metrics/*` into `resources/` and `storage/` | [x] |
| 3 | Resources | Separate Resources event with grouped shape | [x] |
| 4 | Resources — uptime | Optional (no longer skips the event) | [ ] |
| 5 | Resources — cpu | Add `count` | [ ] |
| 6 | Resources — memory | Add `usedBytes`, `usedPercent` | [ ] |
| 7 | Resources — memory | Estimate when `MemAvailable` is missing + `estimated` flag | [ ] |
| 8 | Resources — swap | Add `usedPercent`; sent even when memory fails | [ ] |
| 9 | Resources — network | Move into `network` group | [x] |
| 10 | Storage | Separate Storage event | [x] |
| 11 | Storage — mount | Use `f_frsize` instead of `f_bsize` | [ ] |
| 12 | Storage — mount | `usedPercent` with df formula | [ ] |
| 13 | Storage — mount | Add `reservedBytes` | [ ] |
| 14 | Storage — mount | Add `device`, `fsType` from mount table | [ ] |
| 15 | Storage — mount | Network paths reported as not supported | [ ] |
| 16 | Storage — mount | `mounts` optional in config | [ ] |
| 17 | Storage — server total | `server` group from mount table | [ ] |
| 18 | Storage — server total | `partial` flag | [ ] |

---

## 1. Requirements

| Item | Requirement |
|---|---|
| OS | Linux |
| Kernel | 3.2+ (Go 1.25+ runtime). 3.14+ gives exact memory; 3.2–3.13 gives estimated memory |
| Go | 1.25+ to build |
| Install | Physical servers only (never inside a VM), run by systemd as a normal user |

**systemd settings that must never be added** (they hide data the agent reads):

| Setting | What breaks |
|---|---|
| `ProcSubset=pid`, `ProtectProc=` | cpu, memory, swap missing |
| `ProtectHome=yes` / `tmpfs` | `/home` disk hidden from storage |
| `InaccessiblePaths=`, `TemporaryFileSystem=` on a disk path | that disk hidden or wrong |

---

## 2. Config

```yaml
redis:
  addr: 127.0.0.1:6379
  password: ""
  db: 0

identity:
  system:
    id: sys-001
    name: billing
  server:
    name: app-01
    ip: 10.0.0.21

logTailer:
  enabled: true
  files:
    - path: /var/log/app/app.log
      channel: app-logs

resources:
  enabled: true
  channel: server-resources
  interval: 30s

storage:
  enabled: true
  channel: server-storage
  interval: 5m
  mounts:            # optional
    - /
    - /var/log

heartbeat:
  enabled: true
  channel: agent-heartbeat
  interval: 10s
```

### Validation (only when the block is enabled)

| Block | Rules |
|---|---|
| `resources` | `channel` required; `interval` positive duration |
| `storage` | `channel` required; `interval` positive duration; each `mounts` entry non-empty; `mounts` may be empty |

The old `metrics` block is removed (no fallback).

---

## 3. Code layout

```
resources/
├── collector.go
├── uptime/
├── cpu/
├── load/
├── memory/
└── network/
storage/
├── collector.go
└── disk/
```

Every metric folder has the same files:

| File | Job | Touches the server? |
|---|---|---|
| `read.go` | Get raw data from Linux | Yes, the only file that does |
| `parse.go` | Raw data → numbers | No |
| `calculate.go` | Numbers → published values | No |
| `<name>_test.go` | Tests with fake input | No |

---

## 4. Event rules (all events)

| Rule | Detail |
|---|---|
| Identity | flat at the top of every event |
| Grouping | related values in a group; small events stay flat |
| Sizes | whole bytes |
| Percent | 0–100, decimal |
| Missing data | field or group omitted, never a fake `0` |
| Guards | no subtraction below 0 (uint64 wrap-around), no divide by zero |
| Crash | never; a panic restarts only that component after 1 s |

---

## 5. Resources event

Channel: `resources.channel` · Interval: `resources.interval`

```json
{
  "systemId": "sys-001",
  "systemName": "billing",
  "serverName": "app-01",
  "serverIp": "10.0.0.21",
  "timestamp": "2026-09-17T03:00:00Z",
  "uptimeSeconds": 123456,

  "cpu": {
    "count": 8,
    "usedPercent": 12.5,
    "load1": 0.03,
    "load5": 0.08,
    "load15": 0.07
  },

  "memory": {
    "totalBytes": 16466857984,
    "usedBytes": 2453692416,
    "availableBytes": 14013165568,
    "usedPercent": 14.9,
    "estimated": false
  },

  "swap": {
    "totalBytes": 4294967296,
    "usedBytes": 0,
    "usedPercent": 0
  },

  "network": {
    "rxBytesPerSec": 10240,
    "txBytesPerSec": 5120
  }
}
```

### 5.1 Uptime

| Field | Source | Logic |
|---|---|---|
| `uptimeSeconds` | `/proc/uptime` | whole seconds |

| Missing case | Result |
|---|---|
| `/proc/uptime` unreadable | `uptimeSeconds` omitted, event still sent |

### 5.2 CPU

```json
"cpu": { "count": 8, "usedPercent": 12.5, "load1": 0.03, "load5": 0.08, "load15": 0.07 }
```

| Field | Source | Logic |
|---|---|---|
| `count` | `/proc/stat` | number of `cpu0…cpuN` lines |
| `usedPercent` | `/proc/stat` `cpu` line | between 2 ticks: `(Δtotal − Δidle) ÷ Δtotal × 100` |
| `load1/5/15` | `/proc/loadavg` | taken as is |

`/proc/stat` columns: `user nice system idle iowait irq softirq steal guest guest_nice`

| Column | Counted as |
|---|---|
| user, nice, system, irq, softirq, steal | busy |
| idle, iowait | idle |
| guest, guest_nice | skipped (already inside user/nice) |

`steal` is always 0 on physical servers.

| Missing case | Result |
|---|---|
| First tick after start | `usedPercent` omitted |
| Counters went backwards | `usedPercent` omitted that tick |
| `/proc/stat` unreadable | `count`, `usedPercent` omitted |
| `/proc/loadavg` unreadable | `load1/5/15` omitted |
| Both unreadable | `cpu` omitted |

### 5.3 Memory

```json
"memory": { "totalBytes": 16466857984, "usedBytes": 2453692416, "availableBytes": 14013165568, "usedPercent": 14.9, "estimated": false }
```

Source: `/proc/meminfo`, kB × 1024, read once per tick. Lines read every tick:
`MemTotal`, `MemAvailable`, `MemFree`, `Buffers`, `Cached`, `Shmem`, `SReclaimable`.

```
if MemAvailable exists:
    available = MemAvailable                                         estimated = false
else:
    available = MemFree + Buffers + Cached − Shmem + SReclaimable    estimated = true

usedBytes   = MemTotal − available     (guard: not below 0)
usedPercent = used ÷ MemTotal × 100
```

| `estimated` | Meaning |
|---|---|
| `false` | exact, from kernel `MemAvailable` |
| `true` | not exact, our formula (usually within a few %) |

`totalBytes` is always exact. `MemFree` is only used for the estimate and is not sent.

`MemAvailable` exists from kernel 3.14 (also backported on RHEL/CentOS 7). The
agent checks whether the line exists, not the kernel version.

| Missing case | Result |
|---|---|
| No `MemAvailable` | estimate, `estimated: true` |
| `/proc/meminfo` unreadable | `memory` omitted |

### 5.4 Swap

```json
"swap": { "totalBytes": 4294967296, "usedBytes": 0, "usedPercent": 0 }
```

| Field | Logic |
|---|---|
| `totalBytes` | `SwapTotal × 1024` |
| `usedBytes` | `(SwapTotal − SwapFree) × 1024` (guard: not below 0) |
| `usedPercent` | `used ÷ total × 100`, `0` when no swap |

| Missing case | Result |
|---|---|
| No swap on server | all `0` |
| No `MemAvailable` | swap still sent |
| `/proc/meminfo` unreadable | `swap` omitted |

### 5.5 Network

```json
"network": { "rxBytesPerSec": 10240, "txBytesPerSec": 5120 }
```

| Field | Source | Logic |
|---|---|---|
| `rxBytesPerSec` | `/proc/net/dev` column 0 | Δbytes ÷ seconds between 2 ticks, summed over physical NICs |
| `txBytesPerSec` | `/proc/net/dev` column 8 | same |

Physical NIC = has `/sys/class/net/<name>/device`.

| Interface | Counted |
|---|---|
| `eth0`, `eno1`, bond members | yes |
| `lo`, `docker0`, `br0`, `virbr0`, `vnet`/`tap`, `bond0`, VLANs | no (traffic already on the physical NIC) |

| Missing case | Result |
|---|---|
| First tick after start | `network` omitted |
| A NIC counter went backwards | `network` omitted that tick |
| NIC appeared mid-window | left out of that tick |

---

## 6. Storage event

Channel: `storage.channel` · Interval: `storage.interval`

```json
{
  "systemId": "sys-001",
  "systemName": "billing",
  "serverName": "app-01",
  "serverIp": "10.0.0.21",
  "timestamp": "2026-09-17T03:00:00Z",

  "server": {
    "totalBytes": 2180612804608,
    "usedBytes": 98038161408,
    "freeBytes": 1972589973504,
    "reservedBytes": 109984669696,
    "usedPercent": 4.74,
    "partial": false
  },

  "mounts": [
    {
      "path": "/",
      "device": "/dev/sda1",
      "fsType": "ext4",
      "totalBytes": 1081101176832,
      "usedBytes": 49019080704,
      "freeBytes": 977089740800,
      "reservedBytes": 54992355328,
      "usedPercent": 4.78
    },
    { "path": "/mnt/nas", "fsType": "nfs4", "error": "network filesystem not supported" },
    { "path": "/missing", "error": "no such file or directory" }
  ]
}
```

### 6.1 Disk math (shared by mounts and server total)

Raw from `statfs`: `f_blocks`, `f_bfree`, `f_bavail`, `f_frsize`.

| Field | Logic | `df -B1` column |
|---|---|---|
| `totalBytes` | `f_blocks × f_frsize` | 1B-blocks |
| `usedBytes` | `(f_blocks − f_bfree) × f_frsize` | Used |
| `freeBytes` | `f_bavail × f_frsize` | Available |
| `reservedBytes` | `(f_bfree − f_bavail) × f_frsize` (guard: not below 0) | not shown |
| `usedPercent` | `used ÷ (used + free) × 100` | Use% (df rounds up) |

- `f_frsize` is the unit of the block counts; `f_bsize` is not used.
- `used + free + reserved = total`.
- `reservedBytes` = space only root can write (ext4 keeps 5%).
- `usedPercent` 100% = normal apps can't write.

### 6.2 Mount table

Source: `/proc/self/mounts`, read once per tick, used by both 6.3 and 6.4.

- Each line → `device`, `path`, `fsType`; decode `\040` (space) and other octal escapes.
- **Local** types: `ext2, ext3, ext4, xfs, btrfs, vfat`
- **Network** types: `nfs, nfs4, cifs, smb3, ceph, glusterfs, fuse.sshfs, 9p`
- Everything else (tmpfs, proc, overlay, squashfs, …) is ignored.

### 6.3 Mounts (from config)

| Field | Logic |
|---|---|
| `path` | from config |
| `device` | mount table, longest mount point matching the path |
| `fsType` | same |
| sizes and percent | 6.1 on `statfs(path)` |

- Checked in config order, one after another.
- A path doesn't need to be a mount point: `/var/log` shows the disk it lives on.
- No dedup here; every configured path is listed.

| Missing case | Result |
|---|---|
| Network filesystem | `{ path, fsType, error: "network filesystem not supported" }`, no `statfs` |
| `statfs` fails | `{ path, error }`, other paths continue |
| `mounts` empty or omitted | `mounts` empty, `server` still sent |

### 6.4 Server total

```json
"server": { "totalBytes": 2180612804608, "usedBytes": 98038161408, "freeBytes": 1972589973504, "reservedBytes": 109984669696, "usedPercent": 4.74, "partial": false }
```

```
mount table
  → keep local types only
  → skip /dev/loop*
  → sort by path, shortest first
  → keep first entry per device
  → statfs each
       error      → partial = true, skip
       total == 0 → skip
  → sum totalBytes, usedBytes, freeBytes, reservedBytes
  → usedPercent from the sums (df formula)
```

- Does not depend on config `mounts`.
- `totalBytes` = mounted, usable storage (not physical disk size).
- Percent is from the sums, never an average of each disk's percent.

| `partial` | Meaning |
|---|---|
| `false` | every local filesystem was read |
| `true` | at least one couldn't be read; total is smaller than reality |

| Missing case | Result |
|---|---|
| Some filesystem unreadable | summed without it, `partial: true` |
| No filesystem readable | `server` omitted |

| Setup | Accuracy |
|---|---|
| ext4 / xfs (with or without LVM, mdadm) | exact, same as `df` |
| btrfs single disk | kernel estimate, same as `df` |
| btrfs multi-disk | free can be far off (kernel estimate) |
| Type not in local list | not counted |

---

## 7. Log event (unchanged)

Channel: `logTailer.files[].channel`

```json
{
  "systemId": "sys-001",
  "systemName": "billing",
  "serverName": "app-01",
  "serverIp": "10.0.0.21",
  "path": "/var/log/app/app.log",
  "channel": "app-logs",
  "timestamp": "2026-09-17T03:00:00Z",
  "message": "user login ok"
}
```

---

## 8. Heartbeat event (unchanged)

Channel: `heartbeat.channel` (default `agent-heartbeat`)

```json
{ "systemId": "sys-001", "serverName": "app-01" }
```
