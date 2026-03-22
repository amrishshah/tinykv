# CLAUDE.md — TinyKV

This file documents the TinyKV codebase for AI assistants. It covers architecture, conventions, and development workflows.

---

## Project Overview

TinyKV is an educational implementation of a transactional key-value store in Go. It was developed incrementally across weekly lab assignments:

- **Week 4:** Core DB with WAL, snapshots, basic transactions
- **Week 5 Part 1:** Batched writes and background flusher
- **Week 5 Part 2:** LRU cache
- **Week 6:** MVCC (Multi-Version Concurrency Control)

**Module:** `github.com/amrishshah/tinykv`
**Go version:** 1.22.3
**Package:** Single package `tinykv` — no subdirectories
**Dependencies:** stdlib only

---

## Repository Structure

```
tinykv/
├── db.go         # Core database engine (892 lines)
├── cache.go      # LRU cache implementation (120 lines)
├── db_test.go    # Test suite (1219 lines)
└── go.mod        # Module definition
```

Runtime data files (created in user-provided directory):
- `wal.log` — Write-Ahead Log
- `snapshot.db` — Periodic state snapshot

---

## Architecture

### Core Components

#### 1. `DB` struct (`db.go:251`)
The main database instance. Holds all state: MVCC data, WAL handle, write batch, LRU cache, active transactions, and background service channels.

#### 2. `VersionData` / `Version` (`db.go:184`)
MVCC storage. Each key maps to a linked list of `Version` structs. Each version tracks:
- `xmin` — transaction ID that created this version
- `xmax` — transaction ID that superseded/deleted it (`0` = still current)
- `next` — pointer to previous version (linked list)

A snapshot read finds the first version where `xmin <= snapshot && (xmax == 0 || xmax > snapshot)`.

#### 3. `Tx` struct (`db.go:24`)
A transaction. Buffers writes locally in `writes map[string]*string` (nil value = pending delete). On commit, writes are flushed to WAL and applied to MVCC. On rollback, the map is discarded.

#### 4. `LRUCache` (`cache.go`)
Thread-safe LRU cache backed by `container/list.List` and a `map[string]*list.Element`. Default capacity: 1000 entries. Maintains hit/miss statistics.

#### 5. WAL (Write-Ahead Log)
Tab-separated entries written to `wal.log`. On open, the DB replays the WAL to recover uncommitted or flushed-but-not-snapshotted writes. Format:

```
BEGIN_TX	<txid>
SET	<txid>	<key>	<value>
DELETE	<txid>	<key>
COMMIT_TX
```

#### 6. Background Services
Started in `Open()`, stopped in `Close()`:
- **Background flusher** (`startBackgroundFlusher`): Drains the write batch every 10ms or when signaled
- **Background GC** (`startBackgroundGC`): Removes obsolete MVCC versions every 30s

---

## Key Data Structures

### `Version` (`db.go:184`)
```go
type Version struct {
    value string
    xmin  uint64   // TX that created this version
    xmax  uint64   // TX that replaced/deleted it (0 = current)
    next  *Version // Linked list to older version
}
```

### `VersionData` (`db.go:191`)
```go
type VersionData struct {
    mv       sync.RWMutex
    versions map[string]*Version
}
```

### `Tx` (`db.go:24`)
```go
type Tx struct {
    db       *DB
    mu       sync.Mutex
    writes   map[string]*string // nil value = pending delete
    active   bool
    txid     uint64  // Write transaction ID
    snapshot uint64  // Read snapshot (set at Begin())
}
```

### `DB` (`db.go:251`)
Key fields:
```go
type DB struct {
    mu           sync.RWMutex
    data         *VersionData   // MVCC storage
    wal          *os.File
    nextTXID     uint64         // Atomic counter
    activeTxs    map[uint64]*Tx
    activeTxMu   sync.RWMutex
    writeBatch   []logEntry     // Pending WAL entries
    batchSize    int            // Default: 100
    flushTimeout time.Duration  // Default: 10ms
    flushSignal  chan struct{}   // Signal background flusher
    shutdownChan chan struct{}
    cache        *LRUCache
    cacheEnabled bool
    gcInterval   time.Duration  // Default: 30s
    gcRunning    atomic.Bool
}
```

### `LRUCache` (`cache.go`)
```go
type LRUCache struct {
    capacity int
    mu       sync.Mutex
    items    map[string]*list.Element
    order    *list.List
    hits     uint64
    misses   uint64
}
```

---

## Configuration Constants

All defined in `db.go`:

| Constant/Default | Value | Location |
|---|---|---|
| `compactionThreshold` | 1000 ops | `db.go:22` |
| `batchSize` | 100 entries | `db.go:306` |
| `flushTimeout` | 10ms | `db.go:307` |
| Cache capacity | 1000 entries | `db.go:310` |
| `gcInterval` | 30 seconds | `db.go:314` |

---

## Development Conventions

### Error Handling

Sentinel errors are defined at package level (`db.go:16`):
```go
var (
    ErrDBClosed    = errors.New("DB is closed")
    ErrKeyNotFound = errors.New("key not found")
    ErrTxNotActive = errors.New("transaction is not active")
)
```

- Internal errors are wrapped with `fmt.Errorf("context: %w", err)`
- Callers compare directly against sentinel values: `if err == ErrKeyNotFound`

### Synchronization

- **`sync.RWMutex`** for read-heavy structs (`DB.mu`, `VersionData.mv`, `DB.activeTxMu`)
- **`sync.Mutex`** for write-exclusive structs (`DB.batchMu`, `Tx.mu`, `LRUCache.mu`)
- **Atomic operations** for counters: `atomic.AddUint64(&db.nextTXID, 1)`
- **`atomic.Bool`** for state flags: `db.gcRunning`
- **Channel signaling** (non-blocking) for flusher:
  ```go
  select {
  case db.flushSignal <- struct{}{}:
  default: // already signaled, skip
  }
  ```
- **`sync.WaitGroup`** to wait for background goroutines on shutdown

### Cleanup

Always use `defer` for lock release and file cleanup:
```go
db.mu.Lock()
defer db.mu.Unlock()
```

### Nil-Pointer Semantics in `Tx.writes`

`map[string]*string` uses pointer-to-string:
- Key absent → no pending write
- Key present, value `nil` → pending delete
- Key present, value non-nil → pending set

This lets the transaction distinguish "not touched" from "explicitly deleted".

### Snapshot Atomicity

Snapshots are written to a `.tmp` file and renamed atomically (`os.Rename`) to prevent corrupt reads during a crash (`db.go:689`).

---

## Build & Test

### Build
```bash
go build ./...
```

### Test
```bash
go test ./...                           # Run all tests
go test -v ./...                        # Verbose output
go test -v -run TestTransactionAtomicity ./...  # Single test
go test -bench . ./...                  # Run benchmarks
go test -race ./...                     # Race detector (recommended)
```

### Benchmarks
```
BenchmarkWritesWithBatching
BenchmarkWritesNoBatching
BenchmarkGetWithCache
BenchmarkGetWithoutCache
```

---

## Usage Example

```go
db, err := tinykv.Open("/path/to/data")
if err != nil { ... }
defer db.Close()

// Direct writes (non-transactional)
db.Set("key", "value")
val, err := db.Get("key")

// Transactional writes
tx, err := db.Begin()
tx.Set("account", "alice")
tx.Set("balance", "1000")
if err := tx.Commit(); err != nil {
    tx.Rollback()
}

// Maintenance
db.Flush()    // Force write batch to disk
db.Compact()  // Snapshot + clear WAL
```

---

## Test Structure

Tests are organized in `db_test.go` by feature area:

| Category | Tests |
|---|---|
| Transactions | `TestTransactionAtomicity`, `TestIncompleteTransaction`, `TestTransactionRollback`, `TestReadYourOwnWrites` |
| Concurrency | `TestMassiveConcurrentReads`, `TestConcurrentReadWrite`, `TestConcurrentTransactions`, `TestConcurrentReadWriteDelete` |
| Compaction | `TestCompaction`, `TestCompactionWithUpdates`, `TestCompactionPreservesTransactions`, `TestCompactionSizeReduction` |
| Batching | `TestBatchingPersistence`, `TestBatchFullFlush`, `TestExplicitFlush`, `TestCompactionFlushesFirst` |
| Cache | `TestCacheInvalidationOnSet`, `TestCacheInvalidationOnDelete`, `TestLRUEviction` |
| MVCC | `TestMVCCSnapshotIsolation`, `TestMVCCConcurrentReads`, `TestMVCCVersionChainWalk`, `TestMVCCReadOldVersion`, `TestMVCCDeleteVisibility`, `TestMVCCPersistence` |

Each test uses `t.TempDir()` for an isolated, auto-cleaned data directory.

---

## Key Algorithms

### MVCC Read (`db.go:221`)
Walk the version chain for a key and return the first version visible to the given snapshot:
```go
for version != nil {
    if version.xmin <= snapshot && (version.xmax == 0 || version.xmax > snapshot) {
        return version.value, nil
    }
    version = version.next
}
```

### Garbage Collection (`db.go:823`)
Find the oldest active transaction snapshot; remove any MVCC version where `xmax < oldestSnapshot` (invisible to all current readers). Always retains the latest version of each key.

### WAL Replay (`db.go:341`)
On open, scan `wal.log` line by line. Reconstruct the MVCC version chain from committed transactions; discard entries from incomplete transactions (no matching `COMMIT_TX`).

### Compaction (`db.go:655`)
1. Flush any pending write batch
2. Write snapshot of current live versions to `snapshot.db.tmp`, then rename atomically
3. Truncate `wal.log`
4. Reset operation counter

---

## Git History / Development Progression

```
4f8d656  MVCC implementation        (Week 6)
7dae920  Week 5 - LRU Cache
ff89fc5  week 5 part 1 -- buffer write
e40c447  week 4 building tinykv
```

When adding features, follow the weekly progression pattern: each layer builds on the previous. The test suite should continue to pass after each change.
