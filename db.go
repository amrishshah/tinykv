package tinykv

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrDBClosed    = errors.New("DB is closed")
	ErrKeyNotFound = errors.New("key not found")
	ErrTxNotActive = errors.New("transaction is not active")
)

const compactionThreshold = 1000 // Compact after 1000 operations

type Tx struct {
	db       *DB
	mu       sync.Mutex
	writes   map[string]*string
	active   bool
	txid     uint64 //Write Transaction
	snapshot uint64 // Read Transaction ..
}

func (db *DB) Begin() (*Tx, error) {

	if db.closed {
		return nil, ErrDBClosed
	}

	txid := atomic.AddUint64(&db.nextTXID, 1)

	tx := &Tx{
		db:       db,
		txid:     txid,
		snapshot: txid,
		writes:   make(map[string]*string),
		active:   true,
	}

	db.activeTxMu.Lock()
	db.activeTxs[txid] = tx
	db.activeTxMu.Unlock()

	return tx, nil
}

func (tx *Tx) Set(key, value string) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if !tx.active {
		return ErrTxNotActive
	}

	tx.writes[key] = &value
	return nil
}

func (tx *Tx) Delete(key string) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if !tx.active {
		return ErrTxNotActive
	}
	tx.writes[key] = nil // nil means deleted
	return nil
}

func (tx *Tx) Get(key string) (string, error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if !tx.active {
		return "", ErrTxNotActive
	}

	if value, exists := tx.writes[key]; exists {
		if value != nil {
			return *value, nil
		} else {
			return "", ErrKeyNotFound
		}
	}

	value, err := tx.db.data.Get(key, tx.snapshot)
	if err != nil {
		return "", err
	}

	return value, nil

}

func (tx *Tx) Rollback() error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if !tx.active {
		return ErrTxNotActive
	}

	tx.active = false
	tx.writes = nil

	// Unregister transaction
	tx.db.activeTxMu.Lock()
	delete(tx.db.activeTxs, tx.txid)
	tx.db.activeTxMu.Unlock()

	return nil
}

func (tx *Tx) Commit() error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if !tx.active {
		return ErrTxNotActive
	}

	// Flush any pending batched writes
	if err := tx.db.Flush(); err != nil {
		return err
	}

	// Acquire db.mu for writing to WAL and updating data
	tx.db.mu.Lock()
	defer tx.db.mu.Unlock()

	// Write transaction marker with TXID
	if err := tx.db.Write(fmt.Sprintf("BEGIN_TX\t%d", tx.txid)); err != nil {
		return err
	}

	for key, value := range tx.writes {
		switch value {
		case nil:
			// Include TXID
			if err := tx.db.Write(fmt.Sprintf("DELETE\t%d\t%s", tx.txid, key)); err != nil {
				return err
			}
		default:
			// Include TXID
			if err := tx.db.Write(fmt.Sprintf("SET\t%d\t%s\t%s", tx.txid, key, *value)); err != nil {
				return err
			}
		}
	}

	if err := tx.db.Write("COMMIT_TX"); err != nil {
		return err
	}

	tx.active = false

	// Update memory with versioned data
	for key, value := range tx.writes {
		switch value {
		case nil:
			tx.db.data.Delete(key, tx.txid)
		default:
			tx.db.data.Set(key, *value, tx.txid)
		}
	}

	// Unregister transaction
	tx.db.activeTxMu.Lock()
	delete(tx.db.activeTxs, tx.txid)
	tx.db.activeTxMu.Unlock()

	return nil
}

type Version struct {
	value string
	xmin  uint64
	xmax  uint64
	next  *Version
}

type VersionData struct {
	mv       sync.RWMutex
	versions map[string]*Version
}

func NewVersionedData() *VersionData {
	return &VersionData{
		versions: make(map[string]*Version),
	}
}

func (vd *VersionData) Set(key, value string, txid uint64) {
	vd.mv.Lock()
	defer vd.mv.Unlock()

	// Mark old version as superseded
	if oldVersion := vd.versions[key]; oldVersion != nil && oldVersion.xmax == 0 {
		oldVersion.xmax = txid // Mark as replaced by this transaction
	}

	newversion := &Version{
		value: value,
		xmin:  txid,
		xmax:  0,
		next:  vd.versions[key],
	}

	vd.versions[key] = newversion
}

func (vd *VersionData) Get(key string, snapshot uint64) (string, error) {
	vd.mv.RLock()
	defer vd.mv.RUnlock()
	version := vd.versions[key]
	for version != nil {
		if version.xmin <= snapshot && (version.xmax == 0 || version.xmax > snapshot) {
			return version.value, nil
		}
		version = version.next
	}
	return "", ErrKeyNotFound
}

func (vd *VersionData) Delete(key string, txid uint64) error {
	vd.mv.Lock()
	defer vd.mv.Unlock()

	version := vd.versions[key]
	if version == nil {
		return ErrKeyNotFound
	}

	// Mark current version as deleted
	if version.xmax == 0 {
		version.xmax = txid
	}

	return nil
}

type DB struct {
	mu           sync.RWMutex
	data         *VersionData
	closed       bool
	wal          *os.File
	walPath      string
	snapshotPath string
	lastSnapshot uint64 // NEW - operation count at last snapshot
	opsSinceSnap uint64 // NEW - operations since last snapshot

	nextTXID   uint64         // Atomic counter
	activeTxs  map[uint64]*Tx // Track active transactions
	activeTxMu sync.RWMutex

	writeBatch   []logEntry
	batchMu      sync.Mutex
	batchSize    int
	flushTimeout time.Duration

	// Background flusher
	flushSignal  chan struct{}  // Signal to flush immediately
	shutdownChan chan struct{}  // Signal to stop background goroutine
	flushDone    sync.WaitGroup // Wait for flusher to finish

	cache        *LRUCache
	cacheEnabled bool // Allow disabling for testing

	gcInterval time.Duration
	gcDone     sync.WaitGroup
	lastGCTime time.Time
	gcRunning  atomic.Bool
}

type logEntry struct {
	op    string // SET and DELETE
	key   string
	value string // empty for delete
	txid  uint64
}

func Open(dir string) (*DB, error) {

	//WAL
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		return nil, fmt.Errorf("directory does not exist: %s", dir)
	}

	walPath := filepath.Join(dir, "wal.log")
	snapshotPath := filepath.Join(dir, "snapshot.db")

	db := &DB{
		data:         NewVersionedData(),
		walPath:      walPath,
		snapshotPath: snapshotPath,
		writeBatch:   make([]logEntry, 0, 100),
		batchSize:    100,
		flushTimeout: 10 * time.Millisecond,
		flushSignal:  make(chan struct{}, 1),
		shutdownChan: make(chan struct{}),
		cache:        NewLRUCache(1000),
		cacheEnabled: true,
		nextTXID:     0,
		activeTxs:    make(map[uint64]*Tx),
		gcInterval:   30 * time.Second,
	}

	//loadSnashot
	if err := db.LoadSnapshot(); err != nil {
		return nil, err
	}

	wal, err := os.OpenFile(walPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL: %w", err)
	}

	db.wal = wal

	// 5. Replay log
	if err := db.Replay(); err != nil {
		wal.Close()
		return nil, err
	}

	db.startBackgroundFlusher()
	db.startBackgroundGC()

	return db, nil
}

func (db *DB) Replay() error {
	if _, err := db.wal.Seek(0, 0); err != nil {
		return fmt.Errorf("failed to seek: %w", err)
	}

	scanner := bufio.NewScanner(db.wal)
	inTx := false
	var currentTxid uint64
	txWrites := make(map[string]*string)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		parts := strings.Split(line, "\t")
		if len(parts) < 1 {
			continue
		}

		switch parts[0] {
		case "BEGIN_TX":
			// Format: BEGIN_TX	<txid>
			if len(parts) >= 2 {
				fmt.Sscanf(parts[1], "%d", &currentTxid)

				// Update nextTXID
				if currentTxid >= db.nextTXID {
					db.nextTXID = currentTxid + 1
				}
			}
			inTx = true
			txWrites = make(map[string]*string)

		case "COMMIT_TX":
			if inTx {
				// Apply all buffered writes with proper TXID
				for key, value := range txWrites {
					if value == nil {
						db.data.Delete(key, currentTxid)
					} else {
						db.data.Set(key, *value, currentTxid)
					}
				}
			}
			inTx = false
			txWrites = make(map[string]*string)

		case "SET":
			if inTx {
				// Inside transaction: Format is SET	<txid>	<key>	<value>
				if len(parts) >= 4 {
					v := parts[3]
					txWrites[parts[2]] = &v
				}
			} else {
				// Outside transaction: Format is SET	<txid>	<key>	<value>
				if len(parts) >= 4 {
					var txid uint64
					fmt.Sscanf(parts[1], "%d", &txid)

					// Update nextTXID
					if txid >= db.nextTXID {
						db.nextTXID = txid + 1
					}

					db.data.Set(parts[2], parts[3], txid)
				}
			}

		case "DELETE":
			if inTx {
				// Inside transaction: Format is DELETE	<txid>	<key>
				if len(parts) >= 3 {
					txWrites[parts[2]] = nil
				}
			} else {
				// Outside transaction: Format is DELETE	<txid>	<key>
				if len(parts) >= 3 {
					var txid uint64
					fmt.Sscanf(parts[1], "%d", &txid)

					// Update nextTXID
					if txid >= db.nextTXID {
						db.nextTXID = txid + 1
					}

					db.data.Delete(parts[2], txid)
				}
			}
		}
	}

	return scanner.Err()
}

func (db *DB) Write(data string) error {
	_, err := db.wal.Write([]byte(data + "\n"))
	if err != nil {
		return err
	}
	return db.wal.Sync()
}

func (db *DB) Set(key, value string) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrDBClosed
	}
	// if err := db.Write("SET\t" + key + "\t" + value); err != nil {
	// 	return err
	// }

	txid := atomic.AddUint64(&db.nextTXID, 1)

	db.AddtoBatch(logEntry{
		op:    "SET",
		key:   key,
		value: value,
		txid:  txid,
	})

	//Update cache
	if db.cacheEnabled {
		db.cache.Put(key, value) // Update with new value
	}

	db.data.Set(key, value, txid)

	db.opsSinceSnap++

	if db.opsSinceSnap >= compactionThreshold {
		if err := db.Flush(); err != nil {
			return err
		}
		if err := db.CompactwithoutLock(); err != nil {
			return err
		}
	}

	return nil
}

func (db *DB) Get(key string) (string, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return "", ErrDBClosed
	}
	if db.cacheEnabled {
		if value, exists := db.cache.Get(key); exists {
			return value, nil
		}
	}
	const maxSnapshot = ^uint64(0)

	value, err := db.data.Get(key, maxSnapshot)
	if err != nil {
		return "", err
	}

	// Update cache
	if db.cacheEnabled {
		db.cache.Put(key, value) // Update with new value
	}

	return value, nil
}

func (db *DB) Delete(key string) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrDBClosed
	}
	// if err := db.Write("DELETE\t" + key); err != nil {
	// 	return err
	// }

	txid := atomic.AddUint64(&db.nextTXID, 1)

	db.AddtoBatch(logEntry{
		op:    "DELETE",
		key:   key,
		value: "",
		txid:  txid,
	})

	db.data.Delete(key, txid)

	// Invalidate cache
	if db.cacheEnabled {
		db.cache.Remove(key) // Remove from cache
	}

	db.opsSinceSnap++

	if db.opsSinceSnap >= compactionThreshold {
		if err := db.Flush(); err != nil {
			return err
		}
		if err := db.CompactwithoutLock(); err != nil {
			return err
		}
	}

	return nil
}

func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrDBClosed
	}
	close(db.shutdownChan)
	db.flushDone.Wait()
	db.gcDone.Wait()

	if db.cacheEnabled {
		db.cache.Clear() // Clear cache
	}

	db.wal.Close()
	db.closed = true
	db.data = nil
	return nil
}

func (db *DB) Has(key string) (bool, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.closed {
		return false, ErrDBClosed
	}

	const maxSnapshot = ^uint64(0)
	_, err := db.data.Get(key, maxSnapshot)

	return err == nil, nil
}

func (db *DB) Len() (int, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.closed {
		return 0, ErrDBClosed
	}

	db.data.mv.RLock()
	count := len(db.data.versions)
	db.data.mv.RUnlock()

	return count, nil
}

func (db *DB) Keys() ([]string, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.closed {
		return nil, ErrDBClosed
	}

	db.data.mv.RLock()
	dbkeys := make([]string, 0, len(db.data.versions))
	for key := range db.data.versions {
		dbkeys = append(dbkeys, key)
	}
	db.data.mv.RUnlock()

	sort.Strings(dbkeys)
	return dbkeys, nil
}

func (db *DB) CompactwithoutLock() error {
	if err := db.Snapshot(); err != nil {
		return err
	}

	//Close WAL
	if err := db.wal.Close(); err != nil {
		return err
	}

	wal, err := os.OpenFile(db.walPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("failed to open WAL: %w", err)
	}

	db.wal = wal
	db.lastSnapshot = db.opsSinceSnap
	db.opsSinceSnap = 0
	return nil
}

func (db *DB) Compact() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrDBClosed
	}

	if err := db.CompactwithoutLock(); err != nil {
		return err
	}

	return nil
}

func (db *DB) Snapshot() error {
	tempPath := db.snapshotPath + ".tmp"
	tempsnap, err := os.Create(tempPath)
	if err != nil {
		return fmt.Errorf("failed to create snapshot: %w", err)
	}
	defer tempsnap.Close()

	db.data.mv.RLock()
	const maxSnapshot = ^uint64(0) // See all committed data

	// Write each key's latest visible version
	for key, version := range db.data.versions {
		curr := version
		for curr != nil {
			if curr.xmin <= maxSnapshot && (curr.xmax == 0 || curr.xmax > maxSnapshot) {
				line := fmt.Sprintf("%s\t%s\t%d\t%d\n", key, curr.value, curr.xmin, curr.xmax)
				if _, err := tempsnap.Write([]byte(line)); err != nil {
					db.data.mv.RUnlock()
					return fmt.Errorf("failed to write snapshot: %w", err)
				}
				break
			}
			curr = curr.next
		}
	}
	db.data.mv.RUnlock()

	if err := tempsnap.Sync(); err != nil {
		return err
	}

	tempsnap.Close()

	if err := os.Rename(tempPath, db.snapshotPath); err != nil {
		return err
	}

	return nil
}

func (db *DB) LoadSnapshot() error {
	if _, err := os.Stat(db.snapshotPath); os.IsNotExist(err) {
		return nil
	}

	snap, err := os.Open(db.snapshotPath)
	if err != nil {
		return err
	}
	defer snap.Close()

	scanner := bufio.NewScanner(snap)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		parts := strings.Split(line, "\t")
		if len(parts) < 4 {
			continue
		}

		key := parts[0]
		value := parts[1]
		var xmin, xmax uint64
		fmt.Sscanf(parts[2], "%d", &xmin)
		fmt.Sscanf(parts[3], "%d", &xmax)

		// Reconstruct version
		version := &Version{
			value: value,
			xmin:  xmin,
			xmax:  xmax,
			next:  nil,
		}

		db.data.mv.Lock()
		db.data.versions[key] = version
		db.data.mv.Unlock()

		// Update nextTXID
		if xmin >= db.nextTXID {
			db.nextTXID = xmin + 1
		}
		if xmax > 0 && xmax >= db.nextTXID {
			db.nextTXID = xmax + 1
		}
	}

	return scanner.Err()
}

func (db *DB) startBackgroundFlusher() {
	db.flushDone.Add(1)

	go func() {
		defer db.flushDone.Done()

		ticker := time.NewTicker(db.flushTimeout)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				db.Flush()
			case <-db.flushSignal:
				db.Flush()
			case <-db.shutdownChan:
				db.Flush()
				return
			}
		}
	}()
}

func (db *DB) AddtoBatch(log logEntry) {
	db.batchMu.Lock()
	defer db.batchMu.Unlock()

	db.writeBatch = append(db.writeBatch, log)

	//THis is important code - when channel is already full and if you push another then it will block if used without select.
	if len(db.writeBatch) >= db.batchSize {
		select {
		case db.flushSignal <- struct{}{}:
			// Signal sent successfully
		default:
			// Channel full, flush already pending
		}
	}
}

func (db *DB) Flush() error {
	db.batchMu.Lock()
	defer db.batchMu.Unlock()

	if len(db.writeBatch) == 0 {
		return nil
	}

	for _, entry := range db.writeBatch {
		var line string
		switch entry.op {
		case "DELETE":
			// Include TXID in WAL
			line = fmt.Sprintf("DELETE\t%d\t%s", entry.txid, entry.key)
		default:
			// Include TXID in WAL
			line = fmt.Sprintf("SET\t%d\t%s\t%s", entry.txid, entry.key, entry.value)
		}

		if _, err := db.wal.Write([]byte(line + "\n")); err != nil {
			return err
		}
	}

	if err := db.wal.Sync(); err != nil {
		return err
	}

	db.writeBatch = db.writeBatch[:0]

	return nil
}

func (db *DB) GarbageCollect() int {

	if !db.gcRunning.CompareAndSwap(false, true) {
		return 0 // GC already running
	}

	defer db.gcRunning.Store(false)

	db.activeTxMu.RLock()

	// Find oldest active transaction
	oldestSnapshot := db.nextTXID
	for _, tx := range db.activeTxs {
		if tx.snapshot < oldestSnapshot {
			oldestSnapshot = tx.snapshot
		}
	}
	db.activeTxMu.RUnlock()

	// Clean versions older than oldest snapshot
	db.data.mv.Lock()
	defer db.data.mv.Unlock()

	versionsRemoved := 0

	for _, version := range db.data.versions {
		// Keep latest version always
		if version.next == nil {
			continue
		}

		// Walk chain, remove old versions
		prev := version
		curr := version.next

		for curr != nil {
			// If this version is invisible to all active txs
			if curr.xmax != 0 && curr.xmax < oldestSnapshot {
				// Remove from chain
				prev.next = curr.next
				versionsRemoved++

				curr = curr.next
			} else {
				prev = curr
				curr = curr.next
			}
		}
	}
	db.lastGCTime = time.Now()
	return versionsRemoved
}

func (db *DB) startBackgroundGC() {
	db.gcDone.Add(1)

	go func() {
		defer db.gcDone.Done()
		ticker := time.NewTicker(db.gcInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				db.GarbageCollect()
			case <-db.shutdownChan:
				return
			}
		}
	}()
}
