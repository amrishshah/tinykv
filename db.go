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
	"time"
)

var (
	ErrDBClosed    = errors.New("DB is closed")
	ErrKeyNotFound = errors.New("key not found")
	ErrTxNotActive = errors.New("transaction is not active")
)

const compactionThreshold = 1000 // Compact after 1000 operations

type Tx struct {
	db     *DB
	mu     sync.Mutex
	writes map[string]*string
	active bool
}

func (db *DB) Begin() (*Tx, error) {
	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		return nil, ErrDBClosed
	}

	return &Tx{
		db:     db,
		writes: make(map[string]*string),
		active: true,
	}, nil
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

	value, exists := tx.db.data[key]
	if !exists {
		return "", ErrKeyNotFound
	}

	return value, nil

}

func (tx *Tx) Rollback() error {
	tx.mu.Lock()
	defer tx.db.mu.Unlock()
	defer tx.mu.Unlock()
	if !tx.active {
		return ErrTxNotActive
	}
	tx.active = false
	tx.writes = nil
	return nil
}

func (tx *Tx) Commit() error {
	tx.mu.Lock()
	defer tx.db.mu.Unlock()
	defer tx.mu.Unlock()
	if !tx.active {
		return ErrTxNotActive
	}

	// Flush any pending batched writes
	if err := tx.db.Flush(); err != nil {
		return err
	}

	if err := tx.db.Write("BEGIN_TX"); err != nil {
		return err
	}

	for key, value := range tx.writes {
		switch value {
		case nil:
			if err := tx.db.Write("DELETE\t" + key); err != nil {
				return err
			}
		default:
			if err := tx.db.Write("SET\t" + key + "\t" + *value); err != nil {
				return err
			}
		}
	}

	if err := tx.db.Write("COMMIT_TX"); err != nil {
		return err
	}

	tx.active = false

	//update Memory
	for key, value := range tx.writes {
		switch value {
		case nil:
			delete(tx.db.data, key)
		default:
			tx.db.data[key] = *value
		}
	}

	return nil
}

type DB struct {
	mu           sync.RWMutex
	data         map[string]string
	closed       bool
	wal          *os.File
	walPath      string
	snapshotPath string
	lastSnapshot uint64 // NEW - operation count at last snapshot
	opsSinceSnap uint64 // NEW - operations since last snapshot

	writeBatch   []logEntry
	batchMu      sync.Mutex
	batchSize    int
	flushTimeout time.Duration

	// Background flusher
	flushSignal  chan struct{}  // Signal to flush immediately
	shutdownChan chan struct{}  // Signal to stop background goroutine
	flushDone    sync.WaitGroup // Wait for flusher to finish

}

type logEntry struct {
	op    string // SET and DELETE
	key   string
	value string // empty for delete
}

func Open(dir string) (*DB, error) {

	//WAL
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		return nil, fmt.Errorf("directory does not exist: %s", dir)
	}

	walPath := filepath.Join(dir, "wal.log")
	snapshotPath := filepath.Join(dir, "snapshot.db")

	db := &DB{
		data:         make(map[string]string),
		walPath:      walPath,
		snapshotPath: snapshotPath,
		writeBatch:   make([]logEntry, 0, 100),
		batchSize:    100,
		flushTimeout: 10 * time.Millisecond,
		flushSignal:  make(chan struct{}, 1),
		shutdownChan: make(chan struct{}),
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

	return db, nil
}

func (db *DB) Replay() error {
	if _, err := db.wal.Seek(0, 0); err != nil {
		return fmt.Errorf("failed to seek: %w", err)
	}

	scanner := bufio.NewScanner(db.wal)
	inTx := false
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
			inTx = true
			txWrites = make(map[string]*string)

		case "COMMIT_TX":
			if inTx {
				for key, value := range txWrites {
					if value == nil {
						delete(db.data, key)
					} else {
						db.data[key] = *value
					}
				}
			}
			inTx = false
			txWrites = make(map[string]*string)

		case "SET":
			if len(parts) != 3 {
				continue
			}
			if inTx {
				v := parts[2]
				txWrites[parts[1]] = &v
			} else {
				db.data[parts[1]] = parts[2]
			}

		case "DELETE":
			if len(parts) != 2 {
				continue
			}
			if inTx {
				txWrites[parts[1]] = nil
			} else {
				delete(db.data, parts[1])
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

	db.AddtoBatch(logEntry{
		op:    "SET",
		key:   key,
		value: value,
	})

	db.data[key] = value
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
	value, exists := db.data[key]
	if !exists {
		return "", ErrKeyNotFound
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

	db.AddtoBatch(logEntry{
		op:    "DELETE",
		key:   key,
		value: "",
	})
	delete(db.data, key)

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
	_, exists := db.data[key]
	return exists, nil
}

func (db *DB) Len() (int, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return 0, ErrDBClosed
	}
	return len(db.data), nil
}

func (db *DB) Keys() ([]string, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return nil, ErrDBClosed
	}

	dbkeys := []string{}

	for key := range db.data {
		dbkeys = append(dbkeys, key)
	}

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

	for key, value := range db.data {
		if _, err := tempsnap.Write([]byte(key + "\t" + value + "\n")); err != nil {
			return fmt.Errorf("failed to write snapshot: %w", err)
		}
	}

	if err := tempsnap.Sync(); err != nil {
		return err
	}

	// Close before rename (required on Windows)
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
		part := strings.Split(line, "\t")
		if len(part) < 2 {
			continue
		}
		db.data[part[0]] = part[1]
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
		switch entry.op {
		case "DELETE":
			if _, err := db.wal.Write([]byte("DELETE\t" + entry.key + "\n")); err != nil {
				return err
			}
		default:
			if _, err := db.wal.Write([]byte("SET\t" + entry.key + "\t" + entry.value + "\n")); err != nil {
				return err
			}
		}
	}

	if err := db.wal.Sync(); err != nil {
		return err
	}
	db.writeBatch = db.writeBatch[:0]

	return nil
}
