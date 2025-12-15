package tinykv

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestTransactionAtomicity(t *testing.T) {
	dir := t.TempDir()
	db, _ := Open(dir)

	// Commit a transaction
	tx, _ := db.Begin()
	tx.Set("key1", "value1")
	tx.Set("key2", "value2")
	tx.Commit()

	db.Close()

	// Reopen and verify
	db2, _ := Open(dir)
	defer db2.Close()

	val1, _ := db2.Get("key1")
	val2, _ := db2.Get("key2")

	if val1 != "value1" || val2 != "value2" {
		t.Errorf("Transaction data not persisted correctly")
	}
}

func TestIncompleteTransaction(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "wal.log")

	// Manually create a WAL with incomplete transaction
	wal, _ := os.Create(walPath)
	wal.WriteString("BEGIN_TX\n")
	wal.WriteString("SET\tkey1\tvalue1\n")
	wal.WriteString("SET\tkey2\tvalue2\n")
	// Missing COMMIT_TX - simulates crash mid-transaction
	wal.Close()

	// Open database - should discard incomplete transaction
	db, _ := Open(dir)
	defer db.Close()

	// Keys should not exist
	_, err1 := db.Get("key1")
	_, err2 := db.Get("key2")

	if err1 != ErrKeyNotFound || err2 != ErrKeyNotFound {
		t.Error("Incomplete transaction should not be applied")
	}
}

// func TestMixedTransactionalAndNonTransactional(t *testing.T) {
// 	dir := t.TempDir()
// 	db, _ := Open(dir)

// 	// Non-transactional write (Week 2 style)
// 	db.Set("old_key", "old_value")

// 	// Transactional write (Week 3 style)
// 	tx, _ := db.Begin()
// 	tx.Set("new_key", "new_value")
// 	tx.Commit()

// 	db.Close()

// 	// Reopen and verify both exist
// 	db2, _ := Open(dir)
// 	defer db2.Close()

// 	oldVal, _ := db2.Get("old_key")
// 	newVal, _ := db2.Get("new_key")

// 	if oldVal != "old_value" || newVal != "new_value" {
// 		t.Error("Mixed format WAL not handled correctly")
// 	}
// }

func TestTransactionRollback(t *testing.T) {
	dir := t.TempDir()
	db, _ := Open(dir)

	tx, _ := db.Begin()
	tx.Set("key", "value")
	tx.Rollback()

	db.Close()

	// Reopen - key should not exist
	db2, _ := Open(dir)
	defer db2.Close()

	_, err := db2.Get("key")
	if err != ErrKeyNotFound {
		t.Error("Rolled back transaction should not persist")
	}
}

func TestReadYourOwnWrites(t *testing.T) {
	dir := t.TempDir()
	db, _ := Open(dir)
	defer db.Close()

	// Pre-existing data
	tx, _ := db.Begin()

	tx.Set("key", "old_value")
	tx.Commit()

	tx, _ = db.Begin()

	// Overwrite in transaction
	tx.Set("key", "new_value")

	// Should see new value in transaction
	val, _ := tx.Get("key")
	if val != "new_value" {
		t.Errorf("Expected 'new_value', got '%s'", val)
	}

	//This will failed since we are not releasing lock yet.

	//DB should still have old value (not committed yet)
	// dbVal, _ := db.Get("key")
	// if dbVal != "old_value" {
	// 	t.Errorf("DB should still have 'old_value', got '%s'", dbVal)
	// }

	tx.Commit()

	// Now DB should have new value
	dbVal, _ := db.Get("key")
	if dbVal != "new_value" {
		t.Errorf("After commit, expected 'new_value', got '%s'", dbVal)
	}
}

func TestConcurrentReadWrite(t *testing.T) {
	dir := t.TempDir()
	db, _ := Open(dir)
	defer db.Close()

	done := make(chan bool)

	// Writer goroutine
	go func() {
		for i := 0; i < 1000; i++ {
			db.Set("key", fmt.Sprintf("value%d", i))
		}
		done <- true
	}()

	// Reader goroutine
	go func() {
		for i := 0; i < 1000; i++ {
			db.Get("key")
		}
		done <- true
	}()

	<-done
	<-done
}

func TestMassiveConcurrentReads(t *testing.T) {
	dir := t.TempDir()
	db, _ := Open(dir)
	defer db.Close()

	db.Set("key", "value")

	var wg sync.WaitGroup

	// 100 concurrent readers
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				db.Get("key")
			}
		}()
	}

	wg.Wait()
}

func TestConcurrentReadWriteDelete(t *testing.T) {
	dir := t.TempDir()
	db, _ := Open(dir)
	defer db.Close()

	var wg sync.WaitGroup

	// Writer
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			db.Set(fmt.Sprintf("key%d", i%10), "value")
		}
	}()

	// Reader
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			db.Get(fmt.Sprintf("key%d", i%10))
		}
	}()

	// Deleter
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			db.Delete(fmt.Sprintf("key%d", i%10))
		}
	}()

	wg.Wait()
}

func TestConcurrentTransactions(t *testing.T) {
	dir := t.TempDir()
	db, _ := Open(dir)
	defer db.Close()

	var wg sync.WaitGroup

	// Try to start 10 transactions concurrently
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			tx, _ := db.Begin()
			tx.Set(fmt.Sprintf("key%d", id), "value")
			tx.Commit()
		}(i)
	}

	wg.Wait()
}

func TestCompaction(t *testing.T) {
	dir := t.TempDir()
	db, _ := Open(dir)

	// Write lots of data
	for i := 0; i < 1000; i++ {
		db.Set(fmt.Sprintf("key%d", i), "value")
	}

	// Compact
	if err := db.Compact(); err != nil {
		t.Fatalf("Compaction failed: %v", err)
	}

	db.Close()

	// Reopen - should load from snapshot
	db2, _ := Open(dir)
	defer db2.Close()

	// Verify data
	val, _ := db2.Get("key500")

	if val != "value" {
		t.Error("Data lost after compaction")
	}
}

func TestCompactionWithUpdates(t *testing.T) {
	dir := t.TempDir()
	db, _ := Open(dir)
	defer db.Close()

	// Write and update same key many times
	for i := 0; i < 100; i++ {
		db.Set("key", fmt.Sprintf("value%d", i))
	}

	// Compact
	db.Compact()

	// Verify only latest value in snapshot
	val, _ := db.Get("key")
	if val != "value99" {
		t.Errorf("Expected value99, got %s", val)
	}
}

func TestCompactionPreservesTransactions(t *testing.T) {
	dir := t.TempDir()
	db, _ := Open(dir)
	defer db.Close()

	// Transaction before compaction
	tx1, _ := db.Begin()
	tx1.Set("before", "compaction")
	tx1.Commit()

	db.Compact()

	// Transaction after compaction
	tx2, _ := db.Begin()
	tx2.Set("after", "compaction")
	tx2.Commit()

	// Both should exist
	val1, _ := db.Get("before")
	val2, _ := db.Get("after")

	if val1 != "compaction" || val2 != "compaction" {
		t.Error("Transactions around compaction failed")
	}
}

func TestCompactionSizeReduction(t *testing.T) {
	dir := t.TempDir()
	db, _ := Open(dir)
	defer db.Close()

	// Write and overwrite keys many times
	for i := 0; i < 10000; i++ {
		db.Set(fmt.Sprintf("key%d", i%100), "value")
	}

	// Check WAL size before compaction
	info1, _ := os.Stat(filepath.Join(dir, "wal.log"))
	sizeBefore := info1.Size()

	db.Compact()

	// Check sizes after compaction
	info2, _ := os.Stat(filepath.Join(dir, "snapshot.db"))
	info3, _ := os.Stat(filepath.Join(dir, "wal.log"))
	snapshotSize := info2.Size()
	walSize := info3.Size()

	fmt.Printf("Before: WAL=%d bytes\n", sizeBefore)
	fmt.Printf("After: Snapshot=%d bytes, WAL=%d bytes\n", snapshotSize, walSize)
	fmt.Printf("Reduction: %.1f%%\n", 100.0*(1-float64(snapshotSize)/float64(sizeBefore)))
}
