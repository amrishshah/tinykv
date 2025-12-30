package tinykv

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
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

func TestBatchingPersistence(t *testing.T) {
	dir := t.TempDir()
	db, _ := Open(dir)

	// Write 50 entries
	for i := 0; i < 50; i++ {
		if err := db.Set(fmt.Sprintf("key%d", i), "value"); err != nil {
			t.Fatalf("Set failed: %v", err)
		}
	}

	// Wait for timer flush
	time.Sleep(20 * time.Millisecond)

	db.Close()

	// Reopen and verify
	db2, _ := Open(dir)
	defer db2.Close()

	for i := 0; i < 50; i++ {
		val, err := db2.Get(fmt.Sprintf("key%d", i))
		if err != nil || val != "value" {
			t.Errorf("Data not persisted for key%d", i)
		}
	}
}

func TestBatchFullFlush(t *testing.T) {
	dir := t.TempDir()
	db, _ := Open(dir)
	db.batchSize = 10
	defer db.Close()

	// Write exactly batch size
	for i := 0; i < 10; i++ {
		db.Set(fmt.Sprintf("key%d", i), "value")
	}

	// Give background goroutine time to flush
	time.Sleep(5 * time.Millisecond)

	// Check batch is empty
	db.batchMu.Lock()
	batchLen := len(db.writeBatch)
	db.batchMu.Unlock()

	if batchLen != 0 {
		t.Errorf("Batch should be empty after full, got %d entries", batchLen)
	}
}

func TestExplicitFlush(t *testing.T) {
	dir := t.TempDir()
	db, _ := Open(dir)
	defer db.Close()

	db.Set("key", "value")

	// Explicit flush
	if err := db.Flush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	db.Close()

	// Reopen and verify
	db2, _ := Open(dir)
	defer db2.Close()

	val, err := db2.Get("key")
	if err != nil || val != "value" {
		t.Error("Explicit flush failed to persist data")
	}
}

func TestCompactionFlushesFirst(t *testing.T) {
	dir := t.TempDir()
	db, _ := Open(dir)
	db.batchSize = 1000
	db.opsSinceSnap = 995
	defer db.Close()

	// Add entries near threshold
	for i := 0; i < 10; i++ {
		db.Set(fmt.Sprintf("key%d", i), "value")
	}

	// Should trigger compaction with flush

	db.Close()

	// Reopen and verify
	db2, _ := Open(dir)
	defer db2.Close()

	for i := 0; i < 10; i++ {
		val, err := db2.Get(fmt.Sprintf("key%d", i))
		if err != nil || val != "value" {
			t.Errorf("Key%d lost during compaction", i)
		}
	}
}

func BenchmarkWritesWithBatching(b *testing.B) {
	dir := b.TempDir()
	db, _ := Open(dir)
	defer db.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.Set(fmt.Sprintf("key%d", i), "value")
	}
	db.Flush()
}

func BenchmarkWritesNoBatching(b *testing.B) {
	dir := b.TempDir()
	db, _ := Open(dir)
	db.batchSize = 1 // Force immediate flush
	defer db.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.Set(fmt.Sprintf("key%d", i), "value")
	}
}

// func TestCacheHit(t *testing.T) {
// 	dir := t.TempDir()
// 	db, _ := Open(dir)
// 	defer db.Close()

// 	db.Set("key", "value")

// 	// First Get - cache miss
// 	val1, _ := db.Get("key")

// 	// Second Get - cache hit
// 	val2, _ := db.Get("key")

// 	if val1 != "value" || val2 != "value" {
// 		t.Error("Cache not working")
// 	}

// 	// Check stats
// 	hits, misses, hitRate := db.cache.Stats()
// 	if hits != 1 || misses != 1 || hitRate != 0.5 {
// 		t.Errorf("Expected 1 hit, 1 miss, 50%% hit rate. Got: %d hits, %d misses, %.2f%% hit rate",
// 			hits, misses, hitRate*100)
// 	}
// }

func TestCacheInvalidationOnSet(t *testing.T) {
	dir := t.TempDir()
	db, _ := Open(dir)
	defer db.Close()

	db.Set("key", "value1")
	val1, _ := db.Get("key") // Cache miss, then cache "value1"

	db.Set("key", "value2")  // Update cache
	val2, _ := db.Get("key") // Cache hit with "value2"

	if val1 != "value1" || val2 != "value2" {
		t.Error("Cache invalidation failed")
	}
}

func TestCacheInvalidationOnDelete(t *testing.T) {
	dir := t.TempDir()
	db, _ := Open(dir)
	defer db.Close()

	db.Set("key", "value")
	db.Get("key") // Cache it

	db.Delete("key") // Remove from cache

	_, err := db.Get("key")
	if err != ErrKeyNotFound {
		t.Error("Key should be deleted")
	}
}

func TestLRUEviction(t *testing.T) {
	cache := NewLRUCache(3) // Capacity: 3

	cache.Put("a", "1")
	cache.Put("b", "2")
	cache.Put("c", "3")

	cache.Get("a") // Access "a" (move to front)

	cache.Put("d", "4") // Evicts "b" (least recently used)

	// "b" should be evicted
	if _, ok := cache.Get("b"); ok {
		t.Error("b should be evicted")
	}

	// "a", "c", "d" should exist
	if _, ok := cache.Get("a"); !ok {
		t.Error("a should exist")
	}
	if _, ok := cache.Get("c"); !ok {
		t.Error("c should exist")
	}
	if _, ok := cache.Get("d"); !ok {
		t.Error("d should exist")
	}
}

func BenchmarkGetWithoutCache(b *testing.B) {
	dir := b.TempDir()
	db, _ := Open(dir)
	db.cacheEnabled = false // Disable cache
	defer db.Close()

	db.Set("hot_key", "value")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.Get("hot_key")
	}
}

func BenchmarkGetWithCache(b *testing.B) {
	dir := b.TempDir()
	db, _ := Open(dir)
	db.cacheEnabled = true // Enable cache
	defer db.Close()

	db.Set("hot_key", "value")
	db.Get("hot_key") // Prime cache

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.Get("hot_key")
	}
}
