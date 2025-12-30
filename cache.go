package tinykv

import (
	"container/list"
	"sync"
)

type LRUCache struct {
	capacity int
	mu       sync.Mutex
	items    map[string]*list.Element
	order    *list.List
	// Stats
	hits   uint64
	misses uint64
}

type cacheEntry struct {
	key   string
	value string
}

func NewLRUCache(capacity int) *LRUCache {

	if capacity <= 0 {
		capacity = 1 // Minimum capacity
	}

	return &LRUCache{
		capacity: capacity,
		items:    make(map[string]*list.Element),
		order:    list.New(),
	}
}

func (c *LRUCache) Get(key string) (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	elem, exists := c.items[key]

	if !exists {
		c.misses++       // Track miss
		return "", false // Cache miss
	}

	c.hits++

	// Move to front (most recently used)
	c.order.MoveToFront(elem)

	entry := elem.Value.(*cacheEntry)
	return entry.value, true // Cache hit
}

func (c *LRUCache) Put(key, value string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Check if key already exists
	if elem, exists := c.items[key]; exists {
		// Update existing entry
		c.order.MoveToFront(elem)
		entry := elem.Value.(*cacheEntry)
		entry.value = value
		return
	}

	// Add new entry
	entry := &cacheEntry{key: key, value: value}
	elem := c.order.PushFront(entry)
	c.items[key] = elem

	// Evict if over capacity
	if c.order.Len() > c.capacity {
		c.evictOldest()
	}
}

func (c *LRUCache) evictOldest() {
	// Assumes lock is already held
	oldest := c.order.Back()
	if oldest != nil {
		c.order.Remove(oldest)
		entry := oldest.Value.(*cacheEntry)
		delete(c.items, entry.key)
	}
}

func (c *LRUCache) Remove(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if elem, exists := c.items[key]; exists {
		c.order.Remove(elem)
		delete(c.items, key)

	}
}

func (c *LRUCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.hits = 0   // Reset
	c.misses = 0 // Reset
	c.items = make(map[string]*list.Element)
	c.order = list.New()
}

func (c *LRUCache) Stats() (hits, misses uint64, hitRate float64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	hits = c.hits
	misses = c.misses
	total := hits + misses
	if total > 0 {
		hitRate = float64(hits) / float64(total)
	}
	return
}
