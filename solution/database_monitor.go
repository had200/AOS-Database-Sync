package solution

import (
	"sort"
	"sync"
)

// MonitorTransaction represents a transaction
type MonitorTransaction struct {
	db         *DatabaseMonitor
	lockedKeys map[string]bool
	operations []func() // Operations to execute on commit
}

// DatabaseMonitor with proper 2-phase locking for atomic multi-key operations
type DatabaseMonitor struct {
	mu      sync.Mutex
	cond    *sync.Cond
	locked  map[string]bool
	records map[string]*Record
}

func NewDatabaseMonitor() *DatabaseMonitor {
	db := &DatabaseMonitor{
		locked:  make(map[string]bool),
		records: make(map[string]*Record),
	}
	db.cond = sync.NewCond(&db.mu)
	return db
}

func (db *DatabaseMonitor) BeginTransaction() *MonitorTransaction {
	return &MonitorTransaction{
		db:         db,
		lockedKeys: make(map[string]bool),
		operations: make([]func(), 0),
	}
}

// acquireKeysForAtomicOperation locks all keys for atomic multi-key operation
func (tx *MonitorTransaction) acquireKeysForAtomicOperation(keys ...string) {
	if len(keys) == 0 {
		return
	}

	// Sort keys to prevent deadlock
	keysCopy := make([]string, len(keys))
	copy(keysCopy, keys)
	sort.Strings(keysCopy)

	tx.db.mu.Lock()
	defer tx.db.mu.Unlock()

	// Try to acquire all locks
	for {
		allAvailable := true
		for _, key := range keysCopy {
			// If key is locked by another transaction, wait
			if tx.db.locked[key] && !tx.lockedKeys[key] {
				allAvailable = false
				break
			}
		}

		if allAvailable {
			// Lock all keys at once
			for _, key := range keysCopy {
				if !tx.lockedKeys[key] {
					tx.db.locked[key] = true
					tx.lockedKeys[key] = true
				}
			}
			return
		}

		// Wait for keys to become available
		tx.db.cond.Wait()
	}
}

// Read operation with immediate release
func (db *DatabaseMonitor) Read(tx *MonitorTransaction, key string) (int, bool) {
	tx.acquireKeysForAtomicOperation(key)

	rec, ok := db.records[key]
	if !ok {
		return 0, false
	}

	// Store read result to use in operations
	value := rec.Value
	return value, true
}

// Write operation - defer actual write to commit
func (db *DatabaseMonitor) Write(tx *MonitorTransaction, key string, value int) {
	// Acquire lock for this key
	tx.acquireKeysForAtomicOperation(key)

	// Add write operation to transaction
	tx.operations = append(tx.operations, func() {
		if _, ok := db.records[key]; !ok {
			db.records[key] = &Record{Value: value}
		} else {
			db.records[key].Value = value
		}
	})
}

// Update operation - defer actual update to commit
func (db *DatabaseMonitor) Update(tx *MonitorTransaction, key string, delta int) bool {
	// Acquire lock for this key
	tx.acquireKeysForAtomicOperation(key)

	// Check if key exists
	rec, ok := db.records[key]
	if !ok {
		return false
	}

	// Store current value for the operation
	currentValue := rec.Value

	// Add update operation to transaction
	tx.operations = append(tx.operations, func() {
		db.records[key].Value = currentValue + delta
	})
	return true
}

// Delete operation - defer actual delete to commit
func (db *DatabaseMonitor) Delete(tx *MonitorTransaction, key string) bool {
	// Acquire lock for this key
	tx.acquireKeysForAtomicOperation(key)

	// Check if key exists
	_, ok := db.records[key]
	if !ok {
		return false
	}

	// Add delete operation to transaction
	tx.operations = append(tx.operations, func() {
		delete(db.records, key)
	})
	return true
}

// Commit executes all operations atomically and releases locks
func (db *DatabaseMonitor) Commit(tx *MonitorTransaction) {
	// Execute all operations (this happens while keys are still locked)
	for _, op := range tx.operations {
		op()
	}

	// Release all locks
	db.mu.Lock()
	defer db.mu.Unlock()

	for key := range tx.lockedKeys {
		db.locked[key] = false
	}
	tx.lockedKeys = make(map[string]bool)
	db.cond.Broadcast()
}
