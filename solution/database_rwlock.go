// File: database_rwlock.go
package solution

import (
	"sort"
	"sync"
)

// Record represents a single key-value entry

// MonitorTransactionRWLock represents a transaction with deferred operations
type MonitorTransactionRWLock struct {
	db         *DatabaseRWLock
	lockedKeys map[string]bool
	operations []func()
}

// DatabaseRWLock supports multi-key atomic operations using RWMutex
type DatabaseRWLock struct {
	mu      sync.RWMutex
	records map[string]*Record
	locked  map[string]bool // per-key locks
	cond    *sync.Cond
}

// NewDatabaseRWLock creates a new instance
func NewDatabaseRWLock() *DatabaseRWLock {
	db := &DatabaseRWLock{
		records: make(map[string]*Record),
		locked:  make(map[string]bool),
	}
	db.cond = sync.NewCond(&db.mu)
	return db
}

// BeginTransaction starts a new transaction
func (db *DatabaseRWLock) BeginTransaction() *MonitorTransactionRWLock {
	return &MonitorTransactionRWLock{
		db:         db,
		lockedKeys: make(map[string]bool),
		operations: []func(){},
	}
}

// acquireKeys locks multiple keys atomically to avoid deadlocks
func (tx *MonitorTransactionRWLock) acquireKeys(keys ...string) {
	if len(keys) == 0 {
		return
	}
	keysCopy := append([]string{}, keys...)
	sort.Strings(keysCopy)

	tx.db.mu.Lock()
	defer tx.db.mu.Unlock()

	for {
		allFree := true
		for _, key := range keysCopy {
			if tx.db.locked[key] && !tx.lockedKeys[key] {
				allFree = false
				break
			}
		}
		if allFree {
			for _, key := range keysCopy {
				if !tx.lockedKeys[key] {
					tx.db.locked[key] = true
					tx.lockedKeys[key] = true
				}
			}
			return
		}
		tx.db.cond.Wait()
	}
}

// Read gets a value safely
func (db *DatabaseRWLock) Read(tx *MonitorTransactionRWLock, key string) (int, bool) {
	tx.acquireKeys(key)
	rec, ok := db.records[key]
	if !ok {
		return 0, false
	}
	return rec.Value, true
}

// Write defers a write operation to commit
func (db *DatabaseRWLock) Write(tx *MonitorTransactionRWLock, key string, value int) {
	tx.acquireKeys(key)
	tx.operations = append(tx.operations, func() {
		if _, ok := db.records[key]; !ok {
			db.records[key] = &Record{Value: value}
		} else {
			db.records[key].Value = value
		}
	})
}

// Update defers an update operation
func (db *DatabaseRWLock) Update(tx *MonitorTransactionRWLock, key string, delta int) bool {
	tx.acquireKeys(key)
	rec, ok := db.records[key]
	if !ok {
		return false
	}
	oldValue := rec.Value
	tx.operations = append(tx.operations, func() {
		db.records[key].Value = oldValue + delta
	})
	return true
}

// Delete defers a delete operation
func (db *DatabaseRWLock) Delete(tx *MonitorTransactionRWLock, key string) bool {
	tx.acquireKeys(key)
	_, ok := db.records[key]
	if !ok {
		return false
	}
	tx.operations = append(tx.operations, func() {
		delete(db.records, key)
	})
	return true
}

// Commit executes all operations and releases locks
func (db *DatabaseRWLock) Commit(tx *MonitorTransactionRWLock) {
	// Execute deferred operations while holding keys
	for _, op := range tx.operations {
		op()
	}

	// Release locks
	db.mu.Lock()
	for key := range tx.lockedKeys {
		db.locked[key] = false
	}
	tx.lockedKeys = make(map[string]bool)
	db.mu.Unlock()
	db.cond.Broadcast()
}
