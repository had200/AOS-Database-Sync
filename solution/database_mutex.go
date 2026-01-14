package solution

import "sync"

// MutexTransaction represents a transaction
type MutexTransaction struct{}

// DatabaseMutex is a coarse-grained mutex database
type DatabaseMutex struct {
	mu      sync.Mutex
	records map[string]*Record
}

// NewDatabaseMutex constructs a new DatabaseMutex
func NewDatabaseMutex() *DatabaseMutex {
	return &DatabaseMutex{
		records: make(map[string]*Record),
	}
}

// BeginTransaction locks the entire database
func (db *DatabaseMutex) BeginTransaction() *MutexTransaction {
	db.mu.Lock()
	return &MutexTransaction{}
}

// Read retrieves a key
func (db *DatabaseMutex) Read(tx *MutexTransaction, key string) (int, bool) {
	record, exists := db.records[key]
	if !exists {
		return 0, false
	}
	return record.Value, true
}

// Write sets the value for a key
func (db *DatabaseMutex) Write(tx *MutexTransaction, key string, value int) {
	record, exists := db.records[key]
	if !exists {
		db.records[key] = &Record{Value: value}
	} else {
		record.Value = value
	}
}

// Update increments a key
func (db *DatabaseMutex) Update(tx *MutexTransaction, key string, delta int) bool {
	record, exists := db.records[key]
	if !exists {
		return false
	}
	record.Value += delta
	return true
}

// Delete removes a key
func (db *DatabaseMutex) Delete(tx *MutexTransaction, key string) bool {
	_, exists := db.records[key]
	if exists {
		delete(db.records, key)
	}
	return exists
}

// Commit unlocks the database
func (db *DatabaseMutex) Commit(tx *MutexTransaction) {
	db.mu.Unlock()
}
