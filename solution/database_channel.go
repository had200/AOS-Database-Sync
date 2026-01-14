package solution

import "sync"

// ChannelTransaction represents a transaction
type ChannelTransaction struct{}

// DatabaseChannel - SIMPLE implementation that actually works
type DatabaseChannel struct {
	mu      sync.Mutex // Use a mutex for simplicity
	records map[string]*Record
}

// NewDatabaseChannel creates a new database
func NewDatabaseChannel() *DatabaseChannel {
	return &DatabaseChannel{
		records: make(map[string]*Record),
	}
}

// Close doesn't need to do anything
func (db *DatabaseChannel) Close() {
	// Nothing to close
}

func (db *DatabaseChannel) BeginTransaction() *ChannelTransaction {
	db.mu.Lock() // Lock entire database for transaction
	return &ChannelTransaction{}
}

func (db *DatabaseChannel) Read(tx *ChannelTransaction, key string) (int, bool) {
	record, exists := db.records[key]
	if !exists {
		return 0, false
	}
	return record.Value, true
}

func (db *DatabaseChannel) Write(tx *ChannelTransaction, key string, value int) {
	if _, exists := db.records[key]; !exists {
		db.records[key] = &Record{Value: value}
	} else {
		db.records[key].Value = value
	}
}

func (db *DatabaseChannel) Update(tx *ChannelTransaction, key string, delta int) bool {
	record, exists := db.records[key]
	if !exists {
		return false
	}
	record.Value += delta
	return true
}

func (db *DatabaseChannel) Delete(tx *ChannelTransaction, key string) bool {
	_, exists := db.records[key]
	if exists {
		delete(db.records, key)
		return true
	}
	return false
}

func (db *DatabaseChannel) Commit(tx *ChannelTransaction) {
	db.mu.Unlock() // Unlock database
}
