package kvstore

import (
	"errors"
	"sync"
	"fmt"
)

// KVStore represents the in-memory key-value store.
type KVStore struct {
	mu   sync.RWMutex
	data map[string]string
}

// NewKVStore initializes and returns a new KVStore instance.
func NewKVStore() *KVStore {
	return &KVStore{
		data: make(map[string]string),
	}
}

// Set inserts or updates the value for a given key.
func (s *KVStore) Set(key, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = value
}

// Get retrieves the value associated with the given key.
// Returns an error if the key does not exist.
func (s *KVStore) Get(key string) (string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	val, ok := s.data[key]
	if !ok {
		return "", errors.New("key not found")
	}
	return val, nil
}

// Delete removes the key-value pair associated with the given key.
// Returns an error if the key does not exist.
func (s *KVStore) Delete(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.data[key]
	if !ok {
		return errors.New("key not found")
	}
	delete(s.data, key)
	return nil
}

// PrintData prints the current in-memory data map.
func (s *KVStore) PrintData() {
	s.mu.RLock()
	defer s.mu.RUnlock()
	fmt.Println(s.data)
}
