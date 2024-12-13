package kvstore

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"
)

// Forward declare Broker to avoid circular dependency
type Broker interface {
	IncrementLoad(storeName string)
}

// KVStore represents the in-memory key-value store.
type KVStore struct {
	mu     sync.RWMutex
	data   map[string]string
	name   string
	broker Broker
}

// NewKVStore initializes and returns a new KVStore instance.
func NewKVStore(name string, broker Broker) *KVStore {
	return &KVStore{
		data:   make(map[string]string),
		name:   name,
		broker: broker,
	}
}

// Set inserts or updates the value for a given key.
func (s *KVStore) Set(key, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = value
	if s.broker != nil {
		s.broker.IncrementLoad(s.name)
	}
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
	if s.broker != nil {
		s.broker.IncrementLoad(s.name)
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
	if s.broker != nil {
		s.broker.IncrementLoad(s.name)
	}
	return nil
}

// Name returns the name of the KVStore.
func (s *KVStore) Name() string {
	return s.name
}

// PrintData prints the current in-memory data map.
func (s *KVStore) PrintData() {
	s.mu.RLock()
	defer s.mu.RUnlock()
	fmt.Println(s.data)
}

// SaveToDisk saves the in-memory data to a file in JSON format.
func (s *KVStore) SaveToDisk() error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Open or create the file for writing
	filename := "./data/kvstore/" + s.name + ".snapshot.json"
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create snapshot file: %w", err)
	}
	defer file.Close()

	// Serialize the map to JSON
	encoder := json.NewEncoder(file)
	err = encoder.Encode(s.data)
	if err != nil {
		return fmt.Errorf("failed to encode data to JSON: %w", err)
	}

	fmt.Println("Data successfully saved to disk:", filename)
	return nil
}

// LoadFromDisk loads data from a file into the in-memory key-value store.
func (s *KVStore) LoadFromDisk(filename string) error {
	// Open the snapshot file
	file, err := os.Open(filename)
	if err != nil {
		if os.IsNotExist(err) {
			fmt.Println("Snapshot file does not exist. Starting with an empty store.")
			return nil
		}
		return fmt.Errorf("failed to open snapshot file: %w", err)
	}
	defer file.Close()

	// Deserialize the JSON data into the map
	var data map[string]string
	decoder := json.NewDecoder(file)
	err = decoder.Decode(&data)
	if err != nil {
		return fmt.Errorf("failed to decode JSON data: %w", err)
	}

	// Update the in-memory store
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data = data

	fmt.Println("Data successfully loaded from disk:", filename)
	return nil
}

// StartPeriodicSnapshots starts a goroutine that saves the data to disk periodically.
func (s *KVStore) StartPeriodicSnapshots(interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		filename := s.name + ".snapshot.json"
		for range ticker.C {
			err := s.SaveToDisk()
			if err != nil {
				fmt.Println("Error during periodic snapshot:", err)
			} else {
				fmt.Println("Periodic snapshot saved to disk:", filename)
			}
		}
	}()
}
