package kvstore

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"sync"
	"time"
)

// KVStore represents the in-memory key-value store.
type KVStore struct {
	mu        sync.RWMutex
	data      map[string]string
	Name      string
	IPAddress string
	PeerIP    string
}

// LoadAndMergeFromDisk loads data from a file and merges it with the existing in-memory key-value store.
func (s *KVStore) LoadAndMergeFromDisk() error {
	// Open the snapshot file
	filename := "peerof" + s.Name + ".snapshot.json"
	file, err := os.Open(filename)
	if err != nil {
		if os.IsNotExist(err) {
			fmt.Println("Snapshot file does not exist. No data to merge.")
			return nil
		}
		return fmt.Errorf("failed to open snapshot file: %w", err)
	}
	defer file.Close()

	// Deserialize the JSON data into a temporary map
	var data map[string]string
	decoder := json.NewDecoder(file)
	err = decoder.Decode(&data)
	if err != nil {
		return fmt.Errorf("failed to decode JSON data: %w", err)
	}

	// Merge the temporary map with the in-memory store
	s.mu.Lock()
	defer s.mu.Unlock()
	for key, value := range data {
		s.data[key] = value
	}

	fmt.Println("Data successfully loaded and merged from disk:", filename)
	return nil
}

// NewKVStore initializes and returns a new KVStore instance.
func NewKVStore(name string, port string) *KVStore {
	return &KVStore{
		data:      make(map[string]string),
		Name:      name,
		IPAddress: fmt.Sprintf("localhost:%s", port), // Set correct address format
		PeerIP:    "",
	}
}

// SetPeerIP sets the peer IP address for the KVStore.
func (s *KVStore) SetPeerIP(PeerIP string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.PeerIP = PeerIP
}

// GetPeerIP returns the peer IP address for the KVStore.
func (s *KVStore) GetPeerIP() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.PeerIP
}

// Set inserts or updates the value for a given key.
func (s *KVStore) Set(key, value string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if key == "" {
		return errors.New("key cannot be empty")
	}
	s.data[key] = value
	return nil
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

// GetAllData returns a copy of the entire data map.
func (s *KVStore) GetAllData() map[string]string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Create a copy of the data map to avoid race conditions
	dataCopy := make(map[string]string)
	for key, value := range s.data {
		dataCopy[key] = value
	}
	return dataCopy
}

// SaveToDisk saves the in-memory data to a file in JSON format.
func (s *KVStore) SaveToDisk() error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Open or create the file for writing
	filename := s.Name + ".snapshot.json"
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

func (s *KVStore) RequestPeerBackup(peerURL string) {
	resp, err := http.Get(peerURL + "/peer-backup")
	if err != nil {
		fmt.Println("Error sending request to peer-backup:", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		fmt.Println("Error response from peer-backup:", resp.Status)
		return
	}

	var data map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		fmt.Println("Error decoding response data:", err)
		return
	}
	peerBackupFileName := "peerof" + s.Name + ".snapshot.json"
	file, err := os.Create(peerBackupFileName)
	if err != nil {
		fmt.Println("Error creating snapshot file:", err)
		return
	}
	defer file.Close()

	if err := json.NewEncoder(file).Encode(data); err != nil {
		fmt.Println("Error encoding data to snapshot file:", err)
		return
	}

	fmt.Println("Data successfully saved to peer.snapshot.json")
}

// StartPeriodicSnapshots starts a goroutine that saves the data to disk periodically.
func (s *KVStore) StartPeriodicSnapshots(interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		filename := s.Name + ".snapshot.json"
		for range ticker.C {
			peer_ip := s.GetPeerIP()
			if peer_ip != "" {
				s.RequestPeerBackup(fmt.Sprintf("http://%s", peer_ip))
			}
			err := s.SaveToDisk()
			if err != nil {
				fmt.Println("Error during periodic snapshot:", err)
			} else {
				fmt.Println("Periodic snapshot saved to disk:", filename)
			}
		}
	}()
}
