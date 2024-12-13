package broker

import (
	"encoding/json"
	"errors"
	"fmt"
	"kv/kvstore"
	"os"
	"sync"
)

// SaveSnapshot saves the current state of the broker to a JSON file.
func (b *Broker) SaveSnapshot() error {
	var filePath = "./data/broker/broker_snapshot.json"

	b.mu.RLock()
	defer b.mu.RUnlock()

	data := struct {
		Stores map[string]int `json:"stores"`
	}{
		Stores: b.loads,
	}

	file, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(filePath, file, 0644)
}

// LoadSnapshot loads the broker state from a JSON file.
func (b *Broker) LoadSnapshot() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	var filePath = "./data/broker/broker_snapshot.json"

	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	data := struct {
		Stores map[string]int `json:"stores"`
	}{}

	if err := json.NewDecoder(file).Decode(&data); err != nil {
		return err
	}

	for name, load := range data.Stores {
		if store, exists := b.stores[name]; exists {
			b.loads[store.Name()] = load
		}
	}

	return nil
}

// Broker manages multiple KVStore instances and handles load balancing.
type Broker struct {
	mu     sync.RWMutex
	stores map[string]*kvstore.KVStore
	loads  map[string]int // Simple load metric: number of operations handled
	peers  map[string]*kvstore.KVStore
}

// NewBroker initializes and returns a new Broker instance.
func NewBroker() *Broker {
	return &Broker{
		stores: make(map[string]*kvstore.KVStore),
		loads:  make(map[string]int),
		peers:  make(map[string]*kvstore.KVStore),
	}
}

// CreateStore creates a new KVStore with the given name and adds it to the broker.
func (b *Broker) CreateStore(name string, ip_address string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, exists := b.stores[name]; exists {
		return errors.New("store with this name already exists")
	}
	store := kvstore.NewKVStore(name, ip_address)
	b.stores[name] = store
	b.loads[name] = 0
	return nil
}

// RemoveStore removes a KVStore from the broker.
func (b *Broker) RemoveStore(name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, exists := b.stores[name]; !exists {
		return errors.New("store not found")
	}
	delete(b.stores, name)
	delete(b.loads, name)
	return nil
}

// GetLeastLoadedStore returns the name of the store with the least load.
func (b *Broker) GetLeastLoadedStore() (*kvstore.KVStore, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if len(b.stores) == 0 {
		return nil, errors.New("no stores available")
	}
	var leastLoadedStore *kvstore.KVStore
	minLoad := int(^uint(0) >> 1) // Initialize with maximum int
	for name, store := range b.stores {
		if b.loads[name] < minLoad {
			minLoad = b.loads[name]
			leastLoadedStore = store
		}
	}
	return leastLoadedStore, nil
}

// IncrementLoad increments the load metric for a given store.
func (b *Broker) IncrementLoad(storeName string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, exists := b.loads[storeName]; exists {
		b.loads[storeName]++
	}
}

// ResetLoad resets the load metric for a given store.
func (b *Broker) ResetLoad(storeName string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, exists := b.loads[storeName]; exists {
		b.loads[storeName] = 0
	}
}

// ListStores returns a list of all store names managed by the broker.
func (b *Broker) ListStores() []string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	names := make([]string, 0, len(b.stores))
	for name := range b.stores {
		names = append(names, name)
	}
	return names
}

// GetStore retrieves a store by name.
func (b *Broker) GetStore(name string) (*kvstore.KVStore, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	store, exists := b.stores[name]
	if !exists {
		return nil, errors.New("store not found")
	}
	return store, nil
}

// StoreExists checks if a store with the given name exists.
func (b *Broker) StoreExists(name string) bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	_, exists := b.stores[name]
	return exists
}

// StoreExists checks if a store with the given name exists.
func (b *Broker) ManualSnapshotStore() error {
	b.mu.RLock()
	defer b.mu.RUnlock()
	for name, store := range b.stores {
		err := store.SaveToDisk()
		if err != nil {
			return fmt.Errorf("failed to save snapshot for store %s: %v", name, err)
		}
	}
	return nil
}

func (b *Broker) GetKey(key string) (string, error) {
	for _, store := range b.stores {
		val, err := store.Get(key)
		if err == nil {
			fmt.Println("Value:", val)
			return val, nil
		}
	}
	fmt.Println("Key not found in any store")
	return "", errors.New("key not found in any store")
}

func (b *Broker) SetKey(key string, value string) error {
	store, err := b.GetLeastLoadedStore()
	if err != nil {
		fmt.Println("Error retrieving store:", err)
		return fmt.Errorf("failed to retrieve the least loaded store: %w", err)
	}
	store.Set(key, value)

	if err != nil {
		fmt.Println("Error setting key:", err)
		return fmt.Errorf("failed to retrieve the least loaded store: %w", err)
	}
	b.IncrementLoad(store.Name())
	fmt.Println("Set operation successful.")
	return nil
}

func (b *Broker) DeleteKey(key string) bool {
	for _, store := range b.stores {
		err := store.Delete(key)
		if err == nil {
			b.ResetLoad(store.Name())
			fmt.Println("Delete operation successful.")
			return true
		}
	}
	fmt.Println("Key not found in any store")
	return false
}

func (b *Broker) LoadStoreFromSnapshot(storename string, filename string) {
	store, err := b.GetStore(storename)
	if err != nil {
		fmt.Println("Error retrieving store:", err)
		return
	}
	err = store.LoadFromDisk(filename)
	if err != nil {
		fmt.Println("Error loading data from disk:", err)
	} else {
		fmt.Println("Data loaded successfully from", filename)
	}
}

func (b *Broker) GetAllData() []string {
	b.mu.RLock()
	defer b.mu.RUnlock()

	var allData []string
	for name, store := range b.stores {
		data := store.GetAllData()
		for k, v := range data {
			allData = append(allData, fmt.Sprintf("Store: %s, Key: %s, Value: %s", name, k, v))
		}
	}
	return allData
}

func (b *Broker) ListAllData() error {
	b.mu.RLock()
	defer b.mu.RUnlock()
	for name, store := range b.stores {
		fmt.Printf("Store: %s\n", name)
		store.PrintData()
	}
	return nil
}

// PairKVStores pairs the existing KVStores and stores them in a slice of pairs
func (b *Broker) PairKVStores() error {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if len(b.stores) < 2 {
		return errors.New("not enough stores to create pairs")
	}

	stores := make([]*kvstore.KVStore, 0, len(b.stores))
	for _, store := range b.stores {
		stores = append(stores, store)
	}

	// Pairing logic
	for i := 0; i < len(stores); i++ {
		b.peers[stores[i].Name()] = stores[(i+1)%len(stores)]
	}

	return nil
}

func (b *Broker) DisplayPeers() {
	for name, store := range b.peers {
		fmt.Println("Snapshot of " + name + " is in " + store.Name())
	}
}
