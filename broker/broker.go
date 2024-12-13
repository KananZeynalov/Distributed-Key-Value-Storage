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

// Pair represents a pair of KVStores
type Pair struct {
	Store1 *kvstore.KVStore
	Store2 *kvstore.KVStore
}

// Broker manages multiple KVStore instances and handles load balancing.
type Broker struct {
	mu     sync.RWMutex
	stores map[string]*kvstore.KVStore
	loads  map[string]int // Simple load metric: number of operations handled
	pairs  []Pair
}

// NewBroker initializes and returns a new Broker instance.
func NewBroker() *Broker {
	return &Broker{
		stores: make(map[string]*kvstore.KVStore),
		loads:  make(map[string]int),
	}
}

// CreateStore creates a new KVStore with the given name and adds it to the broker.
func (b *Broker) CreateStore(name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, exists := b.stores[name]; exists {
		return errors.New("store with this name already exists")
	}
	store := kvstore.NewKVStore(name)
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

func (b *Broker) DeleteKey(key string) {
	for _, store := range b.stores {
		err := store.Delete(key)
		if err == nil {
			b.ResetLoad(store.Name())
			fmt.Println("Delete operation successful.")
			return
		}
	}
	fmt.Println("Key not found in any store")
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
func (b *Broker) PairKVStores() ([]Pair, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if len(b.stores) < 2 {
		return nil, errors.New("not enough stores to create pairs")
	}

	var pairs []Pair
	storeNames := make([]string, 0, len(b.stores))
	for name := range b.stores {
		storeNames = append(storeNames, name)
	}

	// Pairing logic
	for i := 0; i < len(storeNames); i++ {
		for j := i + 1; j < len(storeNames); j++ {
			pairs = append(pairs, Pair{
				Store1: storeNames[i],
				Store2: storeNames[j],
			})
		}
	}

	return pairs, nil
}
