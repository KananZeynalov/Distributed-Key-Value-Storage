package broker

import (
	"errors"
	"kv/kvstore"
	"sync"
)

// Broker manages multiple KVStore instances and handles load balancing.
type Broker struct {
	mu     sync.RWMutex
	stores map[string]*kvstore.KVStore
	loads  map[string]int // Simple load metric: number of operations handled
}

// NewBroker initializes and returns a new Broker instance.
func NewBroker() *Broker {
	return &Broker{
		stores: make(map[string]*kvstore.KVStore),
		loads:  make(map[string]int),
	}
}

// AddStore adds a new KVStore to the broker.
func (b *Broker) AddStore(store *kvstore.KVStore) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, exists := b.stores[store.Name()]; exists {
		return errors.New("store with this name already exists")
	}
	b.stores[store.Name()] = store
	b.loads[store.Name()] = 0
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
