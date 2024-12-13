package broker

import (
	"encoding/json"
	"errors"
	"fmt"
	"kv/kvstore"
	"log"
	"os"
	"sync"
	"time"
)

func (b *Broker) StartPeering() error {
	NotifyPeersOfEachOther(b.peerlist)
	return nil
}

// SaveSnapshot saves the current state of the broker to a JSON file.
func (b *Broker) SaveSnapshot() error {
	var filePath = "./data/broker/broker_snapshot.json"

	// Ensure the directory exists
	dir := "./data/broker"
	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
		log.Fatalf("Failed to create directory: %v", err)
	}

	// Check if the file exists
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		// File does not exist, create it
		file, err := os.Create(filePath)
		if err != nil {
			log.Fatalf("Failed to create file: %v", err)
		}
		defer file.Close()
		log.Printf("File created: %s", filePath)
	} else {
		// File exists, open it
		file, err := os.Open(filePath)
		if err != nil {
			log.Fatalf("Failed to open file: %v", err)
		}
		defer file.Close()
		log.Printf("File opened: %s", filePath)
	}

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
	mu       sync.RWMutex
	stores   map[string]*kvstore.KVStore
	loads    map[string]int // Simple load metric: number of operations handled
	peerlist *LinkedList
}

// NewBroker initializes and returns a new Broker instance.
func NewBroker() *Broker {
	return &Broker{
		stores:   make(map[string]*kvstore.KVStore),
		loads:    make(map[string]int),
		peerlist: &LinkedList{},
	}
}

// Node represents a kvstore, this kvstore has the Next's replication
type StoreNode struct {
	Name      string
	IpAddress string
	Next      *StoreNode
	Prev      *StoreNode
}

// LinkedList represents the peer architecture
type LinkedList struct {
	Head *StoreNode
}

// AddNode appends a new node to the circular list
func (ll *LinkedList) AddNode(name, ipAddress string) {
	newNode := &StoreNode{Name: name, IpAddress: ipAddress}

	if ll.Head == nil { // If the list is empty
		ll.Head = newNode
		newNode.Next = newNode // Points to itself
		newNode.Prev = newNode
	} else { // Append to the end
		tail := ll.Head.Prev // Get the tail node
		tail.Next = newNode
		newNode.Prev = tail
		newNode.Next = ll.Head
		ll.Head.Prev = newNode
	}
}

// RemoveNode removes a node by name
func (ll *LinkedList) RemoveNode(name string) error {
	if ll.Head == nil {
		return fmt.Errorf("list is empty")
	}

	current := ll.Head
	for {
		if current.Name == name {
			// Update pointers
			if current.Next == current { // Only one node in the list
				ll.Head = nil
			} else {
				current.Prev.Next = current.Next
				current.Next.Prev = current.Prev
				if current == ll.Head { // Removing the head
					ll.Head = current.Next
				}
			}
			return nil // Node removed
		}

		current = current.Next
		if current == ll.Head {
			break // Completed a full circle
		}
	}
	return fmt.Errorf("node with name %s not found", name)
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

	b.peerlist.AddNode(name, ip_address)
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
	b.peerlist.RemoveNode(name)
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

	if b.peerlist.Head == nil {
		fmt.Println("List is empty")
		return "", errors.New("List is empty")
	}

	current := b.peerlist.Head
	for {
		//TODO
		val, err := b.stores[current.Name].Get(key)
		if err == nil {
			fmt.Println("Value:", val)
			return val, nil
		}
		current = current.Next
		if current == b.peerlist.Head {
			break // Completed a full circle
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

// DisplayForward displays the list from head to tail (circularly)
func (ll *LinkedList) DisplayForward() {
	if ll.Head == nil {
		fmt.Println("List is empty")
		return
	}

	current := ll.Head
	for {
		fmt.Printf("Name: %s, IP: %s\n", current.Name, current.IpAddress)
		current = current.Next
		if current == ll.Head {
			break // Completed a full circle
		}
	}
}

func (b *Broker) GetList() *LinkedList {
	return b.peerlist
}

// EnablePeriodicSnapshots configures periodic snapshots for a given store.
func (b *Broker) EnablePeriodicSnapshots(storename string, intervalSeconds int) error {
	store, err := b.GetStore(storename)
	if err != nil {
		return err
	}
	interval := time.Duration(intervalSeconds) * time.Second
	store.StartPeriodicSnapshots(interval)
	return nil
}
