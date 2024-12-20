package broker

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"kv/kvstore"
	"log"
	"net/http"
	"sync"
)

func (b *Broker) StartPeering() error {
	NotifyPeersOfEachOther(b.peerlist)
	return nil
}

func (b *Broker) GetStorePeerIP(storeName string) (string, string, error) {

	store, exists := b.stores[storeName]
	if !exists {
		return "", "", errors.New("store not found")
	}

	current := b.peerlist.Head
	if current == nil {
		return "", "", errors.New("peer list is empty")
	}

	for {
		if current.Name == store.Name {
			return current.Prev.IpAddress, current.Prev.Name, nil
		}
		current = current.Next
		if current == b.peerlist.Head {
			break // full circle
		}
	}

	return "", "", errors.New("peer not found")
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

func (b *Broker) CreateStore(name string, ip_address string) error {
	fmt.Printf("Attempting to create store:\nName: %s\nIP Address: %s\n", name, ip_address)

	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.stores[name]; exists {
		fmt.Printf("Store with name '%s' already exists. Skipping creation.\n", name)
		return errors.New("store with this name already exists")
	}

	if ip_address == "" {
		fmt.Printf("Error: Empty IP address for store '%s'.\n", name)
		return errors.New("invalid IP address")
	}

	// Add to stores and peerlist
	fmt.Printf("Registering new store:\nName: %s\nIP Address: %s\n", name, ip_address)
	store := &kvstore.KVStore{
		Name:      name,
		IPAddress: ip_address,
	}
	b.stores[name] = store
	b.loads[name] = 0

	fmt.Printf("Adding to peer list: Name: %s, IP Address: %s\n", name, ip_address)
	b.peerlist.AddNode(name, ip_address)

	// Debug: Print current list of stores
	fmt.Println("Current list of stores:")
	for storeName, store := range b.stores {
		fmt.Printf("  Store Name: %s, IP Address: %s\n", storeName, store.IPAddress)
	}

	// Notify existing stores about the new store
	fmt.Printf("Notifying peers about the new store: %s\n", name)
	b.StartPeering()

	return nil
}

func (b *Broker) RemoveStore(name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	store, exists := b.stores[name]
	if !exists {
		return errors.New("store not found")
	}

	delete(b.stores, name)
	delete(b.loads, name)
	b.peerlist.RemoveNode(name)

	// Notify remaining stores about the removal
	b.StartPeering()

	// Optionally, send a delete request to the KVStore to gracefully shut it down
	url := fmt.Sprintf("http://%s/shutdown", store.IPAddress)
	req, err := http.NewRequest("POST", url, nil)
	if err != nil {
		log.Printf("Error creating shutdown request for store %s: %v", name, err)
		return nil // Continue even if shutdown request fails
	}
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Printf("Error sending shutdown request to store %s: %v", name, err)
		return nil
	}
	resp.Body.Close()

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
		url := fmt.Sprintf("http://%s/save", store.IPAddress)
		resp, err := http.Post(url, "application/json", nil)
		if err != nil {
			log.Printf("Failed to send manual snapshot request to store %s: %v", name, err)
			continue
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			log.Printf("Store %s responded with status: %d", name, resp.StatusCode)
		} else {
			log.Printf("Manual snapshot triggered for store %s successfully.", name)
		}
	}
	return nil
}

func (b *Broker) GetKey(key string) (string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	// Iterate over all KVStores to find the key
	for _, store := range b.stores {
		url := fmt.Sprintf("http://%s/get?key=%s", store.IPAddress, key)
		resp, err := http.Get(url)
		if err != nil {
			fmt.Printf("Error contacting KVStore at %s: %v\n", store.IPAddress, err)
			//Ediz, I could not find the ip of its peer. Le it be ip_peer;
			ip_peer, name_peer, err := b.GetStorePeerIP(store.Name)
			if err != nil {
				fmt.Printf("Error getting peer ip of %s: %v\n", store.Name, err)
			}
			fmt.Printf("Now %s will continue where he left\n", name_peer)
			url := fmt.Sprintf("http://%s/peer-dead", ip_peer)
			http.Post(url, "application/json", nil)
			delete(b.stores, store.Name)
			delete(b.loads, store.Name)
			b.peerlist.RemoveNode(store.Name)
			b.StartPeering()
			continue
		}
		defer resp.Body.Close()

		if resp.StatusCode == http.StatusOK {
			var result map[string]string
			if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
				fmt.Printf("Error decoding response from KVStore at %s: %v\n", store.IPAddress, err)
				continue
			}

			// Found the key, return the value
			if value, ok := result["value"]; ok {
				fmt.Printf("Key '%s' found in KVStore: %s\n", key, store.IPAddress)
				return value, nil
			}
		}
	}

	return "", fmt.Errorf("key '%s' not found in any KVStore", key)
}

func (b *Broker) SetKey(key string, value string) error {
	store, err := b.GetLeastLoadedStore()
	if err != nil {
		return fmt.Errorf("no available KVStore: %w", err)
	}

	url := fmt.Sprintf("http://%s/set", store.IPAddress)
	data := map[string]string{
		"key":   key,
		"value": value,
	}
	jsonData, err := json.Marshal(data)
	if err != nil {
		return err
	}

	resp, err := http.Post(url, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("error contacting KVStore at %s: %w", store.IPAddress, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("KVStore returned status: %d", resp.StatusCode)
	}

	b.IncrementLoad(store.Name)
	fmt.Printf("Key '%s' set in KVStore: %s\n", key, store.IPAddress)
	return nil
}

// DeleteKey deletes a key from the specific KVStore where it is located.
func (b *Broker) DeleteKey(key string) (bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	var storeIP string
	exists := false
	// Iterate over all KVStores to find the key
	for _, store := range b.stores {
		url := fmt.Sprintf("http://%s/get?key=%s", store.IPAddress, key)
		resp, err := http.Get(url)
		if err != nil {
			fmt.Printf("Error contacting KVStore at %s: %v\n", store.IPAddress, err)
			continue
		}
		defer resp.Body.Close()

		if resp.StatusCode == http.StatusOK {
			storeIP = store.IPAddress
			exists = true
		}
	}

	if !exists {
		log.Printf("Key '%s' not found in keyLocation map.\n", key)
		return false, fmt.Errorf("key '%s' not found in keyLocation map", key)
	}

	url := fmt.Sprintf("http://%s/delete", storeIP)
	data := map[string]string{
		"key": key,
	}
	jsonData, err := json.Marshal(data)
	if err != nil {
		log.Printf("Error marshalling delete request: %v\n", err)
		return false, fmt.Errorf("error marshalling delete request: %v", err)
	}

	resp, err := http.Post(url, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		log.Printf("Error contacting KVStore at %s: %v\n", storeIP, err)
		return false, fmt.Errorf("error contacting KVStore at %s: %v", storeIP, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		// Successfully deleted the key, remove it from the keyLocation map
		log.Printf("key '%s' successfully deleted from KVStore at %s", key, storeIP)
		return true, nil
	}

	// Log response if deletion failed
	log.Printf("Failed to delete key '%s' from KVStore at %s, status code: %d\n", key, storeIP, resp.StatusCode)
	return false, fmt.Errorf("failed to delete key '%s' from KVStore at %s, status code: %d", key, storeIP, resp.StatusCode)
}

func (b *Broker) LoadStoreFromSnapshot(storename string, filename string) {
	store, err := b.GetStore(storename)
	if err != nil {
		fmt.Println("Error retrieving store:", err)
		return
	}

	url := fmt.Sprintf("http://%s/load", store.IPAddress)
	data := map[string]string{
		"filename": filename,
	}
	jsonData, err := json.Marshal(data)
	if err != nil {
		fmt.Printf("Error marshalling load snapshot request for store %s: %v\n", storename, err)
		return
	}

	resp, err := http.Post(url, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		fmt.Printf("Error sending load snapshot request to store %s: %v\n", storename, err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		fmt.Printf("Store %s responded with status: %d\n", storename, resp.StatusCode)
	} else {
		fmt.Println("Data loaded successfully from", filename)
	}
}

func (b *Broker) GetAllData() []string {
	b.mu.RLock()
	defer b.mu.RUnlock()

	var allData []string
	for name, store := range b.stores {
		url := fmt.Sprintf("http://%s/getall", store.IPAddress)
		resp, err := http.Get(url)
		if err != nil {
			log.Printf("Error contacting KVStore at %s: %v", store.IPAddress, err)
			continue
		}

		if resp.StatusCode != http.StatusOK {
			log.Printf("KVStore %s responded with status: %d", name, resp.StatusCode)
			resp.Body.Close()
			continue
		}

		var data map[string]string
		if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
			log.Printf("Error decoding getall response from store %s: %v", name, err)
			resp.Body.Close()
			continue
		}
		resp.Body.Close()

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
		url := fmt.Sprintf("http://%s/getall", store.IPAddress)
		resp, err := http.Get(url)
		if err != nil {
			fmt.Printf("Error contacting KVStore at %s: %v\n", store.IPAddress, err)
			continue
		}

		if resp.StatusCode != http.StatusOK {
			fmt.Printf("KVStore %s responded with status: %d\n", name, resp.StatusCode)
			resp.Body.Close()
			continue
		}

		var data map[string]string
		if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
			fmt.Printf("Error decoding getall response from store %s: %v\n", name, err)
			resp.Body.Close()
			continue
		}
		resp.Body.Close()

		for k, v := range data {
			fmt.Printf("  Key: %s, Value: %s\n", k, v)
		}
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

	url := fmt.Sprintf("http://%s/start-snapshots?interval=%d", store.IPAddress, intervalSeconds)
	resp, err := http.Post(url, "application/json", nil)
	if err != nil {
		return fmt.Errorf("error sending start snapshots request to store %s: %w", storename, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("store %s responded with status: %d", storename, resp.StatusCode)
	}

	return nil
}
