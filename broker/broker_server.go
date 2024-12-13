package broker

import (
	"encoding/json"
	"errors"
	"fmt"
	"kv/kvstore" 
	"kv/broker"  
	"net/http"
	"sync"
)

// Wraps a broker to expose it via HTTP.
type BrokerHandler struct {
	broker *broker.Broker
	mu     sync.RWMutex
}

// Creates a new BrokerHandler instance.
func NewBrokerHandler(b *broker.Broker) *BrokerHandler {
	return &BrokerHandler{broker: b}
}

//TODO: GetHandler 

// Assign the given key-value pair to the least loaded store
func (h *BrokerHandler) SetHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST is allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		key string json:"key"
		value string json:"value"
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	h.mu.RLock()
	defer h.mu.RUnlock()

	// Get the least loaded store
	store, err := h.broker.GetLeastLoadedStore()
	if err != nil {
		http.Error(w, "No available stores: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Perform the Set operation
	if err := store.Set(req.Key, req.Value); err != nil {
		http.Error(w, "Failed to set key-value pair: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Respond with success
	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/json")
	response := map[string]string{
		"message": "Set operation successful",
		"store":   store.Name(),
	}
	json.NewEncoder(w).Encode(response)
}


// ListStoresHandler lists all the stores in the broker.
func (h *BrokerHandler) ListStoresHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Only GET is allowed", http.StatusMethodNotAllowed)
		return
	}

	h.mu.RLock()
	defer h.mu.RUnlock()

	stores := h.broker.ListStores()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(stores)
}

// GetStoreHandler retrieves a specific store's load.
func (h *BrokerHandler) GetStoreHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Only GET is allowed", http.StatusMethodNotAllowed)
		return
	}

	storeName := r.URL.Query().Get("name")
	if storeName == "" {
		http.Error(w, "Missing 'name' query parameter", http.StatusBadRequest)
		return
	}

	h.mu.RLock()
	defer h.mu.RUnlock()

	store, err := h.broker.GetStore(storeName)
	if err != nil {
		http.Error(w, fmt.Sprintf("Store not found: %v", err), http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]int{"load": h.broker.Loads[store.Name()]})
}

// SetupRoutes sets up HTTP routes for the broker.
func (h *BrokerHandler) SetupRoutes() {
	http.HandleFunc("/set", h.SetHandler)
	http.HandleFunc("/stores/list", h.ListStoresHandler)
	http.HandleFunc("/stores/get", h.GetStoreHandler)
}

type KVStoreConfig struct {
	Name      string `json:"name"`
	IPAddress string `json:"ip_address"`
}

func LoadKVStoresConfig(filePath string) ([]KVStoreConfig, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var config struct {
		KVStores []KVStoreConfig `json:"kvstores"`
	}

	if err := json.NewDecoder(file).Decode(&config); err != nil {
		return nil, err
	}

	return config.KVStores, nil
}

func (h *BrokerHandler) ConfigurePeers() (){

}


func main() {
	// Initialize the broker
	b := broker.NewBroker()

	// Load KVStore configurations from the JSON file
	configs, err := LoadKVStoresConfig("kvstores_config.json")
	if err != nil {
		panic("Failed to load KVStore configurations: " + err.Error())
	}

	// Initialize and add stores to the broker
	for _, config := range configs {
		store := kvstore.NewKVStore(config.Name, config.IPAddress)
		if err := b.AddStore(store); err != nil {
			panic("Failed to add store: " + err.Error())
		}
	}

	// Create a new BrokerHandler
	handler := NewBrokerHandler(b)

	// Setup HTTP routes
	handler.SetupRoutes()

	// Start the HTTP server
	fmt.Println("Starting broker web server on :8080")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		fmt.Println("Error starting server:", err)
	}
}
