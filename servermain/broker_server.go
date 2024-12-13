package main

import (
	"encoding/json"
	"fmt"
	"kv/broker"
	"net/http"
	"os"
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

// SetupRoutes sets up HTTP routes for the broker.
func (h *BrokerHandler) SetupRoutes() {
	http.HandleFunc("/set", h.SetHandler)
	http.HandleFunc("/get", h.GetHandler)
	http.HandleFunc("/getall", h.GetAllHandler)
	http.HandleFunc("/stores/list", h.ListStoresHandler)
	// http.HandleFunc("/stores/get", h.GetStoreHandler)
	http.HandleFunc("/delete", h.DeleteHandler)
	http.HandleFunc("/snapshot/enable", h.EnableSnapshotHandler)
	http.HandleFunc("/store/new", h.NewKVHandler)
	http.HandleFunc("/snapshot/manual", h.ManualSnapshotHandler)
	http.HandleFunc("/snapshot/broker", h.SnapshotBrokerHandler)

}

// Get the value of the given key
func (h *BrokerHandler) GetHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Only GET is allowed", http.StatusMethodNotAllowed)
		return
	}

	key := r.URL.Query().Get("key")

	h.mu.RLock()
	defer h.mu.RUnlock()
	// Perform the Get operation
	val, err := h.broker.GetKey(key)
	if err != nil {
		http.Error(w, "Failed to get the value: "+key+err.Error(), http.StatusInternalServerError)
		return
	}

	// Respond with success
	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/json")
	response := map[string]string{
		"message": "Get operation successful",
		"value":   val,
	}
	json.NewEncoder(w).Encode(response)
}

func (h *BrokerHandler) GetAllHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Only GET is allowed", http.StatusMethodNotAllowed)
		return
	}

	h.mu.RLock()
	defer h.mu.RUnlock()
	// Perform the Get operation
	data := h.broker.GetAllData()

	// Respond with success
	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/json")
	// response := map[string]string{data}
	json.NewEncoder(w).Encode(data)

}

// Assign the given key-value pair to the least loaded store
func (h *BrokerHandler) SetHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST is allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	h.mu.RLock()
	defer h.mu.RUnlock()

	if err := h.broker.SetKey(req.Key, req.Value); err != nil {
		http.Error(w, "Failed to set key-value pair: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Respond with success
	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/json")
	response := map[string]string{
		"message": "Set operation successful",
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


// DeleteHandler: POST /delete { "key": "..." }
func (h *BrokerHandler) DeleteHandler(w http.ResponseWriter, r *http.Request) {
    if r.Method != http.MethodPost {
        http.Error(w, "Only POST is allowed", http.StatusMethodNotAllowed)
        return
    }

    var req struct {
        Key string `json:"key"`
    }

    if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
        http.Error(w, "Invalid request body", http.StatusBadRequest)
        return
    }

    h.mu.Lock()
    deleted := h.broker.DeleteKey(req.Key)
    h.mu.Unlock()

    if deleted {
        // Key was successfully deleted
        response := map[string]string{
            "message": "Delete operation successful",
        }
        jsonResponse(w, response)
    } else {
        // Key was not found in any store
        http.Error(w, "Key not found in any store", http.StatusNotFound)
    }
}

// EnableSnapshotHandler: POST /snapshot/enable { "storename": "...", "interval": <seconds> }
func (h *BrokerHandler) EnableSnapshotHandler(w http.ResponseWriter, r *http.Request) {
    if r.Method != http.MethodPost {
        http.Error(w, "Only POST is allowed", http.StatusMethodNotAllowed)
        return
    }

    var req struct {
        Storename string `json:"storename"`
        Interval  int    `json:"interval"`
    }

    if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
        http.Error(w, "Invalid request body", http.StatusBadRequest)
        return
    }
    h.mu.Lock()
    err := h.broker.EnablePeriodicSnapshots(req.Storename, req.Interval)
    h.mu.Unlock()

    if err != nil {
        http.Error(w, "Failed to enable periodic snapshots: "+err.Error(), http.StatusNotFound)
        return
    }

    response := map[string]string{
        "message": fmt.Sprintf("Periodic snapshots enabled for store %s with interval %d seconds.", req.Storename, req.Interval),
    }
    jsonResponse(w, response)
}


// NewKVHandler: POST /store/new { "name": "...", "ip_address": "..." }
func (h *BrokerHandler) NewKVHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST is allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Name      string `json:"name"`
		IPAddress string `json:"ip_address"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	h.mu.Lock()
	err := h.broker.CreateStore(req.Name, req.IPAddress)
	h.mu.Unlock()

	if err != nil {
		http.Error(w, "Failed to create new store: "+err.Error(), http.StatusBadRequest)
		return
	}

	response := map[string]string{
		"message": "New store created: " + req.Name,
	}
	jsonResponse(w, response)
}

// ManualSnapshotHandler: POST /snapshot/manual
func (h *BrokerHandler) ManualSnapshotHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST is allowed", http.StatusMethodNotAllowed)
		return
	}

	h.mu.Lock()
	err := h.broker.ManualSnapshotStore()
	h.mu.Unlock()

	if err != nil {
		http.Error(w, "Failed to perform manual snapshot: "+err.Error(), http.StatusInternalServerError)
		return
	}

	response := map[string]string{
		"message": "Manual snapshot successful",
	}
	jsonResponse(w, response)
}








func (h *BrokerHandler) ConfigurePeers() {

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
		if err := b.CreateStore(config.Name, config.IPAddress); err != nil {
			panic("Failed to add store: " + err.Error())
		}
	}

	// Pair the stores
	b.PairKVStores()
	b.DisplayPeers()

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

// SnapshotBrokerHandler: POST /snapshot/broker
func (h *BrokerHandler) SnapshotBrokerHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST is allowed", http.StatusMethodNotAllowed)
		return
	}

	h.mu.Lock()
	err := h.broker.SaveSnapshot()
	h.mu.Unlock()

	if err != nil {
		http.Error(w, "Error saving broker snapshot: "+err.Error(), http.StatusInternalServerError)
		return
	}

	response := map[string]string{
		"message": "Broker snapshot saved successfully.",
	}
	jsonResponse(w, response)
}




func jsonResponse(w http.ResponseWriter, data interface{}) {
    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(data)
}

