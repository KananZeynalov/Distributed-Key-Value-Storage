package broker

import (
	"encoding/json"
	"fmt"
	"net/http"
	//"os"
	"sync"
)

// Wraps a broker to expose it via HTTP.
type BrokerHandler struct {
	broker *Broker
	mu     sync.RWMutex
}

// GetBroker returns the broker instance.
func (h *BrokerHandler) GetBroker() *Broker {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.broker
}

// Creates a new BrokerHandler instance.
func NewBrokerHandler(b *Broker) *BrokerHandler {
	return &BrokerHandler{broker: b}
}

type RegisterRequest struct {
	Name      string `json:"name"`
	IPAddress string `json:"ip_address"`
}


// SetupRoutes sets up HTTP routes for the broker.
func (h *BrokerHandler) SetupRoutes() {
	//kv store basic operations
	http.HandleFunc("/set", h.SetHandler)
	http.HandleFunc("/get", h.GetHandler)
	http.HandleFunc("/getall", h.GetAllHandler)
	http.HandleFunc("/stores/list", h.ListStoresHandler)
	http.HandleFunc("/delete", h.DeleteHandler)

	//kv store snapshot operations
	http.HandleFunc("/kvstore/snapshot/manual", h.ManualSnapshotHandler)
	http.HandleFunc("/kvstore/snapshot/periodic", h.SnapshotKVStoreHandler)
	// http.HandleFunc("/kvstore/snapshot/load", h.LoadKVSToreSnapshotHandler) TODO: Implement this
	http.HandleFunc("/kvstore/new", h.NewKVHandler)

	//broker snapshot operations
	http.HandleFunc("/broker/snapshot/periodic", h.SnapshotBrokerHandler)
	// http.HandleFunc("/broker/snapshot/load", h.LoadBrokerSnapshotHandler) TODO: Implement this
	http.HandleFunc("/register", h.RegisterHandler)

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

// type KVStoreConfig struct {
// 	Name      string `json:"Name"`
// 	IPAddress string `json:"IPAddress"`
// }

// func LoadKVStoresConfig(filepath string) ([]KVStoreConfig, error) {
//     fmt.Printf("Loading KVStore configurations from file: %s\n", filepath)

//     file, err := os.Open(filepath)
//     if err != nil {
//         return nil, fmt.Errorf("failed to open config file: %w", err)
//     }
//     defer file.Close()

//     var configs []KVStoreConfig
//     decoder := json.NewDecoder(file)
//     if err := decoder.Decode(&configs); err != nil {
//         return nil, fmt.Errorf("failed to decode config file: %w", err)
//     }

//     fmt.Println("Loaded KVStore configurations:")
//     for _, config := range configs {
//         fmt.Printf("  Name: %s, IP Address: %s\n", config.Name, config.IPAddress)
//     }

//     return configs, nil
// }


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

// SnapshotKVStoreHandler: POST /snapshot/enable { "storename": "...", "interval": <seconds> }
func (h *BrokerHandler) SnapshotKVStoreHandler(w http.ResponseWriter, r *http.Request) {
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
		Name      string `json:"Name"`
		IPAddress string `json:"IPAddress"`
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

// RegisterHandler handles registration of KVStore instances
func (h *BrokerHandler) RegisterHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST is allowed", http.StatusMethodNotAllowed)
		return
	}

	var req RegisterRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	// Create the store in the Broker
	err := h.broker.CreateStore(req.Name, req.IPAddress)
	if err != nil {
		http.Error(w, "Failed to create store: "+err.Error(), http.StatusBadRequest)
		return
	}

	// Optionally, notify existing peers about the new store
	NotifyPeersOfEachOther(h.broker.peerlist)

	// Respond with success
	response := map[string]string{
		"message": "Store registered successfully",
	}
	jsonResponse(w, response)
}
