package main

import (
	"encoding/json"
	"fmt"
	"kv/kvstore"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"
)

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

type KVStoreConfig struct {
	Name      string `json:"name"`
	IPAddress string `json:"ip_address"`
}

type KVStoreHandler struct {
	kvstore *kvstore.KVStore
	mu      sync.RWMutex
}

func (h *KVStoreHandler) SetHandler(w http.ResponseWriter, r *http.Request) {
	var requestData map[string]string
	if err := json.NewDecoder(r.Body).Decode(&requestData); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	key, keyExists := requestData["key"]
	value, valueExists := requestData["value"]
	if !keyExists || !valueExists {
		http.Error(w, "Missing key or value in request body", http.StatusBadRequest)
		return
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	if err := h.kvstore.Set(key, value); err != nil {
		http.Error(w, "Failed to set key-value pair", http.StatusInternalServerError)
		return
	}

	response := map[string]string{"key": key, "value": value}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func (h *KVStoreHandler) GetHandler(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "Missing key parameter", http.StatusBadRequest)
		return
	}

	h.mu.RLock()
	defer h.mu.RUnlock()

	value, err := h.kvstore.Get(key)
	if err != nil {
		http.Error(w, "Key not found", http.StatusNotFound)
		return
	}

	response := map[string]string{"key": key, "value": value}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func NewKVStoreHandler(b *kvstore.KVStore) *KVStoreHandler {
	return &KVStoreHandler{kvstore: b}
}

func jsonResponse(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(data)
}

func (h *KVStoreHandler) SaveToDiskHandler(w http.ResponseWriter, r *http.Request) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if err := h.kvstore.SaveToDisk(); err != nil {
		http.Error(w, "Failed to save data to disk", http.StatusInternalServerError)
		return
	}

	response := map[string]string{"status": "Data successfully saved to disk"}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func (h *KVStoreHandler) LoadFromDiskHandler(w http.ResponseWriter, r *http.Request) {
	var requestData map[string]string
	if err := json.NewDecoder(r.Body).Decode(&requestData); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	filename, filenameExists := requestData["filename"]
	if !filenameExists {
		http.Error(w, "Missing filename in request body", http.StatusBadRequest)
		return
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	if err := h.kvstore.LoadFromDisk(filename); err != nil {
		http.Error(w, "Failed to load data from disk", http.StatusInternalServerError)
		return
	}

	response := map[string]string{"status": "Data successfully loaded from disk"}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func (h *KVStoreHandler) GetAllDataHandler(w http.ResponseWriter, r *http.Request) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	data := h.kvstore.GetAllData()
	jsonResponse(w, data)
}

func (h *KVStoreHandler) SetupRoutes() {
	http.HandleFunc("/get", h.GetHandler)
	http.HandleFunc("/set", h.SetHandler)
	http.HandleFunc("/name", h.GetNameHandler)
	http.HandleFunc("/getall", h.GetAllDataHandler)
	http.HandleFunc("/notify", h.PeerNotificationHandler)
	http.HandleFunc("/peer-backup", h.PeerBackupHandler)
}

func (h *KVStoreHandler) PeerBackupHandler(w http.ResponseWriter, r *http.Request) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	data := h.kvstore.GetAllData()
	jsonResponse(w, data)
}

func (h *KVStoreHandler) GetNameHandler(w http.ResponseWriter, r *http.Request) {
	response := map[string]string{"name": h.kvstore.Name()}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func (h *KVStoreHandler) PeerNotificationHandler(w http.ResponseWriter, r *http.Request) {
	var requestData map[string]string
	if err := json.NewDecoder(r.Body).Decode(&requestData); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	peerIP, peerIPExists := requestData["peer_ip"]
	if !peerIPExists {
		http.Error(w, "Missing peer_ip in request body", http.StatusBadRequest)
		return
	}

	h.kvstore.SetPeerIP(peerIP)
}

func (h *KVStoreHandler) StartPeriodicSnapshotsHandler(w http.ResponseWriter, r *http.Request) {
	intervalStr := r.URL.Query().Get("interval")
	if intervalStr == "" {
		http.Error(w, "Missing interval parameter", http.StatusBadRequest)
		return
	}

	interval, err := strconv.Atoi(intervalStr)
	if err != nil || interval <= 0 {
		http.Error(w, "Invalid interval parameter", http.StatusBadRequest)
		return
	}

	go h.kvstore.StartPeriodicSnapshots(time.Duration(interval) * time.Second)

	response := map[string]string{"status": "Periodic snapshots started"}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func main() {

	if len(os.Args) < 3 {
		fmt.Println("Usage: kvstore_server <kvname> <port>")
		os.Exit(1)
	}

	kvname := os.Args[1]
	port := os.Args[2]
	kvStoreInstance := kvstore.NewKVStore(kvname, port)

	handler := NewKVStoreHandler(kvStoreInstance)

	// Setup HTTP routes
	handler.SetupRoutes()

	// Start the HTTP server
	fmt.Printf("Starting KVStore web server on :%s\n", port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		fmt.Println("Error starting server:", err)
	}
}