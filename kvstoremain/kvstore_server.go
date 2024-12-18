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
	"bytes"
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
	//key value store routes
	http.HandleFunc("/get", h.GetHandler)
	http.HandleFunc("/set", h.SetHandler)
	http.HandleFunc("/name", h.GetNameHandler)
	http.HandleFunc("/getall", h.GetAllDataHandler)

	//peering routes
	http.HandleFunc("/notify", h.PeerNotificationHandler) //comes from broker, when it tells you who your peer is
	http.HandleFunc("/peer-dead", h.PeerDeadHandler)      //comes from broker, when your peer is dead. then you load peers data from disk
	http.HandleFunc("/peer-backup", h.PeerBackupHandler)  //comes from peer, when this comes you send all your data in response field

	//snapshot routes
	http.HandleFunc("/save", h.SaveToDiskHandler)
	http.HandleFunc("/load", h.LoadFromDiskHandler)
	http.HandleFunc("/start-snapshots", h.StartPeriodicSnapshotsHandler)

}

func (h *KVStoreHandler) PeerDeadHandler(w http.ResponseWriter, r *http.Request) {
	var requestData map[string]string
	if err := json.NewDecoder(r.Body).Decode(&requestData); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	if err := h.kvstore.LoadAndMergeFromDisk(); err != nil {
		http.Error(w, "Failed to load data from peer backup", http.StatusInternalServerError)
		return
	}

	response := map[string]string{"status": "Data successfully loaded from peer backup"}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func RequestPeerBackup(peerURL string) {
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

	file, err := os.Create("peer.snapshot.json")
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

func (h *KVStoreHandler) PeerBackupHandler(w http.ResponseWriter, r *http.Request) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	data := h.kvstore.GetAllData()
	jsonResponse(w, data)
}

func (h *KVStoreHandler) GetNameHandler(w http.ResponseWriter, r *http.Request) {
	response := map[string]string{"name": h.kvstore.Name}
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

	// Handle the peer IP as needed, e.g., store it, initiate replication, etc.
	h.kvstore.SetPeerIP(peerIP)

	// Optionally, respond with acknowledgment
	response := map[string]string{"message": "Peer notified successfully"}
	jsonResponse(w, response)
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

	// Register with Broker
	brokerURL := os.Getenv("BROKER_URL") // e.g., "http://localhost:8080/register"
	if brokerURL == "" {
		fmt.Println("BROKER_URL environment variable not set")
		os.Exit(1)
	}
err := RegisterWithBroker(brokerURL, kvname, fmt.Sprintf("localhost:%s", port))
	if err != nil {
		fmt.Println("Failed to register with Broker:", err)
		os.Exit(1)
	}

	// Start the HTTP server
	serverAddress := fmt.Sprintf(":%s", port)
	fmt.Printf("Starting KVStore web server on %s\n", serverAddress)
	if err := http.ListenAndServe(serverAddress, nil); err != nil {
		fmt.Printf("Error starting server on %s: %v\n", serverAddress, err)
		os.Exit(1)
	}
}

// RegisterWithBroker sends a registration request to the Broker.
func RegisterWithBroker(brokerURL, name, ip string) error {
	data := map[string]string{
		"name":       name,
		"ip_address": ip,
	}
	jsonData, err := json.Marshal(data)
	if err != nil {
		return err
	}

	resp, err := http.Post(brokerURL, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to register with broker, status code: %d", resp.StatusCode)
	}

	return nil
}

