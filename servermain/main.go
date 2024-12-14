package main

import (
	"fmt"
	"kv/broker"
	"net/http"
)

func main() {
	// Initialize the broker
	b := broker.NewBroker()

	err := b.StartPeering()
	if err != nil {
		panic("Failed to start peering: " + err.Error())
	}
	// Load KVStore configurations from the JSON file
	configs, err := broker.LoadKVStoresConfig("kvstores_config.json")
	if err != nil {
		panic("Failed to load KVStore configurations: " + err.Error())
	}

	// Initialize and add stores to the broker
	for _, config := range configs {
		if err := b.CreateStore(config.Name, config.IPAddress); err != nil {
			panic("Failed to add store: " + err.Error())
		}
	}

	// Create a new BrokerHandler
	handler := broker.NewBrokerHandler(b)

	// Setup HTTP routes
	handler.SetupRoutes()

	handler.GetBroker().GetList().DisplayForward()

	// Start the HTTP server
	fmt.Println("Starting broker web server on :8080")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		fmt.Println("Error starting server:", err)
	}
}
