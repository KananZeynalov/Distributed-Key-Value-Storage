package main

import (
	"fmt"
	"kv/broker"
	"net/http"
)

func main() {
	// Initialize the broker
	b := broker.NewBroker()

	// Start peering
	err := b.StartPeering()
	if err != nil {
		panic("Failed to start peering: " + err.Error())
	}

	// Create a new BrokerHandler
	handler := broker.NewBrokerHandler(b)

	// Setup HTTP routes
	handler.SetupRoutes()

	// Display the peer list (initially empty)
	handler.GetBroker().GetList().DisplayForward()

	// Start the HTTP server
	fmt.Println("Starting broker web server on :8080")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		fmt.Println("Error starting server:", err)
	}
}
