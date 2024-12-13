package main

import (
	"fmt"
	"kv/kvstore"
	"os"
	"strings"
	"time"
)

func main() {
	broker := NewBroker()

	// Initialize default KVStore and add to broker
	defaultStore := kvstore.NewKVStore("default", broker)
	err := broker.AddStore(defaultStore)
	if err != nil {
		fmt.Println("Error adding default store:", err)
		return
	}
	currentStoreName := "default"

	var command string

	for {
		fmt.Println("\nEnter a command (set, get, delete, save, load, print, list-kvs, switch-kv, new-kv, enable-snapshot, exit):")
		fmt.Scanln(&command)

		switch command {
		case "list-kvs":
			fmt.Println("Available stores:")
			for _, name := range broker.ListStores() {
				fmt.Println(name)
			}

		case "set":
			var key, value string
			fmt.Println("Enter key:")
			fmt.Scanln(&key)
			fmt.Println("Enter value:")
			fmt.Scanln(&value)
			store, err := broker.GetStore(currentStoreName)
			if err != nil {
				fmt.Println("Error retrieving store:", err)
				continue
			}
			store.Set(key, value)
			fmt.Println("Set operation successful.")

		case "get":
			var key string
			fmt.Println("Enter key:")
			fmt.Scanln(&key)
			store, err := broker.GetStore(currentStoreName)
			if err != nil {
				fmt.Println("Error retrieving store:", err)
				continue
			}
			val, err := store.Get(key)
			if err != nil {
				fmt.Println("Error retrieving key:", err)
			} else {
				fmt.Println("Value:", val)
			}

		case "delete":
			var key string
			fmt.Println("Enter key:")
			fmt.Scanln(&key)
			store, err := broker.GetStore(currentStoreName)
			if err != nil {
				fmt.Println("Error retrieving store:", err)
				continue
			}
			err = store.Delete(key)
			if err != nil {
				fmt.Println("Error deleting key:", err)
			} else {
				fmt.Println("Delete operation successful.")
			}

		case "save":
			store, err := broker.GetStore(currentStoreName)
			if err != nil {
				fmt.Println("Error retrieving store:", err)
				continue
			}
			err = store.SaveToDisk()
			if err != nil {
				fmt.Println("Error saving data to disk:", err)
			} else {
				fmt.Printf("Snapshot saved successfully as %s.snapshot.json.\n", currentStoreName)
			}

		case "load":
			fmt.Println("Available snapshots:")
			snapshots, err := listSnapshots()
			if err != nil {
				fmt.Println("Error listing snapshots:", err)
				break
			}
			for _, snapshot := range snapshots {
				fmt.Println(snapshot)
			}
			var filename string
			fmt.Println("Enter the filename to load from:")
			fmt.Scanln(&filename)
			store, err := broker.GetStore(currentStoreName)
			if err != nil {
				fmt.Println("Error retrieving store:", err)
				continue
			}
			err = store.LoadFromDisk(filename)
			if err != nil {
				fmt.Println("Error loading data from disk:", err)
			} else {
				fmt.Println("Data loaded successfully from", filename)
			}

		case "print":
			store, err := broker.GetStore(currentStoreName)
			if err != nil {
				fmt.Println("Error retrieving store:", err)
				continue
			}
			store.PrintData()

		case "switch-kv":
			var storeName string
			fmt.Println("Enter the name of the store to switch to:")
			fmt.Scanln(&storeName)
			if broker.StoreExists(storeName) {
				currentStoreName = storeName
				fmt.Println("Switched to store:", storeName)
			} else {
				fmt.Println("Store not found.")
			}

		case "new-kv":
			var storeName string
			fmt.Println("Enter the name of the new store:")
			fmt.Scanln(&storeName)
			newStore := kvstore.NewKVStore(storeName, broker)
			err := broker.AddStore(newStore)
			if err != nil {
				fmt.Println("Error creating new store:", err)
			} else {
				currentStoreName = storeName
				fmt.Println("New store created and switched to:", storeName)
			}

		case "enable-snapshot":
			var intervalSec int
			fmt.Println("Enter the snapshot interval in seconds:")
			fmt.Scanln(&intervalSec)
			interval := time.Duration(intervalSec) * time.Second
			store, err := broker.GetStore(currentStoreName)
			if err != nil {
				fmt.Println("Error retrieving store:", err)
				continue
			}
			store.StartPeriodicSnapshots(interval)
			fmt.Printf("Periodic snapshots enabled for store %s with interval %d seconds.\n", currentStoreName, intervalSec)

		case "exit":
			fmt.Println("Exiting program.")
			return

		default:
			fmt.Println("Unknown command. Please try again.")
		}
	}
}

// listSnapshots lists all snapshot files in the current directory.
func listSnapshots() ([]string, error) {
	files, err := os.ReadDir(".")
	if err != nil {
		return nil, err
	}

	var snapshots []string
	for _, file := range files {
		if strings.HasSuffix(file.Name(), ".snapshot.json") {
			snapshots = append(snapshots, file.Name())
		}
	}
	return snapshots, nil
}
