package main

import (
	"fmt"
	"kv/kvstore"
	"os"
	"strings"
	"time"
)

func main() {
	store := kvstore.NewKVStore("default")
	var command string

	var stores = make(map[string]*kvstore.KVStore)
	stores["default"] = store
	currentStore := "default"

	for {
		fmt.Println("\nEnter a command (set, get, delete, save, load, print, list-kvs, switch-kv, new-kv, enable-snapshot, exit):")
		fmt.Scanln(&command)

		switch command {
		case "list-kvs":
			fmt.Println("Available stores:")
			for name := range stores {
				fmt.Println(name)
			}
		case "set":
			var key, value string
			fmt.Println("Enter key:")
			fmt.Scanln(&key)
			fmt.Println("Enter value:")
			fmt.Scanln(&value)
			stores[currentStore].Set(key, value)
			fmt.Println("Set operation successful.")

		case "get":
			var key string
			fmt.Println("Enter key:")
			fmt.Scanln(&key)
			val, err := stores[currentStore].Get(key)
			if err != nil {
				fmt.Println("Error retrieving key:", err)
			} else {
				fmt.Println("Value:", val)
			}

		case "delete":
			var key string
			fmt.Println("Enter key:")
			fmt.Scanln(&key)
			err := stores[currentStore].Delete(key)
			if err != nil {
				fmt.Println("Error deleting key:", err)
			} else {
				fmt.Println("Delete operation successful.")
			}

		case "save":
			err := stores[currentStore].SaveToDisk()
			if err != nil {
				fmt.Println("Error saving data to disk:", err)
			} else {
				fmt.Printf("Snapshot saved successfully as %s.snapshot.json.\n", currentStore)
			}
		case "load":
			fmt.Println("Available snapshots:")
			// Assuming you have a function to list files in the directory
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
			err = stores[currentStore].LoadFromDisk(filename)
			if err != nil {
				fmt.Println("Error loading data from disk:", err)
			} else {
				fmt.Println("Data loaded successfully from", filename)
			}

		case "print":
			stores[currentStore].PrintData()

		case "switch-kv":
			var storeName string
			fmt.Println("Enter the name of the store to switch to:")
			fmt.Scanln(&storeName)
			if _, exists := stores[storeName]; exists {
				currentStore = storeName
				fmt.Println("Switched to store:", storeName)
			} else {
				fmt.Println("Store not found.")
			}
		case "enable-snapshot":
			var interval time.Duration
			fmt.Println("Enter the snapshot interval in seconds:")
			fmt.Scanln(&interval)
			stores[currentStore].StartPeriodicSnapshots(interval)
			fmt.Printf("Periodic snapshots enabled for store %s with interval %d seconds.\n", currentStore, interval)

		case "new-kv":
			var storeName string
			fmt.Println("Enter the name of the new store:")
			fmt.Scanln(&storeName)
			stores[storeName] = kvstore.NewKVStore(storeName)
			currentStore = storeName
			fmt.Println("New store created and switched to:", storeName)

		case "exit":
			fmt.Println("Exiting program.")
			return

		default:
			fmt.Println("Unknown command. Please try again.")
		}
	}
}

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
