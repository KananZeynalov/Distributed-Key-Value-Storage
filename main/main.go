package main

import (
	"fmt"
	"kv/broker"
	"os"
	"strings"
	"time"
)

func main() {
	broker := broker.NewBroker()
	// Initialize default KVStore and add to broker
	err := broker.CreateStore("default")
	if err != nil {
		fmt.Println("Error creating default store:", err)
		return
	}

	var command string

	for {
		fmt.Printf("Enter a command (set, get, delete, manual-snapshot, load, print, list-kvs, new-kv, enable-snapshot, snapshot-broker, exit):\n")
		fmt.Scanln(&command)
		if command == "snapshot-broker" {
			err := broker.SaveSnapshot()
			if err != nil {
				fmt.Println("Error saving broker snapshot:", err)
			} else {
				fmt.Println("Broker snapshot saved successfully.")
			}
			continue
		}
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

			broker.SetKey(key, value)

		case "get":
			var key string
			fmt.Println("Enter key:")
			fmt.Scanln(&key)
			broker.GetKey(key)

		case "delete":
			var key string
			fmt.Println("Enter key:")
			fmt.Scanln(&key)
			broker.DeleteKey(key)

		case "manual-snapshot":
			broker.ManualSnapshotStore()

		case "load":
			fmt.Println("Available snapshots are:")
			snapshots, err := listSnapshots()
			if err != nil {
				fmt.Println("Error listing snapshots:", err)
				break
			}
			for _, snapshot := range snapshots {
				fmt.Printf(" - %s\n", snapshot)
			}
			var filename string
			fmt.Println("Enter the snapshot name to load from:")
			fmt.Scanln(&filename)

			var storename string
			fmt.Println("Enter the store name to load to:")
			fmt.Scanln(&storename)

			broker.LoadStoreFromSnapshot(storename, filename)

		case "print":
			err := broker.ListAllData()
			if err != nil {
				fmt.Println("Error retrieving store:", err)
				continue
			}

		case "new-kv":
			var storeName string
			fmt.Println("Enter the name of the new store:")
			fmt.Scanln(&storeName)
			err := broker.CreateStore(storeName)
			if err != nil {
				fmt.Println("Error creating new store:", err)
			} else {
				fmt.Println("New store created:", storeName)
			}

		case "enable-snapshot":
			var storeName string
			fmt.Println("Enter the name of the new store:")
			fmt.Scanln(&storeName)

			var intervalSec int
			fmt.Println("Enter the snapshot interval in seconds:")
			fmt.Scanln(&intervalSec)
			interval := time.Duration(intervalSec) * time.Second
			store, err := broker.GetStore(storeName)
			if err != nil {
				fmt.Println("Error retrieving store:", err)
				continue
			}
			store.StartPeriodicSnapshots(interval)
			fmt.Printf("Periodic snapshots enabled for store %s with interval %d seconds.\n", storeName, intervalSec)

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
