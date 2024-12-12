package main

import (
	"fmt"
	"kv/kvstore" 
	"time"
)

func main() {
	store := kvstore.NewKVStore()

	// Set key-value pairs
	fmt.Println("Setting key1 to value1...")
	store.Set("key1", "value1")

	fmt.Println("Setting key2 to value2...")
	store.Set("key2", "value2")

	// Save the current data to disk
	err := store.SaveToDisk("snapshot.json")
	if err != nil {
		fmt.Println("Error saving data to disk:", err)
		return
	}

	fmt.Println("Snapshot saved successfully.")

	// Get and print values
	fmt.Println("\nRetrieving key1...")
	val, err := store.Get("key1")
	if err != nil {
		fmt.Println("Error retrieving key1:", err)
	} else {
		fmt.Println("key1:", val)
	}

	// Print the entire store (debugging purposes)
	fmt.Println("\nCurrent in-memory data:")
	store.PrintData() // Use the custom PrintData method for cleaner output

	// Delete a key
	fmt.Println("\nDeleting key1...")
	err = store.Delete("key1")
	if err != nil {
		fmt.Println("Error deleting key1:", err)
	} else {
		fmt.Println("key1 successfully deleted")
	}

	// Try to get the deleted key
	fmt.Println("\nAttempting to retrieve deleted key1...")
	_, err = store.Get("key1")
	if err != nil {
		fmt.Println("As expected, key1 does not exist. Error:", err)
	}


	// Load data from disk
	err = store.LoadFromDisk("snapshot.json")
	if err != nil {
		fmt.Println("Error loading data from disk:", err)
		return
	}

	// Print the loaded data
	fmt.Println("\nCurrent in-memory data after loading from disk:")
	store.PrintData()

	// Remove old memory to try new snapshot technique

	store.Delete("key1")
	store.Delete("key2") // guys we are deleting key1 and key2 to see newperiodic snapshots
	store.SaveToDisk("snapshot.json") // TESTING TIME TICKER SO GETTING RID OF OLD MEMORY

	// Simulate some activity
	store.Set("kenan", "value1")
	store.Set("bilal", "value2")
	store.Set("ediz", "value3")

	// Start periodic snapshots every 10 seconds
	store.StartPeriodicSnapshots("snapshot.json", 10*time.Second)

	fmt.Println("\nCurrent in-memory data after setting keys:")
	store.PrintData()

	// Wait to observe periodic snapshots
	fmt.Println("\nWaiting for periodic snapshots to trigger...")
	time.Sleep(30 * time.Second) // guys keep program open for 30 seconds to see periodic snapshots

	fmt.Println("Exiting program.")

}