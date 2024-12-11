package main

import (
	"fmt"
	"kv/kvstore" 
)

func main() {
	store := kvstore.NewKVStore()

	// Set key-value pairs
	fmt.Println("Setting key1 to value1...")
	store.Set("key1", "value1")

	fmt.Println("Setting key2 to value2...")
	store.Set("key2", "value2")

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
}