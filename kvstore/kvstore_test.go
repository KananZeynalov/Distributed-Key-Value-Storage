package kvstore

import (
	"testing"
)

func TestKVStore(t *testing.T) {
	store := NewKVStore()

	// Test Set and Get
	store.Set("hello", "world")
	val, err := store.Get("hello")
	if err != nil {
		t.Fatalf("Expected 'hello' key to exist, got error: %v", err)
	}
	if val != "world" {
		t.Fatalf("Expected value 'world', got '%s'", val)
	}

	// Test key not found
	_, err = store.Get("nonexistent")
	if err == nil {
		t.Fatalf("Expected an error for non-existent key")
	}

	// Test Update
	store.Set("hello", "universe")
	val, err = store.Get("hello")
	if val != "universe" {
		t.Fatalf("Expected 'universe', got '%s'", val)
	}

	// Test Delete
	err = store.Delete("hello")
	if err != nil {
		t.Fatalf("Unexpected error deleting key: %v", err)
	}

	_, err = store.Get("hello")
	if err == nil {
		t.Fatalf("Expected error after deleting key")
	}
}
