package kvstore

import (
	"kv/kvstore"
	"os"
	"testing"
	"time"
)

func TestKVStore_SetAndGet(t *testing.T) {
	store := kvstore.NewKVStore("teststore")
	store.Set("foo", "bar")

	val, err := store.Get("foo")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if val != "bar" {
		t.Fatalf("expected value 'bar', got %v", val)
	}
}

func TestKVStore_GetNonExistentKey(t *testing.T) {
	store := kvstore.NewKVStore("teststore")

	_, err := store.Get("nonexistent")
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
}

func TestKVStore_Delete(t *testing.T) {
	store := kvstore.NewKVStore("teststore")
	store.Set("foo", "bar")

	err := store.Delete("foo")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	_, err = store.Get("foo")
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
}

func TestKVStore_DeleteNonExistentKey(t *testing.T) {
	store := kvstore.NewKVStore("teststore")

	err := store.Delete("nonexistent")
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
}
func TestKVStore_SaveAndLoad(t *testing.T) {
	store := kvstore.NewKVStore("teststore")
	store.Set("foo", "bar")
	store.Set("baz", "qux")

	err := store.SaveToDisk()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	newStore := kvstore.NewKVStore("teststore")
	err = newStore.LoadFromDisk("teststore.snapshot.json")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	val, err := newStore.Get("foo")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if val != "bar" {
		t.Fatalf("expected value 'bar', got %v", val)
	}

	val, err = newStore.Get("baz")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if val != "qux" {
		t.Fatalf("expected value 'qux', got %v", val)
	}

	// Clean up
	defer os.Remove("teststore.snapshot.json")
}
func TestKVStore_SaveToDisk(t *testing.T) {
	store := kvstore.NewKVStore("teststore")
	store.Set("foo", "bar")

	err := store.SaveToDisk()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	// Clean up
	defer os.Remove("teststore.snapshot.json")
}

func TestKVStore_LoadFromDisk(t *testing.T) {
	store := kvstore.NewKVStore("teststore")
	store.Set("foo", "bar")
	store.SaveToDisk()

	newStore := kvstore.NewKVStore("teststore")
	err := newStore.LoadFromDisk("teststore.snapshot.json")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	val, err := newStore.Get("foo")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if val != "bar" {
		t.Fatalf("expected value 'bar', got %v", val)
	}

	// Clean up
	defer os.Remove("teststore.snapshot.json")
}

func TestKVStore_StartPeriodicSnapshots(t *testing.T) {
	store := kvstore.NewKVStore("teststore")
	store.Set("foo", "bar")

	store.StartPeriodicSnapshots(1 * time.Second)

	time.Sleep(2 * time.Second)

	_, err := os.Stat("teststore.snapshot.json")
	if os.IsNotExist(err) {
		t.Fatalf("expected snapshot file to exist")
	}

	// Clean up
	defer os.Remove("teststore.snapshot.json")
}
