package kvstore

import (
	"kv/broker"
	"testing"
)

func TestKVStore_SetAndGetMultipleValues(t *testing.T) {
	broker := broker.NewBroker()
	broker.CreateStore("teststore")
	broker.SetKey("key1", "value1")
	broker.SetKey("key2", "value2")

	val1, err := broker.GetKey("key1")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if val1 != "value1" {
		t.Fatalf("expected value 'value1', got %v", val1)
	}

	val2, err := broker.GetKey("key2")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if val2 != "value2" {
		t.Fatalf("expected value 'value2', got %v", val2)
	}
}
