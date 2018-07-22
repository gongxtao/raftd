package store

import (
	"testing"
	"time"
)

var (
	path = "./raftdb"

	addr = "localhost:4049"
	localId = "node1"
)

func TestNewStore(t *testing.T) {
	store, err := NewStore(path, localId, addr)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(10 * time.Second)

	if err := store.Set("name", "gongxt"); err != nil {
		t.Errorf("failed to set: %v", err)
		return
	}

	if err := store.Set("name", "gongxt1"); err != nil {
		t.Errorf("failed to set: %v", err)
		return
	}

	v, err := store.Get("name")
	if err != nil {
		t.Errorf("failed to get: %v", err)
		return
	}

	if v != "gongxt1" {
		t.Errorf("expect: %s, got: %s", "gongxt", v)
	}

	time.Sleep(10 * time.Second)
}
