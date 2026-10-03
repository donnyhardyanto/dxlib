package databases

import (
	"fmt"
	"reflect"
	"sync"
	"testing"
)

func newTestDatabaseManager() *DXDatabaseManager {
	return &DXDatabaseManager{Databases: map[string]*DXDatabase{}, Scripts: map[string]*DXDatabaseScript{}}
}

// NewDatabase used to read the map before taking the lock, so under -race a
// NewDatabase racing a GetOrCreate that creates is reported.
func TestNewDatabaseConcurrentWithCreate(t *testing.T) {
	dm := newTestDatabaseManager()
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(2)
		name := fmt.Sprintf("t%d", i%5)
		go func() { defer wg.Done(); dm.NewDatabase(name, false, false) }()
		go func() { defer wg.Done(); dm.GetOrCreate(name) }()
	}
	wg.Wait()
	if n := len(dm.Names()); n != 5 {
		t.Fatalf("registered %d databases, want 5", n)
	}
}

func TestSetNamesSnapshot(t *testing.T) {
	dm := newTestDatabaseManager()
	b := &DXDatabase{NameId: "b"}
	dm.Set("b", b)
	dm.NewDatabase("a", false, false)

	if got := dm.Names(); !reflect.DeepEqual(got, []string{"a", "b"}) {
		t.Fatalf("Names() = %v", got)
	}
	snap := dm.Snapshot()
	if snap["b"] != b || len(snap) != 2 {
		t.Fatalf("Snapshot() = %v", snap)
	}
	// The snapshot is a copy: changing it leaves the registry alone.
	delete(snap, "a")
	if dm.Get("a") == nil {
		t.Fatal("deleting from the snapshot removed the registry entry")
	}

	replacement := &DXDatabase{NameId: "b"}
	dm.Set("b", replacement)
	if dm.Get("b") != replacement {
		t.Fatal("Set did not replace the handle")
	}
}

func TestRangeWhileCreating(t *testing.T) {
	dm := newTestDatabaseManager()
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			dm.GetOrCreate(fmt.Sprintf("t%d", i))
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			for range dm.Snapshot() {
			}
			_ = dm.Names()
		}
	}()
	wg.Wait()
}
