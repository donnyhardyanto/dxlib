package databases

import (
	"fmt"
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
	if n := len(dm.Databases); n != 5 {
		t.Fatalf("registered %d databases, want 5", n)
	}
}
