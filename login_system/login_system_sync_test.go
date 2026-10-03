package login_system

import (
	"context"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/donnyhardyanto/dxlib/log"
)

func newSyncTestLoginSystem(interval time.Duration) *LoginSystem {
	return &LoginSystem{
		Storage:      DBOnly,
		SyncInterval: interval,
		Log:          log.NewLog(nil, context.Background(), "login-system-sync-test"),
	}
}

// Stop has to end the sync goroutine, not only the ticker: a stopped ticker
// never closes its channel, so a loop ranging over it stayed blocked forever.
func TestSyncLoopEndsOnStop(t *testing.T) {
	before := runtime.NumGoroutine()
	l := newSyncTestLoginSystem(5 * time.Millisecond)
	var runs atomic.Int64
	l.startSyncLoop("test", func() { runs.Add(1) })
	if l.SyncTicker == nil {
		t.Fatal("SyncTicker not set when startSyncLoop returns")
	}

	deadline := time.Now().Add(2 * time.Second)
	for runs.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if runs.Load() == 0 {
		t.Fatal("task never ran")
	}

	l.stopSyncLoop()
	l.stopSyncLoop() // a second Stop must not panic on a closed channel

	for runtime.NumGoroutine() > before && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if n := runtime.NumGoroutine(); n > before {
		t.Fatalf("goroutines: %d before start, %d after stop", before, n)
	}
	settled := runs.Load()
	time.Sleep(30 * time.Millisecond)
	if runs.Load() != settled {
		t.Fatal("task still running after stop")
	}
}

// A zero interval used to panic inside the goroutine, where it was recovered
// and logged; Start must not panic now that the ticker is made before it.
func TestSyncLoopWithZeroIntervalIsDisabled(t *testing.T) {
	l := newSyncTestLoginSystem(0)
	l.startSyncLoop("test", func() { t.Error("task ran with a zero interval") })
	if l.SyncTicker != nil {
		t.Fatal("ticker created for a zero interval")
	}
	l.stopSyncLoop()
}
