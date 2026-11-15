package redis

import (
	"context"
	"net"
	"runtime"
	"strings"
	"testing"
	"time"
)

// refusedAddress is a loopback address nothing listens on: a port the kernel
// handed out and that was closed again.
func refusedAddress(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	return addr
}

// settledGoroutines waits until the goroutine count is at most limit or the
// deadline passes, and returns the last count. go-redis pool goroutines that
// were dialing when the client closed need up to a second to see it.
func settledGoroutines(limit int, deadline time.Duration) int {
	end := time.Now().Add(deadline)
	n := runtime.NumGoroutine()
	for n > limit && time.Now().Before(end) {
		time.Sleep(50 * time.Millisecond)
		runtime.GC()
		n = runtime.NumGoroutine()
	}
	return n
}

// BUG-003: each failed Connect left a go-redis Ring behind whose heartbeat
// goroutine runs for the life of the process.
func TestFailedConnectLeavesNoGoroutineBehind(t *testing.T) {
	const attempts = 20
	const slack = 2

	cases := map[string]func(addr string) *DXRedis{
		"single node": func(addr string) *DXRedis {
			return &DXRedis{NameId: "leak", IsConfigured: true, Address: addr, Context: context.Background()}
		},
		"sentinel": func(addr string) *DXRedis {
			return &DXRedis{NameId: "leak-sentinel", IsConfigured: true, Context: context.Background(),
				HasSentinel: true, SentinelMasterName: "mymaster", SentinelAddresses: []string{addr}}
		},
	}
	for name, build := range cases {
		t.Run(name, func(t *testing.T) {
			addr := refusedAddress(t)
			before := settledGoroutines(0, 200*time.Millisecond)
			for i := 0; i < attempts; i++ {
				r := build(addr)
				err := r.Connect()
				if err == nil {
					_ = r.Disconnect()
					t.Fatalf("attempt %d: Connect to %s succeeded; nothing should listen there", i, addr)
				}
				if !strings.Contains(err.Error(), "Cannot connect to Redis") {
					t.Fatalf("attempt %d: want a connect error, got %v", i, err)
				}
				if r.Connected || r.Connection != nil {
					t.Fatalf("attempt %d: a failed Connect left Connected=%v Connection=%v", i, r.Connected, r.Connection)
				}
			}
			after := settledGoroutines(before+slack, 3*time.Second)
			if after > before+slack {
				t.Fatalf("goroutines grew from %d to %d over %d failed Connects to %s", before, after, attempts, addr)
			}
		})
	}
}
