package login_system

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	goredis "github.com/go-redis/redis/v8"

	"github.com/donnyhardyanto/dxlib/log"
)

// A logout whose Redis deletes failed used to return nil: DEL and HDEL errors
// were dropped, so the session stayed live while the caller thought it was
// gone. These run against a Redis address that refuses connections, so every
// command fails the way it does when the store is down.

const testSessionKey = "abcdef0123456789secret"

// refusedRedisAddr returns a local address nothing listens on.
func refusedRedisAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

func newDownRedisLoginSystem(t *testing.T, tenantId int64) *LoginSystem {
	t.Helper()
	client := goredis.NewClient(&goredis.Options{
		Addr:        refusedRedisAddr(t),
		DialTimeout: 200 * time.Millisecond,
		MaxRetries:  -1,
	})
	t.Cleanup(func() { _ = client.Close() })
	return &LoginSystem{
		TenantId:    tenantId,
		Storage:     RedisOnly,
		KeyPrefix:   fmt.Sprintf("t%d:", tenantId),
		RedisClient: client,
		Log:         log.NewLog(nil, context.Background(), "login-system-errors-test"),
	}
}

func TestRedisOnlyUnregisterReturnsDeleteErrors(t *testing.T) {
	l := newDownRedisLoginSystem(t, 7)
	fired := false
	l.OnSessionUnregistered = func(string, TokenRemoveReasonType, any, map[string]any) { fired = true }

	err := l.InstanceUnregister(testSessionKey, LoggedOut, nil)
	if err == nil {
		t.Fatal("unregister against a Redis that is down returned nil")
	}
	msg := err.Error()
	for _, want := range []string{"DEL t7:abcdef01...", "HDEL t7:" + SessionStoreName, "tenant 7", "redisOnlyInstanceUnregister"} {
		if !strings.Contains(msg, want) {
			t.Errorf("error should name %q: %s", want, msg)
		}
	}
	if strings.Contains(msg, testSessionKey) {
		t.Errorf("error carries the whole session key: %s", msg)
	}
	if fired {
		t.Error("the unregister callback fired although the session was not removed")
	}
}

func TestManagerUnregisterReturnsEveryTenantsFailure(t *testing.T) {
	m := &LoginSystemManager{
		Instances: map[int64]*LoginSystem{
			1: newDownRedisLoginSystem(t, 1),
			2: newDownRedisLoginSystem(t, 2),
		},
		Log: log.NewLog(nil, context.Background(), "login-system-manager-errors-test"),
	}
	err := m.InstanceUnregister(testSessionKey, LoggedOut, nil)
	if err == nil {
		t.Fatal("the fallback scan returned nil although every tenant failed")
	}
	msg := err.Error()
	for _, want := range []string{"InstanceUnregister", "2 of 2 tenants", "tenant 1:", "tenant 2:", "DEL", "HDEL"} {
		if !strings.Contains(msg, want) {
			t.Errorf("error should name %q: %s", want, msg)
		}
	}
	if strings.Contains(msg, testSessionKey) {
		t.Errorf("error carries the whole session key: %s", msg)
	}
}

func TestManagerGetAndUpdateReturnStoreFailures(t *testing.T) {
	m := &LoginSystemManager{
		Instances: map[int64]*LoginSystem{3: newDownRedisLoginSystem(t, 3)},
		Log:       log.NewLog(nil, context.Background(), "login-system-manager-errors-test"),
	}
	data, err := m.InstanceGet(testSessionKey)
	if err == nil || data != nil {
		t.Fatalf("get against a Redis that is down = (%v, %v), want an error", data, err)
	}
	if !strings.Contains(err.Error(), "tenant 3:") {
		t.Errorf("get error should name the tenant: %v", err)
	}
	if err := m.InstanceUpdateSessionData(testSessionKey, map[string]any{"a": 1}); err == nil || !strings.Contains(err.Error(), "tenant 3:") {
		t.Errorf("update against a Redis that is down = %v, want an error naming the tenant", err)
	}
}

func TestIsSessionNotFound(t *testing.T) {
	for _, err := range []error{
		goredis.Nil,
		fmt.Errorf("redisOnlyInstanceGet:HGET: %w", goredis.Nil),
		ErrSessionNotFound,
		ErrSessionExpired,
		fmt.Errorf("dbOnlyInstanceUpdateSessionData:%w", ErrSessionNotFound),
	} {
		if !IsSessionNotFound(err) {
			t.Errorf("IsSessionNotFound(%v) = false", err)
		}
	}
	if IsSessionNotFound(errors.New("dial tcp: connection refused")) {
		t.Error("a store failure reads as not found")
	}
	if ErrSessionNotFound.Error() != "session_not_found" || ErrSessionExpired.Error() != "session_expired" {
		t.Error("the not-found texts changed; callers may match them")
	}
}

func TestShortSessionKey(t *testing.T) {
	if got := ShortSessionKey(testSessionKey); got != "abcdef01..." {
		t.Errorf("ShortSessionKey = %q", got)
	}
	if got := ShortSessionKey("abc"); got != "abc" {
		t.Errorf("a short key should be kept: %q", got)
	}
}

// failCommandsHook answers HGET with a stored session and HDEL with success
// without reaching Redis, and fails every other command, so an unregister sees
// its DEL fail after its HDEL went through.
type failCommandsHook struct{ session string }

var errHookAnswered = errors.New("answered by the test hook")

func (h failCommandsHook) BeforeProcess(ctx context.Context, cmd goredis.Cmder) (context.Context, error) {
	return ctx, errHookAnswered
}

func (h failCommandsHook) AfterProcess(ctx context.Context, cmd goredis.Cmder) error {
	switch c := cmd.(type) {
	case *goredis.StringCmd:
		if c.Name() == "hget" {
			c.SetErr(nil)
			c.SetVal(h.session)
			return nil
		}
	case *goredis.IntCmd:
		if c.Name() == "hdel" {
			c.SetErr(nil)
			c.SetVal(1)
			return nil
		}
	}
	return errors.New("DEL refused by the test hook")
}

func (h failCommandsHook) BeforeProcessPipeline(ctx context.Context, cmds []goredis.Cmder) (context.Context, error) {
	return ctx, nil
}

func (h failCommandsHook) AfterProcessPipeline(ctx context.Context, cmds []goredis.Cmder) error {
	return nil
}

// When HDEL removed the session's data but DEL of the TTL key failed, the
// session can no longer be read and a retry finds nothing, so the unregister
// callback must fire now; the DEL failure is still returned.
func TestRedisOnlyUnregisterFiresCallbackWhenOnlyDelFailed(t *testing.T) {
	l := newDownRedisLoginSystem(t, 7)
	l.RedisClient.AddHook(failCommandsHook{session: `{"user_id": 42}`})
	var got map[string]any
	l.OnSessionUnregistered = func(_ string, _ TokenRemoveReasonType, _ any, data map[string]any) { got = data }

	err := l.InstanceUnregister(testSessionKey, LoggedOut, nil)
	if err == nil || !strings.Contains(err.Error(), "DEL t7:abcdef01...") {
		t.Fatalf("the failed DEL should be returned, got %v", err)
	}
	if strings.Contains(err.Error(), "HDEL") {
		t.Errorf("HDEL succeeded but is reported as failed: %v", err)
	}
	if got == nil {
		t.Fatal("the unregister callback did not fire although the session's data was removed")
	}
	if fmt.Sprint(got["user_id"]) != "42" {
		t.Errorf("callback got session data %v", got)
	}
}
