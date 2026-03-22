package login_system

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime/debug"
	"strings"
	"time"

	goredis "github.com/go-redis/redis/v8"
	"github.com/google/uuid"

	"github.com/donnyhardyanto/dxlib/databases"
	dxlibLog "github.com/donnyhardyanto/dxlib/log"
)

// ====================== Type Definitions ======================

type DeviceInstanceType int

const (
	SingleDeviceSingleInstancePerDevice     DeviceInstanceType = iota // Kick all existing sessions for user
	SingleDeviceMultipleInstancePerDevice                             // Kick sessions on OTHER devices
	MultipleDeviceSingleInstancePerDevice                             // Kick sessions on SAME device
	MultipleDeviceMultipleInstancePerDevice                           // No kick-out
)

type StorageType int

const (
	RedisOnly   StorageType = iota // Redis is session store + auth cache
	RedisWithDB                    // Redis is auth cache, PG is authoritative store
	DBOnly                         // PG is both session store and auth cache
)

type TokenLifetimeType int

const (
	ShortLived TokenLifetimeType = iota // Sliding window: renews TTL on every read (web browsers)
	LongLived                           // Fixed expiry: no renewal on read (mobile apps)
)

type TokenRemoveReasonType int

const (
	LoggedOut   TokenRemoveReasonType = iota // User explicitly logged out
	Replaced                                 // Kicked by new login (DeviceInstanceType rules)
	Expired                                  // TTL expired
	ServerError                              // Server error / capacity
	Closed                                   // Server shutdown
	Evicted                                  // Redis eviction
)

// ====================== Callback Types ======================

type OnSessionRegisteredFunc func(sessionKey string, sessionData map[string]any)
type OnSessionUnregisteredFunc func(sessionKey string, reason TokenRemoveReasonType, reasonData any, sessionData map[string]any)

// ====================== Constants ======================

const SessionStoreName = "runtime_currently_user_sessions" // Redis HASH key and PG table name

// ====================== LoginSystem Struct ======================

type LoginSystem struct {
	TenantId              int64  // 0 = single-tenant / system
	KeyPrefix             string // e.g. "t42:" — prepended to all Redis keys
	Type                  DeviceInstanceType
	Storage               StorageType
	TokenLifetime         TokenLifetimeType
	OnSessionRegistered   OnSessionRegisteredFunc
	OnSessionUnregistered OnSessionUnregisteredFunc
	ExpiredTimeDuration   time.Duration
	Log                   dxlibLog.DXLog
	RedisClient           *goredis.Client // nil in DBOnly mode
	RedisDB               int
	Db                    *databases.DXDatabase // nil in RedisOnly mode
	SyncInterval          time.Duration
	SyncTicker            *time.Ticker
	PubSubKeyExpired      *goredis.PubSub // nil in DBOnly mode
	PubSubKeyEvicted      *goredis.PubSub // nil in DBOnly mode
}

// ====================== Constructors ======================

// NewLoginSystem creates a Redis-only backed LoginSystem.
// tenantId: 0 = single-tenant/system (no key prefix), >0 = per-tenant (prefixed Redis keys).
func NewLoginSystem(tenantId int64, aType DeviceInstanceType, tokenLifetime TokenLifetimeType,
	expiredTime, syncInterval time.Duration,
	redisAddress, redisUsername, redisPassword string, redisDB int,
	onRegistered OnSessionRegisteredFunc, onUnregistered OnSessionUnregisteredFunc,
	log dxlibLog.DXLog) *LoginSystem {

	keyPrefix := ""
	if tenantId > 0 {
		keyPrefix = fmt.Sprintf("t%d:", tenantId)
	}
	return &LoginSystem{
		TenantId:              tenantId,
		KeyPrefix:             keyPrefix,
		Type:                  aType,
		Storage:               RedisOnly,
		TokenLifetime:         tokenLifetime,
		ExpiredTimeDuration:   expiredTime,
		SyncInterval:          syncInterval,
		OnSessionRegistered:   onRegistered,
		OnSessionUnregistered: onUnregistered,
		Log:                   log,
		RedisDB:               redisDB,
		RedisClient: goredis.NewClient(&goredis.Options{
			Addr:     redisAddress,
			Username: redisUsername,
			Password: redisPassword,
			DB:       redisDB,
		}),
	}
}

// NewLoginSystemWithDB creates a Redis+DB backed LoginSystem.
// Redis is the auth cache (fast path). PostgreSQL is the authoritative session store.
// tenantId: 0 = single-tenant/system, >0 = per-tenant.
func NewLoginSystemWithDB(tenantId int64, aType DeviceInstanceType, tokenLifetime TokenLifetimeType,
	expiredTime, syncInterval time.Duration,
	redisAddress, redisUsername, redisPassword string, redisDB int,
	db *databases.DXDatabase,
	onRegistered OnSessionRegisteredFunc, onUnregistered OnSessionUnregisteredFunc,
	log dxlibLog.DXLog) *LoginSystem {

	keyPrefix := ""
	if tenantId > 0 {
		keyPrefix = fmt.Sprintf("t%d:", tenantId)
	}
	return &LoginSystem{
		TenantId:              tenantId,
		KeyPrefix:             keyPrefix,
		Type:                  aType,
		Storage:               RedisWithDB,
		TokenLifetime:         tokenLifetime,
		ExpiredTimeDuration:   expiredTime,
		SyncInterval:          syncInterval,
		OnSessionRegistered:   onRegistered,
		OnSessionUnregistered: onUnregistered,
		Log:                   log,
		RedisDB:               redisDB,
		Db:                    db,
		RedisClient: goredis.NewClient(&goredis.Options{
			Addr:     redisAddress,
			Username: redisUsername,
			Password: redisPassword,
			DB:       redisDB,
		}),
	}
}

// NewLoginSystemDBOnly creates a DB-only backed LoginSystem. No Redis dependency.
// tenantId: 0 = single-tenant/system, >0 = per-tenant.
func NewLoginSystemDBOnly(tenantId int64, aType DeviceInstanceType, tokenLifetime TokenLifetimeType,
	expiredTime, syncInterval time.Duration,
	db *databases.DXDatabase,
	onRegistered OnSessionRegisteredFunc, onUnregistered OnSessionUnregisteredFunc,
	log dxlibLog.DXLog) *LoginSystem {

	keyPrefix := ""
	if tenantId > 0 {
		keyPrefix = fmt.Sprintf("t%d:", tenantId)
	}
	return &LoginSystem{
		TenantId:              tenantId,
		KeyPrefix:             keyPrefix,
		Type:                  aType,
		Storage:               DBOnly,
		TokenLifetime:         tokenLifetime,
		ExpiredTimeDuration:   expiredTime,
		SyncInterval:          syncInterval,
		OnSessionRegistered:   onRegistered,
		OnSessionUnregistered: onUnregistered,
		Log:                   log,
		Db:                    db,
	}
}

// ====================== Redis Key Helpers ======================

// hashKey returns the tenant-scoped Redis HASH key name.
func (l *LoginSystem) hashKey() string { return l.KeyPrefix + SessionStoreName }

// ttlKey returns the tenant-scoped Redis TTL sentinel key for a session.
func (l *LoginSystem) ttlKey(sessionKey string) string { return l.KeyPrefix + sessionKey }

// stripPrefix removes this tenant's KeyPrefix from a Redis key.
// Returns the stripped key and true if the prefix matched (or was empty).
func (l *LoginSystem) stripPrefix(redisKey string) (string, bool) {
	if l.KeyPrefix == "" {
		return redisKey, true
	}
	if !strings.HasPrefix(redisKey, l.KeyPrefix) {
		return "", false
	}
	return redisKey[len(l.KeyPrefix):], true
}

// ====================== Session Key Generation ======================

// GenerateSessionKey creates an opaque session key from UUIDv7 + UUIDv4, dashes removed.
func GenerateSessionKey() (string, error) {
	u7, err := uuid.NewV7()
	if err != nil {
		return "", fmt.Errorf("GenerateSessionKey:UUIDv7: %w", err)
	}
	u4 := uuid.New()
	key := strings.ReplaceAll(u7.String(), "-", "") + strings.ReplaceAll(u4.String(), "-", "")
	return key, nil
}

// ====================== Lifecycle ======================

// Start initializes the session management backend.
// For Redis modes: starts keyspace notification subscribers and sync ticker.
// For DBOnly mode: starts a background cleanup goroutine.
func (l *LoginSystem) Start() error {
	switch l.Storage {
	case RedisOnly, RedisWithDB:
		return l.startRedis()
	case DBOnly:
		return l.startDBOnly()
	}
	return nil
}

// Stop shuts down the session management backend.
func (l *LoginSystem) Stop() {
	switch l.Storage {
	case RedisOnly, RedisWithDB:
		l.stopRedis()
	case DBOnly:
		l.stopDBOnly()
	}
}

// ====================== Public API ======================

// InstanceRegister registers a new session. Kicks existing sessions based on DeviceInstanceType rules.
// sessionData should contain all application-specific fields (roles, pn_type, pn_token, etc.)
// System fields (session_key, usermanagement_user_id, device_id) are injected automatically.
func (l *LoginSystem) InstanceRegister(sessionKey string, userId int64, deviceId string, sessionData map[string]any) error {
	// Inject system fields into sessionData
	sessionData["session_key"] = sessionKey
	sessionData["usermanagement_user_id"] = userId
	sessionData["device_id"] = deviceId

	switch l.Storage {
	case RedisOnly:
		return l.redisOnlyInstanceRegister(sessionKey, userId, deviceId, sessionData)
	case RedisWithDB:
		return l.redisWithDBInstanceRegister(sessionKey, userId, deviceId, sessionData)
	case DBOnly:
		return l.dbOnlyInstanceRegister(sessionKey, userId, deviceId, sessionData)
	}
	return errors.New("InstanceRegister:unknown_storage_type")
}

// InstanceGet retrieves session data by session key. Returns nil if not found or expired.
// For ShortLived tokens, this renews the TTL (sliding window).
// For LongLived tokens, TTL is fixed — no renewal.
func (l *LoginSystem) InstanceGet(sessionKey string) (sessionData map[string]any, err error) {
	switch l.Storage {
	case RedisOnly:
		return l.redisOnlyInstanceGet(sessionKey)
	case RedisWithDB:
		return l.redisWithDBInstanceGet(sessionKey)
	case DBOnly:
		return l.dbOnlyInstanceGet(sessionKey)
	}
	return nil, errors.New("InstanceGet:unknown_storage_type")
}

// InstanceUnregister removes a session and fires OnSessionUnregistered callback.
func (l *LoginSystem) InstanceUnregister(sessionKey string, reason TokenRemoveReasonType, reasonData any) error {
	switch l.Storage {
	case RedisOnly:
		return l.redisOnlyInstanceUnregister(sessionKey, reason, reasonData)
	case RedisWithDB:
		return l.redisWithDBInstanceUnregister(sessionKey, reason, reasonData)
	case DBOnly:
		return l.dbOnlyInstanceUnregister(sessionKey, reason, reasonData)
	}
	return errors.New("InstanceUnregister:unknown_storage_type")
}

// InstanceUpdateSessionData merges updateFields into the existing session data.
// Existing keys not in updateFields are preserved. Keys in updateFields overwrite.
func (l *LoginSystem) InstanceUpdateSessionData(sessionKey string, updateFields map[string]any) error {
	switch l.Storage {
	case RedisOnly:
		return l.redisOnlyInstanceUpdateSessionData(sessionKey, updateFields)
	case RedisWithDB:
		return l.redisWithDBInstanceUpdateSessionData(sessionKey, updateFields)
	case DBOnly:
		return l.dbOnlyInstanceUpdateSessionData(sessionKey, updateFields)
	}
	return errors.New("InstanceUpdateSessionData:unknown_storage_type")
}

// InstanceGetDevicesByUserId returns all active sessions for a user.
func (l *LoginSystem) InstanceGetDevicesByUserId(userId int64) []map[string]any {
	switch l.Storage {
	case RedisOnly:
		return l.redisOnlyInstanceGetDevicesByUserId(userId)
	case RedisWithDB:
		return l.redisWithDBInstanceGetDevicesByUserId(userId)
	case DBOnly:
		return l.dbOnlyInstanceGetDevicesByUserId(userId)
	}
	return nil
}

// ====================== Redis Lifecycle ======================

func (l *LoginSystem) startRedis() error {
	ctx := l.RedisClient.Context()
	_, err := l.RedisClient.Do(ctx, "CONFIG", "SET", "notify-keyspace-events", "Exe").Result()
	if err != nil {
		l.Log.Error("LoginSystem.startRedis:CONFIG_SET_notify-keyspace-events:", err)
		return err
	}

	// Subscribe to expired keys
	expiredChannel := fmt.Sprintf("__keyevent@%d__:expired", l.RedisDB)
	l.PubSubKeyExpired = l.RedisClient.Subscribe(ctx, expiredChannel)
	_, err = l.PubSubKeyExpired.Receive(ctx)
	if err != nil {
		l.Log.Error(fmt.Sprintf("LoginSystem.startRedis:%s:SUBSCRIBE_ERROR:", expiredChannel), err)
		return err
	}
	go func() {
		defer func() {
			if r := recover(); r != nil {
				l.Log.Error("LoginSystem.startRedis:expired-goroutine:PANIC:", fmt.Errorf("%v\n%s", r, debug.Stack()))
			}
		}()
		ch := l.PubSubKeyExpired.Channel()
		for msg := range ch {
			sessionKey, ok := l.stripPrefix(msg.Payload)
			if !ok {
				continue // not our tenant's key
			}
			l.redisExpirationCallback(sessionKey, Expired)
		}
	}()

	// Subscribe to evicted keys
	evictedChannel := fmt.Sprintf("__keyevent@%d__:evicted", l.RedisDB)
	l.PubSubKeyEvicted = l.RedisClient.Subscribe(ctx, evictedChannel)
	_, err = l.PubSubKeyEvicted.Receive(ctx)
	if err != nil {
		l.Log.Error(fmt.Sprintf("LoginSystem.startRedis:%s:SUBSCRIBE_ERROR:", evictedChannel), err)
		_ = l.PubSubKeyExpired.Close()
		return err
	}
	go func() {
		defer func() {
			if r := recover(); r != nil {
				l.Log.Error("LoginSystem.startRedis:evicted-goroutine:PANIC:", fmt.Errorf("%v\n%s", r, debug.Stack()))
			}
		}()
		ch := l.PubSubKeyEvicted.Channel()
		for msg := range ch {
			sessionKey, ok := l.stripPrefix(msg.Payload)
			if !ok {
				continue // not our tenant's key
			}
			l.redisExpirationCallback(sessionKey, Evicted)
		}
	}()

	// Start sync ticker
	go func() {
		defer func() {
			if r := recover(); r != nil {
				l.Log.Error("LoginSystem.startRedis:sync-goroutine:PANIC:", fmt.Errorf("%v\n%s", r, debug.Stack()))
			}
		}()
		l.SyncTicker = time.NewTicker(l.SyncInterval)
		defer l.SyncTicker.Stop()
		for range l.SyncTicker.C {
			l.checkSessions()
		}
	}()

	return nil
}

func (l *LoginSystem) stopRedis() {
	if l.SyncTicker != nil {
		l.SyncTicker.Stop()
	}
	if l.PubSubKeyExpired != nil {
		_ = l.PubSubKeyExpired.Close()
	}
	if l.PubSubKeyEvicted != nil {
		_ = l.PubSubKeyEvicted.Close()
	}
	if l.RedisClient != nil {
		_ = l.RedisClient.Close()
	}
}

func (l *LoginSystem) startDBOnly() error {
	// Start background cleanup goroutine that polls expired_at
	go func() {
		defer func() {
			if r := recover(); r != nil {
				l.Log.Error("LoginSystem.startDBOnly:cleanup-goroutine:PANIC:", fmt.Errorf("%v\n%s", r, debug.Stack()))
			}
		}()
		l.SyncTicker = time.NewTicker(l.SyncInterval)
		defer l.SyncTicker.Stop()
		for range l.SyncTicker.C {
			l.dbOnlyCleanupExpired()
		}
	}()
	return nil
}

func (l *LoginSystem) stopDBOnly() {
	if l.SyncTicker != nil {
		l.SyncTicker.Stop()
	}
}

// ====================== Redis Expiration Callback ======================

// redisExpirationCallback handles Redis key expired/evicted events.
// Reads session data from the HASH (RedisOnly) or PG table (RedisWithDB), then fires callback.
func (l *LoginSystem) redisExpirationCallback(sessionKey string, reason TokenRemoveReasonType) {
	switch l.Storage {
	case RedisOnly:
		l.redisOnlyExpirationCallback(sessionKey, reason)
	case RedisWithDB:
		l.redisWithDBExpirationCallback(sessionKey, reason)
	}
}

// ====================== RedisOnly Implementation ======================

func (l *LoginSystem) redisOnlyInstanceRegister(sessionKey string, userId int64, deviceId string, sessionData map[string]any) error {
	ctx := context.Background()

	// Kick existing sessions based on DeviceInstanceType rules
	tokensToKick := l.redisOnlyFindSessionsToKick(userId, deviceId)
	for _, oldKey := range tokensToKick {
		_ = l.redisOnlyInstanceUnregister(oldKey, Replaced, nil)
	}

	// Store session data in Redis HASH
	sessionDataBytes, err := json.Marshal(sessionData)
	if err != nil {
		return fmt.Errorf("redisOnlyInstanceRegister:marshal: %w", err)
	}
	if err := l.RedisClient.HSet(ctx, l.hashKey(), sessionKey, sessionDataBytes).Err(); err != nil {
		return fmt.Errorf("redisOnlyInstanceRegister:HSET: %w", err)
	}

	// SET the TTL key (the key whose expiry triggers the pub/sub callback)
	if err := l.RedisClient.Set(ctx, l.ttlKey(sessionKey), "1", l.ExpiredTimeDuration).Err(); err != nil {
		l.Log.Error("redisOnlyInstanceRegister:SET_TTL:", err)
	}

	if l.OnSessionRegistered != nil {
		l.OnSessionRegistered(sessionKey, sessionData)
	}
	return nil
}

func (l *LoginSystem) redisOnlyInstanceGet(sessionKey string) (map[string]any, error) {
	ctx := context.Background()

	// Check TTL key exists
	var err error
	if l.TokenLifetime == ShortLived {
		// GETEX with TTL renewal (sliding window)
		_, err = l.RedisClient.GetEx(ctx, l.ttlKey(sessionKey), l.ExpiredTimeDuration).Result()
	} else {
		// GET without TTL renewal (fixed expiry)
		_, err = l.RedisClient.Get(ctx, l.ttlKey(sessionKey)).Result()
	}
	if err != nil {
		return nil, err
	}

	// Read session data from HASH
	val, err := l.RedisClient.HGet(ctx, l.hashKey(), sessionKey).Bytes()
	if err != nil {
		return nil, fmt.Errorf("redisOnlyInstanceGet:HGET: %w", err)
	}

	var sessionData map[string]any
	if err := json.Unmarshal(val, &sessionData); err != nil {
		return nil, fmt.Errorf("redisOnlyInstanceGet:unmarshal: %w", err)
	}

	// Fix JSON number → int64 for user_id
	l.fixJsonUserId(sessionData)
	return sessionData, nil
}

func (l *LoginSystem) redisOnlyInstanceUnregister(sessionKey string, reason TokenRemoveReasonType, reasonData any) error {
	ctx := context.Background()

	// Read session data before deleting
	val, err := l.RedisClient.HGet(ctx, l.hashKey(), sessionKey).Bytes()
	if err != nil && !errors.Is(err, goredis.Nil) {
		l.Log.Error("redisOnlyInstanceUnregister:HGET:", err)
	}

	// Delete TTL key and HASH entry
	_ = l.RedisClient.Del(ctx, l.ttlKey(sessionKey)).Err()
	_ = l.RedisClient.HDel(ctx, l.hashKey(), sessionKey).Err()

	// Fire callback if we have session data
	if val != nil && l.OnSessionUnregistered != nil {
		var sessionData map[string]any
		if err := json.Unmarshal(val, &sessionData); err == nil {
			l.fixJsonUserId(sessionData)
			l.OnSessionUnregistered(sessionKey, reason, reasonData, sessionData)
		}
	}
	return nil
}

func (l *LoginSystem) redisOnlyInstanceUpdateSessionData(sessionKey string, updateFields map[string]any) error {
	ctx := context.Background()

	val, err := l.RedisClient.HGet(ctx, l.hashKey(), sessionKey).Bytes()
	if err != nil {
		return fmt.Errorf("redisOnlyInstanceUpdateSessionData:HGET: %w", err)
	}

	var sessionData map[string]any
	if err := json.Unmarshal(val, &sessionData); err != nil {
		return fmt.Errorf("redisOnlyInstanceUpdateSessionData:unmarshal: %w", err)
	}

	// Merge updateFields into sessionData
	for k, v := range updateFields {
		sessionData[k] = v
	}

	updated, err := json.Marshal(sessionData)
	if err != nil {
		return fmt.Errorf("redisOnlyInstanceUpdateSessionData:marshal: %w", err)
	}

	return l.RedisClient.HSet(ctx, l.hashKey(), sessionKey, updated).Err()
}

func (l *LoginSystem) redisOnlyInstanceGetDevicesByUserId(userId int64) []map[string]any {
	ctx := context.Background()
	all, err := l.RedisClient.HGetAll(ctx, l.hashKey()).Result()
	if err != nil {
		l.Log.Error("redisOnlyInstanceGetDevicesByUserId:HGETALL:", err)
		return nil
	}

	var result []map[string]any
	for _, val := range all {
		var sessionData map[string]any
		if err := json.Unmarshal([]byte(val), &sessionData); err != nil {
			continue
		}
		l.fixJsonUserId(sessionData)
		if uid, ok := sessionData["usermanagement_user_id"].(int64); ok && uid == userId {
			result = append(result, sessionData)
		}
	}
	return result
}

func (l *LoginSystem) redisOnlyFindSessionsToKick(userId int64, deviceId string) []string {
	ctx := context.Background()
	all, err := l.RedisClient.HGetAll(ctx, l.hashKey()).Result()
	if err != nil {
		l.Log.Error("redisOnlyFindSessionsToKick:HGETALL:", err)
		return nil
	}

	var toKick []string
	for key, val := range all {
		var sessionData map[string]any
		if err := json.Unmarshal([]byte(val), &sessionData); err != nil {
			continue
		}
		l.fixJsonUserId(sessionData)
		uid, ok := sessionData["usermanagement_user_id"].(int64)
		if !ok || uid != userId {
			continue
		}
		did, _ := sessionData["device_id"].(string)
		switch l.Type {
		case SingleDeviceSingleInstancePerDevice:
			toKick = append(toKick, key)
		case SingleDeviceMultipleInstancePerDevice:
			if did != deviceId {
				toKick = append(toKick, key)
			}
		case MultipleDeviceSingleInstancePerDevice:
			if did == deviceId {
				toKick = append(toKick, key)
			}
		case MultipleDeviceMultipleInstancePerDevice:
			// no kick-out
		}
	}
	return toKick
}

func (l *LoginSystem) redisOnlyExpirationCallback(sessionKey string, reason TokenRemoveReasonType) {
	ctx := context.Background()
	val, err := l.RedisClient.HGet(ctx, l.hashKey(), sessionKey).Bytes()
	if err != nil {
		// Session data already cleaned up
		return
	}
	_ = l.RedisClient.HDel(ctx, l.hashKey(), sessionKey).Err()

	if l.OnSessionUnregistered != nil {
		var sessionData map[string]any
		if err := json.Unmarshal(val, &sessionData); err == nil {
			l.fixJsonUserId(sessionData)
			l.OnSessionUnregistered(sessionKey, reason, nil, sessionData)
		}
	}
}

// ====================== RedisWithDB Implementation ======================

func (l *LoginSystem) redisWithDBInstanceRegister(sessionKey string, userId int64, deviceId string, sessionData map[string]any) error {
	ctx := context.Background()

	sessionDataBytes, err := json.Marshal(sessionData)
	if err != nil {
		return fmt.Errorf("redisWithDBInstanceRegister:marshal: %w", err)
	}

	err = l.Db.Tx(ctx, &l.Log, databases.LevelSerializable, func(dtx *databases.DXDatabaseTx) (err error) {
		// Find and kick existing sessions based on DeviceInstanceType rules
		var oldSessions []map[string]any

		switch l.Type {
		case SingleDeviceSingleInstancePerDevice:
			_, oldSessions, err = dtx.Select(ctx, SessionStoreName, nil, nil, map[string]any{
				"appmanagement_appinstance_id": l.TenantId,
				"usermanagement_user_id":       userId,
			}, nil, nil, nil, nil, nil, nil, nil)
		case SingleDeviceMultipleInstancePerDevice:
			rows, qErr := dtx.NamedQuery(
				`SELECT * FROM `+SessionStoreName+` WHERE appmanagement_appinstance_id = :appinstance_id AND usermanagement_user_id = :user_id AND device_id != :device_id`,
				map[string]any{"appinstance_id": l.TenantId, "user_id": userId, "device_id": deviceId},
			)
			if qErr != nil {
				return qErr
			}
			defer rows.Close()
			for rows.Next() {
				row := make(map[string]any)
				if scanErr := rows.MapScan(row); scanErr != nil {
					return scanErr
				}
				oldSessions = append(oldSessions, row)
			}
		case MultipleDeviceSingleInstancePerDevice:
			_, oldSessions, err = dtx.Select(ctx, SessionStoreName, nil, nil, map[string]any{
				"appmanagement_appinstance_id": l.TenantId,
				"usermanagement_user_id":       userId,
				"device_id":                    deviceId,
			}, nil, nil, nil, nil, nil, nil, nil)
		case MultipleDeviceMultipleInstancePerDevice:
			// no kick-out
		}
		if err != nil {
			return err
		}

		// Kick old sessions
		for _, oldSession := range oldSessions {
			oldKey, ok := oldSession["session_key"].(string)
			if ok {
				_ = l.RedisClient.Del(ctx, l.ttlKey(oldKey)).Err()
				_ = l.RedisClient.HDel(ctx, l.hashKey(), oldKey).Err()
			}
			if e := l.txFireUnregister(dtx, oldSession, Replaced, nil); e != nil {
				l.Log.Error("redisWithDBInstanceRegister:txFireUnregister:", e)
			}
		}

		// Insert new session
		expiredAt := time.Now().UTC().Add(l.ExpiredTimeDuration)
		_, _, err = dtx.Insert(ctx, SessionStoreName, map[string]any{
			"appmanagement_appinstance_id": l.TenantId,
			"usermanagement_user_id":       userId,
			"device_id":                    deviceId,
			"session_key":                  sessionKey,
			"session_data":                 json.RawMessage(sessionDataBytes),
			"expired_at":                   expiredAt,
		}, nil)
		return err
	})
	if err != nil {
		return err
	}

	// SET in Redis after PG commit
	if err := l.RedisClient.HSet(ctx, l.hashKey(), sessionKey, sessionDataBytes).Err(); err != nil {
		l.Log.Error("redisWithDBInstanceRegister:HSET:", err)
	}
	if err := l.RedisClient.Set(ctx, l.ttlKey(sessionKey), "1", l.ExpiredTimeDuration).Err(); err != nil {
		l.Log.Error("redisWithDBInstanceRegister:SET_TTL:", err)
	}

	if l.OnSessionRegistered != nil {
		l.OnSessionRegistered(sessionKey, sessionData)
	}
	return nil
}

func (l *LoginSystem) redisWithDBInstanceGet(sessionKey string) (map[string]any, error) {
	ctx := context.Background()

	// Check Redis TTL key
	var err error
	if l.TokenLifetime == ShortLived {
		_, err = l.RedisClient.GetEx(ctx, l.ttlKey(sessionKey), l.ExpiredTimeDuration).Result()
	} else {
		_, err = l.RedisClient.Get(ctx, l.ttlKey(sessionKey)).Result()
	}
	if err != nil {
		return nil, err
	}

	// Read from HASH
	val, err := l.RedisClient.HGet(ctx, l.hashKey(), sessionKey).Bytes()
	if err != nil {
		return nil, fmt.Errorf("redisWithDBInstanceGet:HGET: %w", err)
	}

	var sessionData map[string]any
	if err := json.Unmarshal(val, &sessionData); err != nil {
		return nil, fmt.Errorf("redisWithDBInstanceGet:unmarshal: %w", err)
	}

	l.fixJsonUserId(sessionData)
	return sessionData, nil
}

func (l *LoginSystem) redisWithDBInstanceUnregister(sessionKey string, reason TokenRemoveReasonType, reasonData any) error {
	ctx := context.Background()

	// Delete Redis keys
	_ = l.RedisClient.Del(ctx, l.ttlKey(sessionKey)).Err()
	_ = l.RedisClient.HDel(ctx, l.hashKey(), sessionKey).Err()

	// Delete from PG and fire callback
	return l.Db.Tx(ctx, &l.Log, databases.LevelSerializable, func(dtx *databases.DXDatabaseTx) (err error) {
		_, session, err := dtx.SelectOne(ctx, SessionStoreName, nil, nil, map[string]any{
			"session_key": sessionKey,
		}, nil, nil, nil, nil, nil, nil)
		if err != nil {
			return err
		}
		if session == nil {
			return nil
		}
		return l.txFireUnregister(dtx, session, reason, reasonData)
	})
}

func (l *LoginSystem) redisWithDBInstanceUpdateSessionData(sessionKey string, updateFields map[string]any) error {
	ctx := context.Background()

	// Read current from HASH
	val, err := l.RedisClient.HGet(ctx, l.hashKey(), sessionKey).Bytes()
	if err != nil {
		return fmt.Errorf("redisWithDBInstanceUpdateSessionData:HGET: %w", err)
	}

	var sessionData map[string]any
	if err := json.Unmarshal(val, &sessionData); err != nil {
		return fmt.Errorf("redisWithDBInstanceUpdateSessionData:unmarshal: %w", err)
	}

	// Merge
	for k, v := range updateFields {
		sessionData[k] = v
	}

	updated, err := json.Marshal(sessionData)
	if err != nil {
		return fmt.Errorf("redisWithDBInstanceUpdateSessionData:marshal: %w", err)
	}

	// Update Redis HASH
	if err := l.RedisClient.HSet(ctx, l.hashKey(), sessionKey, updated).Err(); err != nil {
		l.Log.Error("redisWithDBInstanceUpdateSessionData:HSET:", err)
	}

	// Update PG
	_, _, err = l.Db.Update(ctx, SessionStoreName, map[string]any{
		"session_data": json.RawMessage(updated),
	}, map[string]any{
		"session_key": sessionKey,
	}, nil)
	if err != nil {
		l.Log.Error("redisWithDBInstanceUpdateSessionData:PG_UPDATE:", err)
	}
	return nil
}

func (l *LoginSystem) redisWithDBInstanceGetDevicesByUserId(userId int64) []map[string]any {
	_, sessions, err := l.Db.Select(context.Background(), SessionStoreName, nil, nil, map[string]any{
		"appmanagement_appinstance_id": l.TenantId,
		"usermanagement_user_id":       userId,
	}, nil, nil, nil, nil, nil, nil, nil)
	if err != nil {
		l.Log.Error("redisWithDBInstanceGetDevicesByUserId:SELECT:", err)
		return nil
	}
	// Extract session_data from each PG row
	var result []map[string]any
	for _, row := range sessions {
		sd := l.extractSessionData(row)
		if sd != nil {
			result = append(result, sd)
		}
	}
	return result
}

func (l *LoginSystem) redisWithDBExpirationCallback(sessionKey string, reason TokenRemoveReasonType) {
	// Clean up HASH entry
	_ = l.RedisClient.HDel(context.Background(), l.hashKey(), sessionKey).Err()

	// Read from PG and fire callback
	err := l.Db.Tx(context.Background(), &l.Log, databases.LevelSerializable, func(dtx *databases.DXDatabaseTx) (err error) {
		_, session, err := dtx.SelectOne(context.Background(), SessionStoreName, nil, nil, map[string]any{
			"session_key": sessionKey,
		}, nil, nil, nil, nil, nil, nil)
		if err != nil {
			return err
		}
		if session == nil {
			return nil
		}
		return l.txFireUnregister(dtx, session, reason, nil)
	})
	if err != nil {
		l.Log.Error("LoginSystem.redisWithDBExpirationCallback:TX_ERROR:", err)
	}
}

// ====================== DBOnly Implementation ======================

func (l *LoginSystem) dbOnlyInstanceRegister(sessionKey string, userId int64, deviceId string, sessionData map[string]any) error {
	ctx := context.Background()

	sessionDataBytes, err := json.Marshal(sessionData)
	if err != nil {
		return fmt.Errorf("dbOnlyInstanceRegister:marshal: %w", err)
	}

	err = l.Db.Tx(ctx, &l.Log, databases.LevelSerializable, func(dtx *databases.DXDatabaseTx) (err error) {
		// Find and kick existing sessions
		var oldSessions []map[string]any

		switch l.Type {
		case SingleDeviceSingleInstancePerDevice:
			_, oldSessions, err = dtx.Select(ctx, SessionStoreName, nil, nil, map[string]any{
				"appmanagement_appinstance_id": l.TenantId,
				"usermanagement_user_id":       userId,
			}, nil, nil, nil, nil, nil, nil, nil)
		case SingleDeviceMultipleInstancePerDevice:
			rows, qErr := dtx.NamedQuery(
				`SELECT * FROM `+SessionStoreName+` WHERE appmanagement_appinstance_id = :appinstance_id AND usermanagement_user_id = :user_id AND device_id != :device_id`,
				map[string]any{"appinstance_id": l.TenantId, "user_id": userId, "device_id": deviceId},
			)
			if qErr != nil {
				return qErr
			}
			defer rows.Close()
			for rows.Next() {
				row := make(map[string]any)
				if scanErr := rows.MapScan(row); scanErr != nil {
					return scanErr
				}
				oldSessions = append(oldSessions, row)
			}
		case MultipleDeviceSingleInstancePerDevice:
			_, oldSessions, err = dtx.Select(ctx, SessionStoreName, nil, nil, map[string]any{
				"appmanagement_appinstance_id": l.TenantId,
				"usermanagement_user_id":       userId,
				"device_id":                    deviceId,
			}, nil, nil, nil, nil, nil, nil, nil)
		case MultipleDeviceMultipleInstancePerDevice:
			// no kick-out
		}
		if err != nil {
			return err
		}

		for _, oldSession := range oldSessions {
			if e := l.txFireUnregister(dtx, oldSession, Replaced, nil); e != nil {
				l.Log.Error("dbOnlyInstanceRegister:txFireUnregister:", e)
			}
		}

		// Insert new session
		expiredAt := time.Now().UTC().Add(l.ExpiredTimeDuration)
		_, _, err = dtx.Insert(ctx, SessionStoreName, map[string]any{
			"appmanagement_appinstance_id": l.TenantId,
			"usermanagement_user_id":       userId,
			"device_id":                    deviceId,
			"session_key":                  sessionKey,
			"session_data":                 json.RawMessage(sessionDataBytes),
			"expired_at":                   expiredAt,
		}, nil)
		return err
	})
	if err != nil {
		return err
	}

	if l.OnSessionRegistered != nil {
		l.OnSessionRegistered(sessionKey, sessionData)
	}
	return nil
}

func (l *LoginSystem) dbOnlyInstanceGet(sessionKey string) (map[string]any, error) {
	ctx := context.Background()

	// SELECT with expired_at check
	_, session, err := l.Db.SelectOne(ctx, SessionStoreName, nil, nil, map[string]any{
		"session_key": sessionKey,
	}, nil, nil, nil, nil, nil, nil)
	if err != nil {
		return nil, err
	}
	if session == nil {
		return nil, errors.New("session_not_found")
	}

	// Check expiry
	expiredAt, err := l.getTimeFromRow(session, "expired_at")
	if err != nil {
		return nil, fmt.Errorf("dbOnlyInstanceGet:expired_at: %w", err)
	}
	if time.Now().UTC().After(expiredAt) {
		return nil, errors.New("session_expired")
	}

	// For ShortLived, update expired_at (sliding window)
	if l.TokenLifetime == ShortLived {
		newExpiredAt := time.Now().UTC().Add(l.ExpiredTimeDuration)
		_, _, err = l.Db.Update(ctx, SessionStoreName, map[string]any{
			"expired_at": newExpiredAt,
		}, map[string]any{
			"session_key": sessionKey,
		}, nil)
		if err != nil {
			l.Log.Error("dbOnlyInstanceGet:UPDATE_expired_at:", err)
		}
	}

	sessionData := l.extractSessionData(session)
	if sessionData == nil {
		return nil, errors.New("dbOnlyInstanceGet:invalid_session_data")
	}
	return sessionData, nil
}

func (l *LoginSystem) dbOnlyInstanceUnregister(sessionKey string, reason TokenRemoveReasonType, reasonData any) error {
	ctx := context.Background()
	return l.Db.Tx(ctx, &l.Log, databases.LevelSerializable, func(dtx *databases.DXDatabaseTx) (err error) {
		_, session, err := dtx.SelectOne(ctx, SessionStoreName, nil, nil, map[string]any{
			"session_key": sessionKey,
		}, nil, nil, nil, nil, nil, nil)
		if err != nil {
			return err
		}
		if session == nil {
			return nil
		}
		return l.txFireUnregister(dtx, session, reason, reasonData)
	})
}

func (l *LoginSystem) dbOnlyInstanceUpdateSessionData(sessionKey string, updateFields map[string]any) error {
	ctx := context.Background()

	// Read current session_data
	_, session, err := l.Db.SelectOne(ctx, SessionStoreName, nil, nil, map[string]any{
		"session_key": sessionKey,
	}, nil, nil, nil, nil, nil, nil)
	if err != nil {
		return err
	}
	if session == nil {
		return errors.New("dbOnlyInstanceUpdateSessionData:session_not_found")
	}

	sessionData := l.extractSessionData(session)
	if sessionData == nil {
		return errors.New("dbOnlyInstanceUpdateSessionData:invalid_session_data")
	}

	// Merge
	for k, v := range updateFields {
		sessionData[k] = v
	}

	updated, err := json.Marshal(sessionData)
	if err != nil {
		return fmt.Errorf("dbOnlyInstanceUpdateSessionData:marshal: %w", err)
	}

	_, _, err = l.Db.Update(ctx, SessionStoreName, map[string]any{
		"session_data": json.RawMessage(updated),
	}, map[string]any{
		"session_key": sessionKey,
	}, nil)
	return err
}

func (l *LoginSystem) dbOnlyInstanceGetDevicesByUserId(userId int64) []map[string]any {
	ctx := context.Background()
	_, sessions, err := l.Db.Select(ctx, SessionStoreName, nil, nil, map[string]any{
		"appmanagement_appinstance_id": l.TenantId,
		"usermanagement_user_id":       userId,
	}, nil, nil, nil, nil, nil, nil, nil)
	if err != nil {
		l.Log.Error("dbOnlyInstanceGetDevicesByUserId:SELECT:", err)
		return nil
	}

	var result []map[string]any
	for _, row := range sessions {
		sd := l.extractSessionData(row)
		if sd != nil {
			result = append(result, sd)
		}
	}
	return result
}

// dbOnlyCleanupExpired is the background goroutine that polls for expired sessions (DBOnly mode).
func (l *LoginSystem) dbOnlyCleanupExpired() {
	ctx := context.Background()
	_, expired, err := l.Db.Select(ctx, SessionStoreName, nil, nil, map[string]any{
		"appmanagement_appinstance_id": l.TenantId,
	}, `AND expired_at <= NOW()`, nil, nil, nil, nil, nil, nil)
	if err != nil {
		l.Log.Error("dbOnlyCleanupExpired:SELECT:", err)
		return
	}

	for _, session := range expired {
		err := l.Db.Tx(ctx, &l.Log, databases.LevelSerializable, func(dtx *databases.DXDatabaseTx) error {
			return l.txFireUnregister(dtx, session, Expired, nil)
		})
		if err != nil {
			l.Log.Error("dbOnlyCleanupExpired:txFireUnregister:", err)
		}
	}
}

// ====================== Shared Helpers ======================

// txFireUnregister deletes the PG session row and fires OnSessionUnregistered within a transaction.
func (l *LoginSystem) txFireUnregister(dtx *databases.DXDatabaseTx, session map[string]any, reason TokenRemoveReasonType, reasonData any) error {
	sessionKey, ok := session["session_key"].(string)
	if !ok {
		return errors.New("txFireUnregister:session_key_missing_or_wrong_type")
	}

	_, _, err := dtx.TxDelete(context.Background(), SessionStoreName, map[string]any{
		"session_key": sessionKey,
	}, nil)
	if err != nil {
		return err
	}

	if l.OnSessionUnregistered != nil {
		sessionData := l.extractSessionData(session)
		if sessionData != nil {
			l.OnSessionUnregistered(sessionKey, reason, reasonData, sessionData)
		}
	}
	return nil
}

// checkSessions is the periodic reconciliation task.
// For RedisOnly: scans HASH for sessions whose TTL key is missing.
// For RedisWithDB: scans PG for sessions whose Redis TTL key is missing.
func (l *LoginSystem) checkSessions() {
	switch l.Storage {
	case RedisOnly:
		l.checkSessionsRedisOnly()
	case RedisWithDB:
		l.checkSessionsRedisWithDB()
	}
}

func (l *LoginSystem) checkSessionsRedisOnly() {
	ctx := context.Background()
	all, err := l.RedisClient.HGetAll(ctx, l.hashKey()).Result()
	if err != nil {
		l.Log.Error("checkSessionsRedisOnly:HGETALL:", err)
		return
	}
	for key := range all {
		exists, err := l.RedisClient.Exists(ctx, l.ttlKey(key)).Result()
		if err != nil {
			l.Log.Error("checkSessionsRedisOnly:EXISTS:", err)
			continue
		}
		if exists == 0 {
			truncated := key
			if len(truncated) > 8 {
				truncated = truncated[:8] + "..."
			}
			l.Log.Warnf("checkSessionsRedisOnly:session_missing_ttl_key:cleaning_up: %v", truncated)
			l.redisOnlyExpirationCallback(key, Expired)
		}
	}
}

func (l *LoginSystem) checkSessionsRedisWithDB() {
	ctx := context.Background()
	_, sessions, err := l.Db.Select(ctx, SessionStoreName, nil, []string{"session_key"}, map[string]any{
		"appmanagement_appinstance_id": l.TenantId,
	}, nil, nil, nil, nil, nil, nil, nil)
	if err != nil {
		l.Log.Error("checkSessionsRedisWithDB:SELECT:", err)
		return
	}
	for _, session := range sessions {
		sessionKey, ok := session["session_key"].(string)
		if !ok {
			continue
		}
		exists, err := l.RedisClient.Exists(ctx, l.ttlKey(sessionKey)).Result()
		if err != nil {
			l.Log.Error("checkSessionsRedisWithDB:EXISTS:", err)
			continue
		}
		if exists == 0 {
			truncated := sessionKey
			if len(truncated) > 8 {
				truncated = truncated[:8] + "..."
			}
			l.Log.Warnf("checkSessionsRedisWithDB:session_missing_in_redis:cleaning_up: %v", truncated)
			l.redisWithDBExpirationCallback(sessionKey, Expired)
		}
	}
}

// extractSessionData unmarshals the session_data JSON column from a PG row into map[string]any.
func (l *LoginSystem) extractSessionData(row map[string]any) map[string]any {
	var sessionData map[string]any
	switch v := row["session_data"].(type) {
	case []byte:
		if err := json.Unmarshal(v, &sessionData); err != nil {
			l.Log.Error("extractSessionData:unmarshal_bytes:", err)
			return nil
		}
	case string:
		if err := json.Unmarshal([]byte(v), &sessionData); err != nil {
			l.Log.Error("extractSessionData:unmarshal_string:", err)
			return nil
		}
	case map[string]any:
		sessionData = v
	default:
		l.Log.Error("extractSessionData:unexpected_type", fmt.Errorf("%T", v))
		return nil
	}
	l.fixJsonUserId(sessionData)
	return sessionData
}

// fixJsonUserId converts float64 user_id back to int64 (JSON numbers unmarshal as float64).
func (l *LoginSystem) fixJsonUserId(sessionData map[string]any) {
	if f, ok := sessionData["usermanagement_user_id"].(float64); ok {
		sessionData["usermanagement_user_id"] = int64(f)
	}
	if f, ok := sessionData["user_id"].(float64); ok {
		sessionData["user_id"] = int64(f)
	}
}

// getTimeFromRow extracts a time.Time from a PG row field.
func (l *LoginSystem) getTimeFromRow(row map[string]any, field string) (time.Time, error) {
	switch v := row[field].(type) {
	case time.Time:
		return v, nil
	case string:
		t, err := time.Parse(time.RFC3339Nano, v)
		if err != nil {
			t, err = time.Parse(time.RFC3339, v)
		}
		return t, err
	default:
		return time.Time{}, fmt.Errorf("getTimeFromRow:%s:unexpected_type:%T", field, v)
	}
}
