package login_system

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/donnyhardyanto/dxlib/databases"
	dxlibLog "github.com/donnyhardyanto/dxlib/log"
)

// LoginSystemManager manages per-tenant LoginSystem instances.
// Each tenant (app instance) gets its own isolated LoginSystem with
// separate Redis key namespace, PG query scope, and TTL.
type LoginSystemManager struct {
	mu              sync.RWMutex
	Instances       map[int64]*LoginSystem // tenantId → LoginSystem
	SessionToTenant sync.Map               // sessionKey → int64 (O(1) routing)
	Log             dxlibLog.DXLog

	// Template config — used by GetOrCreate to build new instances
	DefaultDeviceType    DeviceInstanceType
	DefaultStorage       StorageType
	DefaultTokenLifetime TokenLifetimeType
	DefaultExpiredTime   time.Duration
	DefaultSyncInterval  time.Duration
	RedisAddress         string
	RedisUsername        string
	RedisPassword        string
	RedisDB              int
	Db                   *databases.DXDatabase // nil for RedisOnly

	OnSessionRegistered   OnSessionRegisteredFunc
	OnSessionUnregistered OnSessionUnregisteredFunc
}

// ====================== Constructors ======================

// NewLoginSystemManager creates a manager for Redis-only backed LoginSystem instances.
func NewLoginSystemManager(deviceType DeviceInstanceType, tokenLifetime TokenLifetimeType,
	expiredTime, syncInterval time.Duration,
	redisAddr, redisUser, redisPass string, redisDB int,
	onReg OnSessionRegisteredFunc, onUnreg OnSessionUnregisteredFunc,
	log dxlibLog.DXLog) *LoginSystemManager {

	return &LoginSystemManager{
		Instances:             make(map[int64]*LoginSystem),
		Log:                   log,
		DefaultDeviceType:     deviceType,
		DefaultStorage:        RedisOnly,
		DefaultTokenLifetime:  tokenLifetime,
		DefaultExpiredTime:    expiredTime,
		DefaultSyncInterval:   syncInterval,
		RedisAddress:          redisAddr,
		RedisUsername:         redisUser,
		RedisPassword:         redisPass,
		RedisDB:               redisDB,
		OnSessionRegistered:   onReg,
		OnSessionUnregistered: onUnreg,
	}
}

// NewLoginSystemManagerWithDB creates a manager for Redis+DB backed LoginSystem instances.
func NewLoginSystemManagerWithDB(deviceType DeviceInstanceType, tokenLifetime TokenLifetimeType,
	expiredTime, syncInterval time.Duration,
	redisAddr, redisUser, redisPass string, redisDB int,
	db *databases.DXDatabase,
	onReg OnSessionRegisteredFunc, onUnreg OnSessionUnregisteredFunc,
	log dxlibLog.DXLog) *LoginSystemManager {

	return &LoginSystemManager{
		Instances:             make(map[int64]*LoginSystem),
		Log:                   log,
		DefaultDeviceType:     deviceType,
		DefaultStorage:        RedisWithDB,
		DefaultTokenLifetime:  tokenLifetime,
		DefaultExpiredTime:    expiredTime,
		DefaultSyncInterval:   syncInterval,
		RedisAddress:          redisAddr,
		RedisUsername:         redisUser,
		RedisPassword:         redisPass,
		RedisDB:               redisDB,
		Db:                    db,
		OnSessionRegistered:   onReg,
		OnSessionUnregistered: onUnreg,
	}
}

// NewLoginSystemManagerDBOnly creates a manager for DB-only backed LoginSystem instances.
func NewLoginSystemManagerDBOnly(deviceType DeviceInstanceType, tokenLifetime TokenLifetimeType,
	expiredTime, syncInterval time.Duration,
	db *databases.DXDatabase,
	onReg OnSessionRegisteredFunc, onUnreg OnSessionUnregisteredFunc,
	log dxlibLog.DXLog) *LoginSystemManager {

	return &LoginSystemManager{
		Instances:             make(map[int64]*LoginSystem),
		Log:                   log,
		DefaultDeviceType:     deviceType,
		DefaultStorage:        DBOnly,
		DefaultTokenLifetime:  tokenLifetime,
		DefaultExpiredTime:    expiredTime,
		DefaultSyncInterval:   syncInterval,
		Db:                    db,
		OnSessionRegistered:   onReg,
		OnSessionUnregistered: onUnreg,
	}
}

// ====================== Instance Management ======================

// GetInstance returns the LoginSystem for a tenant, or nil if not yet created.
func (m *LoginSystemManager) GetInstance(tenantId int64) *LoginSystem {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.Instances[tenantId]
}

// GetOrCreate returns the LoginSystem for a tenant, creating it on first access.
// Uses double-check locking for thread safety.
// expiredTimeDuration overrides the default TTL for this tenant.
func (m *LoginSystemManager) GetOrCreate(tenantId int64, expiredTimeDuration time.Duration) *LoginSystem {
	// Fast path: read lock
	m.mu.RLock()
	ls, ok := m.Instances[tenantId]
	m.mu.RUnlock()
	if ok {
		return ls
	}

	// Slow path: write lock + double-check
	m.mu.Lock()
	defer m.mu.Unlock()
	if ls, ok = m.Instances[tenantId]; ok {
		return ls
	}

	// Create new LoginSystem from template config
	switch m.DefaultStorage {
	case RedisOnly:
		ls = NewLoginSystem(tenantId, m.DefaultDeviceType, m.DefaultTokenLifetime,
			expiredTimeDuration, m.DefaultSyncInterval,
			m.RedisAddress, m.RedisUsername, m.RedisPassword, m.RedisDB,
			m.OnSessionRegistered, m.OnSessionUnregistered, m.Log)
	case RedisWithDB:
		ls = NewLoginSystemWithDB(tenantId, m.DefaultDeviceType, m.DefaultTokenLifetime,
			expiredTimeDuration, m.DefaultSyncInterval,
			m.RedisAddress, m.RedisUsername, m.RedisPassword, m.RedisDB,
			m.Db,
			m.OnSessionRegistered, m.OnSessionUnregistered, m.Log)
	case DBOnly:
		ls = NewLoginSystemDBOnly(tenantId, m.DefaultDeviceType, m.DefaultTokenLifetime,
			expiredTimeDuration, m.DefaultSyncInterval,
			m.Db,
			m.OnSessionRegistered, m.OnSessionUnregistered, m.Log)
	}

	if err := ls.Start(); err != nil {
		m.Log.Error("LoginSystemManager.GetOrCreate:Start:", err)
	}

	m.Instances[tenantId] = ls
	return ls
}

// ====================== Session Operations ======================

// InstanceRegister registers a new session for a specific tenant.
func (m *LoginSystemManager) InstanceRegister(tenantId int64, sessionKey string, userId int64,
	deviceId string, sessionData map[string]any, expiredTimeDuration time.Duration) error {

	ls := m.GetOrCreate(tenantId, expiredTimeDuration)
	err := ls.InstanceRegister(sessionKey, userId, deviceId, sessionData)
	if err == nil {
		m.SessionToTenant.Store(sessionKey, tenantId)
	}
	return err
}

// InstanceGet retrieves session data by session key.
// Routes via SessionToTenant map for O(1) lookup; falls back to scanning all instances.
func (m *LoginSystemManager) InstanceGet(sessionKey string) (map[string]any, error) {
	// Fast path: use SessionToTenant map
	if tenantIdVal, ok := m.SessionToTenant.Load(sessionKey); ok {
		tenantId := tenantIdVal.(int64)
		ls := m.GetInstance(tenantId)
		if ls != nil {
			return ls.InstanceGet(sessionKey)
		}
	}

	// Fallback: scan all instances
	m.mu.RLock()
	instances := make([]*LoginSystem, 0, len(m.Instances))
	for _, ls := range m.Instances {
		instances = append(instances, ls)
	}
	m.mu.RUnlock()

	for _, ls := range instances {
		data, err := ls.InstanceGet(sessionKey)
		if err == nil && data != nil {
			m.SessionToTenant.Store(sessionKey, ls.TenantId)
			return data, nil
		}
	}

	return nil, nil
}

// InstanceUnregister removes a session.
// Routes via SessionToTenant map; falls back to scanning all instances.
func (m *LoginSystemManager) InstanceUnregister(sessionKey string, reason TokenRemoveReasonType, reasonData any) error {
	defer m.SessionToTenant.Delete(sessionKey)

	// Fast path
	if tenantIdVal, ok := m.SessionToTenant.Load(sessionKey); ok {
		tenantId := tenantIdVal.(int64)
		ls := m.GetInstance(tenantId)
		if ls != nil {
			return ls.InstanceUnregister(sessionKey, reason, reasonData)
		}
	}

	// Fallback: scan all instances
	m.mu.RLock()
	instances := make([]*LoginSystem, 0, len(m.Instances))
	for _, ls := range m.Instances {
		instances = append(instances, ls)
	}
	m.mu.RUnlock()

	for _, ls := range instances {
		err := ls.InstanceUnregister(sessionKey, reason, reasonData)
		if err == nil {
			return nil
		}
	}

	return nil
}

// InstanceUpdateSessionData merges updateFields into an existing session.
// Routes via SessionToTenant map; falls back to scanning all instances.
func (m *LoginSystemManager) InstanceUpdateSessionData(sessionKey string, updateFields map[string]any) error {
	// Fast path
	if tenantIdVal, ok := m.SessionToTenant.Load(sessionKey); ok {
		tenantId := tenantIdVal.(int64)
		ls := m.GetInstance(tenantId)
		if ls != nil {
			return ls.InstanceUpdateSessionData(sessionKey, updateFields)
		}
	}

	// Fallback: scan all instances
	m.mu.RLock()
	instances := make([]*LoginSystem, 0, len(m.Instances))
	for _, ls := range m.Instances {
		instances = append(instances, ls)
	}
	m.mu.RUnlock()

	for _, ls := range instances {
		err := ls.InstanceUpdateSessionData(sessionKey, updateFields)
		if err == nil {
			m.SessionToTenant.Store(sessionKey, ls.TenantId)
			return nil
		}
	}

	return nil
}

// InstanceGetDevicesByUserId returns all active sessions for a user within a specific tenant.
func (m *LoginSystemManager) InstanceGetDevicesByUserId(tenantId int64, userId int64) []map[string]any {
	ls := m.GetInstance(tenantId)
	if ls == nil {
		return nil
	}
	return ls.InstanceGetDevicesByUserId(userId)
}

// ====================== Raw Redis Operations ======================

// InstanceRedisGetRaw performs a raw Redis GET, routed to the tenant's LoginSystem.
func (m *LoginSystemManager) InstanceRedisGetRaw(tenantId int64, ctx context.Context, key string) (string, error) {
	ls := m.GetInstance(tenantId)
	if ls == nil {
		return "", fmt.Errorf("no_login_system_for_tenant_%d", tenantId)
	}
	return ls.RedisGetRaw(ctx, key)
}

// InstanceRedisSetRaw performs a raw Redis SET, routed to the tenant's LoginSystem.
func (m *LoginSystemManager) InstanceRedisSetRaw(tenantId int64, ctx context.Context, key string, value any, expiration time.Duration) error {
	ls := m.GetInstance(tenantId)
	if ls == nil {
		return fmt.Errorf("no_login_system_for_tenant_%d", tenantId)
	}
	return ls.RedisSetRaw(ctx, key, value, expiration)
}

// InstanceRedisIncr performs an atomic Redis INCR, routed to the tenant's LoginSystem.
func (m *LoginSystemManager) InstanceRedisIncr(tenantId int64, ctx context.Context, key string) (int64, error) {
	ls := m.GetInstance(tenantId)
	if ls == nil {
		return 0, fmt.Errorf("no_login_system_for_tenant_%d", tenantId)
	}
	return ls.RedisIncr(ctx, key)
}

// ====================== Lifecycle ======================

// StopAll stops all managed LoginSystem instances.
func (m *LoginSystemManager) StopAll() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, ls := range m.Instances {
		ls.Stop()
	}
}
