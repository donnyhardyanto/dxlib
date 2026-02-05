package secure_memory

import (
	"sync"

	"github.com/awnumar/memguard"
	"github.com/donnyhardyanto/dxlib/errors"
	"github.com/donnyhardyanto/dxlib/log"
	"github.com/donnyhardyanto/dxlib/vault"
)

// DXSecureMemoryType defines the storage type
type DXSecureMemoryType int

const (
	// DXSecureMemoryTypeLockedBuffer - data in plaintext, RAM locked (no swap)
	// Use for frequently accessed keys (e.g., encryption keys used often)
	DXSecureMemoryTypeLockedBuffer DXSecureMemoryType = iota

	// DXSecureMemoryTypeEnclave - data encrypted in memory
	// Use for sensitive data with less frequent access
	DXSecureMemoryTypeEnclave
)

// DXSecureMemory holds a single secure memory entry
type DXSecureMemory struct {
	Owner        *DXSecureMemoryManager
	Key          string
	StorageType  DXSecureMemoryType
	LockedBuffer *memguard.LockedBuffer
	Enclave      *memguard.Enclave
}

// Get returns a copy of the stored data
func (sm *DXSecureMemory) Get() ([]byte, error) {
	switch sm.StorageType {
	case DXSecureMemoryTypeLockedBuffer:
		if sm.LockedBuffer == nil {
			return nil, errors.Errorf("SECURE_MEMORY_LOCKED_BUFFER_IS_NIL:%s", sm.Key)
		}
		// Return a copy of the data
		data := sm.LockedBuffer.Bytes()
		result := make([]byte, len(data))
		copy(result, data)
		return result, nil

	case DXSecureMemoryTypeEnclave:
		if sm.Enclave == nil {
			return nil, errors.Errorf("SECURE_MEMORY_ENCLAVE_IS_NIL:%s", sm.Key)
		}
		// Open enclave to get temporary LockedBuffer
		lockedBuffer, err := sm.Enclave.Open()
		if err != nil {
			return nil, errors.Wrapf(err, "SECURE_MEMORY_ENCLAVE_OPEN_ERROR:%s", sm.Key)
		}
		// Copy the data
		data := lockedBuffer.Bytes()
		result := make([]byte, len(data))
		copy(result, data)
		// Destroy temporary LockedBuffer (data goes back to encrypted in Enclave)
		lockedBuffer.Destroy()
		return result, nil

	default:
		return nil, errors.Errorf("SECURE_MEMORY_UNKNOWN_STORAGE_TYPE:%s", sm.Key)
	}
}

// Destroy securely wipes and releases the memory
func (sm *DXSecureMemory) Destroy() {
	if sm.LockedBuffer != nil {
		sm.LockedBuffer.Destroy()
		sm.LockedBuffer = nil
	}
	if sm.Enclave != nil {
		sm.Enclave = nil // Enclave doesn't have Destroy, it's garbage collected
	}
}

// DXSecureMemoryManager manages secure memory entries
type DXSecureMemoryManager struct {
	Memories map[string]*DXSecureMemory
	mutex    sync.RWMutex
}

// Store stores data using LockedBuffer (for frequently accessed keys)
func (m *DXSecureMemoryManager) Store(key string, data []byte) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	// If key exists, destroy old data first
	if existing, ok := m.Memories[key]; ok {
		existing.Destroy()
	}

	// Create new LockedBuffer from data
	lockedBuffer := memguard.NewBufferFromBytes(data)

	sm := &DXSecureMemory{
		Owner:        m,
		Key:          key,
		StorageType:  DXSecureMemoryTypeLockedBuffer,
		LockedBuffer: lockedBuffer,
		Enclave:      nil,
	}

	m.Memories[key] = sm
	log.Log.Tracef("Secure memory stored (LockedBuffer): %s", key)
	return nil
}

// StoreEnclave stores data using Enclave (encrypted in memory, for sensitive data)
func (m *DXSecureMemoryManager) StoreEnclave(key string, data []byte) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	// If key exists, destroy old data first
	if existing, ok := m.Memories[key]; ok {
		existing.Destroy()
	}

	// Create Enclave from data (data is encrypted in memory)
	enclave := memguard.NewEnclave(data)

	sm := &DXSecureMemory{
		Owner:        m,
		Key:          key,
		StorageType:  DXSecureMemoryTypeEnclave,
		LockedBuffer: nil,
		Enclave:      enclave,
	}

	m.Memories[key] = sm
	log.Log.Tracef("Secure memory stored (Enclave): %s", key)
	return nil
}

// StoreFromVault gets value from vault and stores using LockedBuffer
func (m *DXSecureMemoryManager) StoreFromVault(v vault.DXVaultInterface, vaultKey string, secureMemoryKey string) error {
	// Get value from vault as string
	value, err := v.ResolveAsString(vaultKey)
	if err != nil {
		return errors.Wrapf(err, "SECURE_MEMORY_VAULT_GET_ERROR:%s", vaultKey)
	}
	if value == "" {
		return errors.Errorf("SECURE_MEMORY_VAULT_KEY_EMPTY_OR_NOT_FOUND:%s", vaultKey)
	}

	// Convert string to bytes and store
	data := []byte(value)
	err = m.Store(secureMemoryKey, data)
	if err != nil {
		return errors.Wrapf(err, "SECURE_MEMORY_STORE_FROM_VAULT_ERROR:%s->%s", vaultKey, secureMemoryKey)
	}

	log.Log.Tracef("Secure memory stored from vault (LockedBuffer): %s -> %s", vaultKey, secureMemoryKey)
	return nil
}

// StoreEnclaveFromVault gets value from vault and stores using Enclave
func (m *DXSecureMemoryManager) StoreEnclaveFromVault(v vault.DXVaultInterface, vaultKey string, secureMemoryKey string) error {
	// Get value from vault as string
	value, err := v.ResolveAsString(vaultKey)
	if err != nil {
		return errors.Wrapf(err, "SECURE_MEMORY_VAULT_GET_ERROR:%s", vaultKey)
	}
	if value == "" {
		return errors.Errorf("SECURE_MEMORY_VAULT_KEY_EMPTY_OR_NOT_FOUND:%s", vaultKey)
	}

	// Convert string to bytes and store as Enclave
	data := []byte(value)
	err = m.StoreEnclave(secureMemoryKey, data)
	if err != nil {
		return errors.Wrapf(err, "SECURE_MEMORY_STORE_ENCLAVE_FROM_VAULT_ERROR:%s->%s", vaultKey, secureMemoryKey)
	}

	log.Log.Tracef("Secure memory stored from vault (Enclave): %s -> %s", vaultKey, secureMemoryKey)
	return nil
}

// Get returns a copy of stored data by key
func (m *DXSecureMemoryManager) Get(key string) ([]byte, error) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	sm, ok := m.Memories[key]
	if !ok {
		return nil, errors.Errorf("SECURE_MEMORY_KEY_NOT_FOUND:%s", key)
	}

	return sm.Get()
}

// MustGet returns data or panics if not found
func (m *DXSecureMemoryManager) MustGet(key string) []byte {
	data, err := m.Get(key)
	if err != nil {
		log.Log.Panic("SECURE_MEMORY_MUST_GET_ERROR", err)
	}
	return data
}

// Exists checks if a key exists
func (m *DXSecureMemoryManager) Exists(key string) bool {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	_, ok := m.Memories[key]
	return ok
}

// Delete removes and destroys a secure memory entry
func (m *DXSecureMemoryManager) Delete(key string) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if sm, ok := m.Memories[key]; ok {
		sm.Destroy()
		delete(m.Memories, key)
		log.Log.Tracef("Secure memory deleted: %s", key)
	}
}

// DestroyAll destroys all secure memory entries
func (m *DXSecureMemoryManager) DestroyAll() {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	for key, sm := range m.Memories {
		sm.Destroy()
		delete(m.Memories, key)
	}
	log.Log.Trace("All secure memory destroyed")
}

// Count returns number of stored entries
func (m *DXSecureMemoryManager) Count() int {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	return len(m.Memories)
}

// Keys returns all stored keys
func (m *DXSecureMemoryManager) Keys() []string {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	keys := make([]string, 0, len(m.Memories))
	for key := range m.Memories {
		keys = append(keys, key)
	}
	return keys
}

// Manager is the global secure memory manager instance
var Manager DXSecureMemoryManager

func init() {
	Manager = DXSecureMemoryManager{
		Memories: make(map[string]*DXSecureMemory),
	}
}
