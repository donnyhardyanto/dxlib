package tables

import (
	"github.com/donnyhardyanto/dxlib/database"
	"github.com/donnyhardyanto/dxlib/errors"
	"github.com/donnyhardyanto/dxlib/utils"
)

// DXRawTable Auto Encryption Methods
// Uses table's EncryptionKeyDefs and EncryptionColumnDefs

// HasEncryptionConfig returns true if any encryption configuration is defined on this table
func (t *DXRawTable) HasEncryptionConfig() bool {
	return len(t.EncryptionKeyDefs) > 0 || len(t.EncryptionColumnDefs) > 0
}

// TxSetAllEncryptionSessionKeys sets all session keys from EncryptionKeyDefs and EncryptionColumnDefs.
// Deduplicates by sessionKey. Call this within a transaction before any operation
// that needs encryption/decryption session keys.
func (t *DXRawTable) TxSetAllEncryptionSessionKeys(dtx *database.DXDatabaseTx) error {
	sessionKeys := make(map[string]string) // sessionKey -> secureMemoryKey

	for _, def := range t.EncryptionKeyDefs {
		if def.SecureMemoryKey != "" && def.SessionKey != "" {
			sessionKeys[def.SessionKey] = def.SecureMemoryKey
		}
	}
	for _, def := range t.EncryptionColumnDefs {
		if def.EncryptionKeyDef != nil && def.EncryptionKeyDef.SecureMemoryKey != "" && def.EncryptionKeyDef.SessionKey != "" {
			sessionKeys[def.EncryptionKeyDef.SessionKey] = def.EncryptionKeyDef.SecureMemoryKey
		}
		if def.HashSaltMemoryKey != "" && def.HashSaltSessionKey != "" {
			sessionKeys[def.HashSaltSessionKey] = def.HashSaltMemoryKey
		}
	}

	for sessionKey, memoryKey := range sessionKeys {
		if err := dtx.TxSetSessionKeyFromSecureMemory(memoryKey, sessionKey); err != nil {
			return errors.Wrapf(err, "SET_ENCRYPTION_SESSION_KEY_ERROR:%s", sessionKey)
		}
	}

	return nil
}

// TxSetDecryptionSessionKeys sets the PostgreSQL session keys needed for decryption within a transaction.
// Collects keys from both EncryptionKeyDefs and EncryptionColumnDefs.
// Call this before executing raw queries on views that use pgp_sym_decrypt.
func (t *DXRawTable) TxSetDecryptionSessionKeys(dtx *database.DXDatabaseTx) error {
	if len(t.EncryptionKeyDefs) == 0 && len(t.EncryptionColumnDefs) == 0 {
		return nil
	}

	// Collect unique session keys from both EncryptionKeyDefs and EncryptionColumnDefs
	sessionKeys := make(map[string]string)
	for _, def := range t.EncryptionKeyDefs {
		if def.SecureMemoryKey != "" && def.SessionKey != "" {
			sessionKeys[def.SessionKey] = def.SecureMemoryKey
		}
	}
	for _, def := range t.EncryptionColumnDefs {
		if def.EncryptionKeyDef != nil && def.EncryptionKeyDef.SecureMemoryKey != "" && def.EncryptionKeyDef.SessionKey != "" {
			sessionKeys[def.EncryptionKeyDef.SessionKey] = def.EncryptionKeyDef.SecureMemoryKey
		}
	}

	for sessionKey, memoryKey := range sessionKeys {
		if err := dtx.TxSetSessionKeyFromSecureMemory(memoryKey, sessionKey); err != nil {
			return errors.Wrapf(err, "SET_DECRYPTION_SESSION_KEY_ERROR:%s", sessionKey)
		}
	}

	return nil
}

// convertEncryptionColumnDefsForSelect converts EncryptionColumnDef to EncryptionColumn for SELECT operations
func (t *DXRawTable) convertEncryptionColumnDefsForSelect() []EncryptionColumn {
	if len(t.EncryptionColumnDefs) == 0 {
		return nil
	}
	result := make([]EncryptionColumn, len(t.EncryptionColumnDefs))
	for i, def := range t.EncryptionColumnDefs {
		result[i] = EncryptionColumn{
			FieldName:          def.FieldName,
			DataFieldName:      def.DataFieldName,
			AliasName:          def.AliasName,
			EncryptionKeyDef:   def.EncryptionKeyDef,
			HashFieldName:      def.HashFieldName,
			HashSaltMemoryKey:  def.HashSaltMemoryKey,
			HashSaltSessionKey: def.HashSaltSessionKey,
			ViewHasDecrypt:     def.ViewHasDecrypt,
		}
	}
	return result
}

// convertEncryptionColumnDefsForWrite converts EncryptionColumnDef to EncryptionColumn for INSERT/UPDATE operations
// Extracts values from data map and removes them so they are not double-inserted
func (t *DXRawTable) convertEncryptionColumnDefsForWrite(data utils.JSON) []EncryptionColumn {
	if len(t.EncryptionColumnDefs) == 0 {
		return nil
	}
	var result []EncryptionColumn
	for _, def := range t.EncryptionColumnDefs {
		// Get value from data using DataFieldName
		if value, exists := data[def.DataFieldName]; exists {
			result = append(result, EncryptionColumn{
				FieldName:          def.FieldName,
				DataFieldName:      def.DataFieldName,
				AliasName:          def.AliasName,
				Value:              value,
				EncryptionKeyDef:   def.EncryptionKeyDef,
				HashFieldName:      def.HashFieldName,
				HashSaltMemoryKey:  def.HashSaltMemoryKey,
				HashSaltSessionKey: def.HashSaltSessionKey,
				ViewHasDecrypt:     def.ViewHasDecrypt,
			})
			// Remove the data field so it's not inserted twice
			delete(data, def.DataFieldName)
		}
	}
	return result
}
