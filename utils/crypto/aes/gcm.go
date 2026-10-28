package aes

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"io"

	"github.com/donnyhardyanto/dxlib/errors"
)

// The authenticated format written by EncryptAESGCM:
//
//	FormatAESGCM (1 byte) || nonce (12 bytes) || ciphertext || tag (16 bytes)
//
// The first byte tells this format apart from what EncryptAES writes, so a
// reader can accept both while callers move over. AES-256-GCM is used, so
// the key is always 32 bytes.
const (
	FormatAESGCM    byte = 1
	AESGCMKeySize        = 32
	AESGCMNonceSize      = 12
	AESGCMTagSize        = 16
	// AESGCMOverhead is how many bytes EncryptAESGCM adds to the plaintext.
	AESGCMOverhead = 1 + AESGCMNonceSize + AESGCMTagSize
)

// EncryptAESGCM encrypts plaintext with AES-256-GCM under key (32 bytes) and
// returns FormatAESGCM || nonce || ciphertext || tag. The nonce is 96 random
// bits from crypto/rand, so one key should encrypt no more than about 2^32
// messages. additionalData, which may be nil, is authenticated but not
// encrypted: DecryptAESGCM must be given the same bytes.
func EncryptAESGCM(key, plaintext, additionalData []byte) ([]byte, error) {
	nonce := make([]byte, AESGCMNonceSize)
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, errors.Wrap(err, "AES_GCM_NONCE: crypto/rand failed")
	}
	return encryptAESGCMWithNonce(key, nonce, plaintext, additionalData)
}

// encryptAESGCMWithNonce is EncryptAESGCM with the nonce given, for the
// known-answer tests. A nonce is never reused with the same key, which is
// why it is not exported.
func encryptAESGCMWithNonce(key, nonce, plaintext, additionalData []byte) ([]byte, error) {
	aead, err := newAESGCM(key)
	if err != nil {
		return nil, err
	}
	if len(nonce) != AESGCMNonceSize {
		return nil, errors.Errorf("AES_GCM_INVALID_NONCE_SIZE: the nonce is %d bytes, not %d", len(nonce), AESGCMNonceSize)
	}
	out := make([]byte, 1+AESGCMNonceSize, AESGCMOverhead+len(plaintext))
	out[0] = FormatAESGCM
	copy(out[1:], nonce)
	return aead.Seal(out, nonce, plaintext, additionalData), nil
}

// DecryptAESGCM reads what EncryptAESGCM wrote with the same key and
// additionalData and returns the plaintext. Any change to the input, the key
// or the associated data is refused with an error; nothing is returned from
// a ciphertext that does not authenticate, and no input length panics.
func DecryptAESGCM(key, data, additionalData []byte) ([]byte, error) {
	aead, err := newAESGCM(key)
	if err != nil {
		return nil, err
	}
	if len(data) < AESGCMOverhead {
		return nil, errors.Errorf("AES_GCM_INVALID_FORMAT: %d bytes is shorter than the %d-byte minimum (format byte, nonce and tag)", len(data), AESGCMOverhead)
	}
	if data[0] != FormatAESGCM {
		return nil, errors.Errorf("AES_GCM_INVALID_FORMAT: the first byte is %d, not the format byte %d", data[0], FormatAESGCM)
	}
	nonce := data[1 : 1+AESGCMNonceSize]
	plaintext, err := aead.Open(nil, nonce, data[1+AESGCMNonceSize:], additionalData)
	if err != nil {
		return nil, errors.New("AES_GCM_AUTHENTICATION_FAILED: the ciphertext, the key or the associated data does not match what was encrypted")
	}
	return plaintext, nil
}

// IsAESGCMFormat reports whether data has the shape EncryptAESGCM writes:
// the format byte first and at least the overhead's length. It is a cheap
// check for a reader that accepts both formats, not a proof: only
// DecryptAESGCM's authentication is.
func IsAESGCMFormat(data []byte) bool {
	return len(data) >= AESGCMOverhead && data[0] == FormatAESGCM
}

func newAESGCM(key []byte) (cipher.AEAD, error) {
	if len(key) != AESGCMKeySize {
		return nil, errors.Errorf("AES_GCM_INVALID_KEY_SIZE: AES-256-GCM takes a %d-byte key, got %d bytes", AESGCMKeySize, len(key))
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, errors.Wrap(err, "AES_GCM_CIPHER")
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, errors.Wrap(err, "AES_GCM_MODE")
	}
	return aead, nil
}
