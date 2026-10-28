package aes

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"github.com/donnyhardyanto/dxlib/errors"
	"io"
)

// Pad appends PKCS#7 padding up to a multiple of blocksize.
//
// Deprecated: EncryptAESGCM needs no padding; Pad stays for callers of
// EncryptAES.
func Pad(data []byte, blocksize int) []byte {
	padding := blocksize - len(data)%blocksize
	if padding == 0 {
		padding = blocksize
	}
	padtext := bytes.Repeat([]byte{byte(padding)}, padding)
	return append(data, padtext...)
}

// RemovePad strips the PKCS#7 padding Pad added. data must not be empty.
//
// Deprecated: DecryptAESGCM returns the plaintext without padding; RemovePad
// stays for callers of DecryptAES.
func RemovePad(data []byte) ([]byte, error) {
	length := len(data)
	unpadding := int(data[length-1])
	if unpadding > length || unpadding > aes.BlockSize {
		return nil, errors.New("INVALID_PADDING")
	}
	return data[:(length - unpadding)], nil
}

// EncryptAES encrypts data with AES-CBC under key (16, 24 or 32 bytes) and
// returns a random 16-byte IV followed by the padded ciphertext.
//
// Deprecated: use EncryptAESGCM, which authenticates what it encrypts.
// EncryptAES stays as it is, output format included, for the peers and the
// stored data that use it; nothing is removed before a major version.
func EncryptAES(key, data []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	data = Pad(data, aes.BlockSize)

	ciphertext := make([]byte, aes.BlockSize+len(data))
	iv := ciphertext[:aes.BlockSize]
	if _, err := io.ReadFull(rand.Reader, iv); err != nil {
		return nil, err
	}

	mode := cipher.NewCBCEncrypter(block, iv)
	mode.CryptBlocks(ciphertext[aes.BlockSize:], data)

	return ciphertext, nil
}

// DecryptAES reads what EncryptAES wrote and returns the plaintext with its
// padding still on (RemovePad strips it). data must be at least 16 bytes and
// a multiple of 16; a reader of untrusted input checks that first.
//
// Deprecated: use DecryptAESGCM, which refuses any input that was not
// written with the same key. DecryptAES stays as it is for the data already
// encrypted with EncryptAES.
func DecryptAES(key, data []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}

	iv := data[:aes.BlockSize]
	cipherText := data[aes.BlockSize:]
	plainText := make([]byte, len(cipherText))

	mode := cipher.NewCBCDecrypter(block, iv)
	mode.CryptBlocks(plainText, cipherText)

	return plainText, nil
}
