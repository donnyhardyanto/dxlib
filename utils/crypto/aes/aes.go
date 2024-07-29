package aes

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"io"
)

func Pad(data []byte, blocksize int) []byte {
	padding := blocksize - len(data)%blocksize
	if padding == 0 {
		padding = blocksize
	}
	padtext := bytes.Repeat([]byte{byte(padding)}, padding)
	return append(data, padtext...)
}

// RemovePad removes padding from data
func RemovePad(data []byte) ([]byte, error) {
	length := len(data)
	unpadding := int(data[length-1])
	if unpadding > length || unpadding > aes.BlockSize {
		return nil, errors.New("Invalid Padding/2")
	}
	return data[:(length - unpadding)], nil
}

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

	// Verify the padding before using it
	padding := plainText[len(plainText)-1]
	if int(padding) > aes.BlockSize || int(padding) < 1 {
		return nil, errors.New("Invalid Padding/1")
	}
	plainText, err = RemovePad(plainText)
	if err != nil {
		return nil, err
	}
	return plainText, nil
}
