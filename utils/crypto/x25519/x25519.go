package x25519

import (
	"crypto/rand"
	"io"

	"golang.org/x/crypto/curve25519"
)

func GenerateKeyPair() (publicKey []byte, privateKey []byte, err error) {
	privateKey = make([]byte, 32)
	if _, err = io.ReadFull(rand.Reader, privateKey); err != nil {
		return nil, nil, err
	}
	publicKey, err = curve25519.X25519(privateKey, curve25519.Basepoint)
	return
}

func ComputeSharedSecret(privateKey, peerPublicKey []byte) ([]byte, error) {
	sharedSecret, err := curve25519.X25519(privateKey, peerPublicKey)
	if err != nil {
		return nil, err
	}
	return sharedSecret, nil
}
