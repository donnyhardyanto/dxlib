package x25519

import (
	"crypto/rand"
	"golang.org/x/crypto/curve25519"
)

func GenerateKeyPair() (publicKey [32]byte, privateKey [32]byte, err error) {
	_, err = rand.Read(privateKey[:])
	if err != nil {
		return publicKey, privateKey, err
	}
	curve25519.ScalarBaseMult(&publicKey, &privateKey)
	return
}

func ComputeSharedSecret(privateKey, peerPublicKey []byte) ([]byte, error) {
	sharedSecret, err := curve25519.X25519(privateKey, peerPublicKey)
	if err != nil {
		return nil, err
	}
	return sharedSecret, nil
}
