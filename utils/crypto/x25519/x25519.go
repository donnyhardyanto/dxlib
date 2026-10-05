// Package x25519 performs X25519 key agreement (RFC 7748) on raw 32-byte keys, as
// peers exchange them on the wire.
package x25519

import (
	"crypto/ecdh"
	"crypto/rand"
)

// GenerateKeyPair returns a fresh X25519 key pair as raw 32-byte slices.
func GenerateKeyPair() (publicKey []byte, privateKey []byte, err error) {
	key, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		return nil, nil, err
	}
	return key.PublicKey().Bytes(), key.Bytes(), nil
}

// ComputeSharedSecret returns the 32-byte X25519 shared secret of privateKey and
// peerPublicKey, both raw 32-byte values. A key of another length or a low-order
// peer point (an all-zero result) is an error.
func ComputeSharedSecret(privateKey, peerPublicKey []byte) ([]byte, error) {
	curve := ecdh.X25519()
	peer, err := curve.NewPublicKey(peerPublicKey)
	if err != nil {
		return nil, err
	}
	own, err := curve.NewPrivateKey(privateKey)
	if err != nil {
		return nil, err
	}
	return own.ECDH(peer)
}
