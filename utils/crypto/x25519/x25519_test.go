package x25519

import (
	"bytes"
	"encoding/hex"
	"testing"
)

// Pins the package to RFC 7748, section 6.1: the shared secret of the two sample key
// pairs there, and each side's public key as derived from its private key.
func TestRFC7748Vectors(t *testing.T) {
	alicePrivate := unhex(t, "77076d0a7318a57d3c16c17251b26645df4c2f87ebc0992ab177fba51db92c2a")
	alicePublic := unhex(t, "8520f0098930a754748b7ddcb43ef75a0dbf3a0d26381af4eba4a98eaa9b4e6a")
	bobPrivate := unhex(t, "5dab087e624a8a4b79e17f8b83800ee66f3bb1292618b6fd1c2f8b27ff88e0eb")
	bobPublic := unhex(t, "de9edb7d7b7dc1b4d35b61c2ece435373f8343c85b78674dadfc7e146f882b4f")
	shared := unhex(t, "4a5d9d5ba4ce2de1728e3bf480350f25e07e21c947d19e3376f09b3c1e161742")

	got, err := ComputeSharedSecret(alicePrivate, bobPublic)
	if err != nil {
		t.Fatalf("alice: %v", err)
	}
	if !bytes.Equal(got, shared) {
		t.Errorf("alice's shared secret = %x, want %x", got, shared)
	}
	got, err = ComputeSharedSecret(bobPrivate, alicePublic)
	if err != nil {
		t.Fatalf("bob: %v", err)
	}
	if !bytes.Equal(got, shared) {
		t.Errorf("bob's shared secret = %x, want %x", got, shared)
	}
}

func TestGeneratedPairsAgree(t *testing.T) {
	publicA, privateA, err := GenerateKeyPair()
	if err != nil {
		t.Fatal(err)
	}
	publicB, privateB, err := GenerateKeyPair()
	if err != nil {
		t.Fatal(err)
	}
	for name, key := range map[string][]byte{"publicA": publicA, "privateA": privateA, "publicB": publicB, "privateB": privateB} {
		if len(key) != 32 {
			t.Errorf("%s is %d bytes, want 32", name, len(key))
		}
	}
	if bytes.Equal(privateA, privateB) {
		t.Fatal("two generated private keys are equal")
	}
	secretA, err := ComputeSharedSecret(privateA, publicB)
	if err != nil {
		t.Fatal(err)
	}
	secretB, err := ComputeSharedSecret(privateB, publicA)
	if err != nil {
		t.Fatal(err)
	}
	if len(secretA) != 32 || !bytes.Equal(secretA, secretB) {
		t.Errorf("shared secrets differ: %x vs %x", secretA, secretB)
	}
}

func TestRejectsBadInput(t *testing.T) {
	public, private, err := GenerateKeyPair()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ComputeSharedSecret(private[:31], public); err == nil {
		t.Error("a 31-byte private key was accepted")
	}
	if _, err := ComputeSharedSecret(private, public[:31]); err == nil {
		t.Error("a 31-byte peer public key was accepted")
	}
	if _, err := ComputeSharedSecret(private, make([]byte, 32)); err == nil {
		t.Error("the all-zero peer point (low order) was accepted")
	}
}

func unhex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
