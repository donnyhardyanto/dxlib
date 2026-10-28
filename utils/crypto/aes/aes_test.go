package aes

import (
	"bytes"
	"crypto/aes"
	"testing"
)

// Pins the deprecated helper to what it has always done, so that moving
// callers to EncryptAESGCM changes nothing for the data already written
// with it: a 16-byte IV, PKCS#7 padding to a whole block, the padding still
// on after DecryptAES, and RemovePad taking it off.
func TestAESCBCUnchanged(t *testing.T) {
	for _, keySize := range []int{16, 24, 32} {
		key := randomBytes(t, keySize)
		for _, n := range []int{0, 1, 15, 16, 17, 32, 100} {
			plaintext := randomBytes(t, n)
			sealed, err := EncryptAES(key, plaintext)
			if err != nil {
				t.Fatalf("key %d, n=%d: encrypt: %v", keySize, n, err)
			}
			wantLen := aes.BlockSize + (n/aes.BlockSize+1)*aes.BlockSize
			if len(sealed) != wantLen {
				t.Errorf("key %d, n=%d: sealed length %d, want %d", keySize, n, len(sealed), wantLen)
			}
			padded, err := DecryptAES(key, sealed)
			if err != nil {
				t.Fatalf("key %d, n=%d: decrypt: %v", keySize, n, err)
			}
			if !bytes.Equal(padded, Pad(append([]byte(nil), plaintext...), aes.BlockSize)) {
				t.Errorf("key %d, n=%d: DecryptAES did not return the padded plaintext", keySize, n)
			}
			opened, err := RemovePad(padded)
			if err != nil {
				t.Fatalf("key %d, n=%d: RemovePad: %v", keySize, n, err)
			}
			if !bytes.Equal(opened, plaintext) {
				t.Errorf("key %d, n=%d: round trip changed the plaintext", keySize, n)
			}
		}
	}
}

func TestPadRemovePad(t *testing.T) {
	for n := 0; n <= 2*aes.BlockSize; n++ {
		padded := Pad(make([]byte, n), aes.BlockSize)
		if len(padded)%aes.BlockSize != 0 || len(padded) <= n {
			t.Errorf("n=%d: padded length %d", n, len(padded))
		}
		want := byte(len(padded) - n)
		for _, b := range padded[n:] {
			if b != want {
				t.Errorf("n=%d: padding byte %d, want %d", n, b, want)
			}
		}
		if _, err := RemovePad(padded); err != nil {
			t.Errorf("n=%d: RemovePad: %v", n, err)
		}
	}
	// Only a padding byte above the block size or the length is refused; the
	// padding bytes are not compared, and a zero count is accepted, as it
	// always was.
	for _, bad := range [][]byte{{17}, {1, 2, 3, 20}} {
		if _, err := RemovePad(bad); err == nil {
			t.Errorf("RemovePad(%v) accepted the padding", bad)
		}
	}
}
