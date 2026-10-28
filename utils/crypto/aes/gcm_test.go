package aes

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"strings"
	"testing"
)

func unhex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatalf("bad hex %q: %v", s, err)
	}
	return b
}

// Pins both directions to the AES-256 test cases of the GCM specification
// (McGrew and Viega, "The Galois/Counter Mode of Operation", test cases 13
// to 16): the nonce is given, so the output is fixed.
func TestAESGCMKnownAnswers(t *testing.T) {
	zeroKey := strings.Repeat("00", 32)
	k15 := "feffe9928665731c6d6a8f9467308308feffe9928665731c6d6a8f9467308308"
	p15 := "d9313225f88406e5a55909c5aff5269a86a7a9531534f7da2e4c303d8a318a721c3c0c95956809532fcf0e2449a6b525b16aedf5aa0de657ba637b391aafd255"
	c15 := "522dc1f099567d07f47f37a32a84427d643a8cdcbfe5c0c97598a2bd2555d1aa8cb08e48590dbb3da7b08b1056828838c5f61e6393ba7a0abcc9f662898015ad"
	cases := []struct {
		name, key, nonce, plaintext, ad, ciphertext, tag string
	}{
		{"case 13, empty plaintext", zeroKey, "000000000000000000000000", "", "", "", "530f8afbc74536b9a963b4f1c4cb738b"},
		{"case 14, one zero block", zeroKey, "000000000000000000000000", strings.Repeat("00", 16), "", "cea7403d4d606b6e074ec5d3baf39d18", "d0d1c8a799996bf0265b98b5d48ab919"},
		{"case 15, 64 bytes", k15, "cafebabefacedbaddecaf888", p15, "", c15, "b094dac5d93471bdec1a502270e3cc6c"},
		{"case 16, 60 bytes with associated data", k15, "cafebabefacedbaddecaf888", p15[:120], "feedfacedeadbeeffeedfacedeadbeefabaddad2", c15[:120], "76fc6ece0f4e1768cddf8853bb2d551b"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			key, nonce := unhex(t, c.key), unhex(t, c.nonce)
			plaintext, ad := unhex(t, c.plaintext), unhex(t, c.ad)
			want := append([]byte{FormatAESGCM}, nonce...)
			want = append(want, unhex(t, c.ciphertext)...)
			want = append(want, unhex(t, c.tag)...)

			got, err := encryptAESGCMWithNonce(key, nonce, plaintext, ad)
			if err != nil {
				t.Fatalf("encrypt: %v", err)
			}
			if !bytes.Equal(got, want) {
				t.Errorf("encrypt = %x\nwant      %x", got, want)
			}
			back, err := DecryptAESGCM(key, want, ad)
			if err != nil {
				t.Fatalf("decrypt: %v", err)
			}
			if !bytes.Equal(back, plaintext) {
				t.Errorf("decrypt = %x, want %x", back, plaintext)
			}
		})
	}
}

func randomBytes(t *testing.T, n int) []byte {
	t.Helper()
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return b
}

func TestAESGCMRoundTrip(t *testing.T) {
	key := randomBytes(t, AESGCMKeySize)
	for _, n := range []int{0, 1, 15, 16, 17, 31, 32, 33, 1000, 65537} {
		plaintext := randomBytes(t, n)
		for _, ad := range [][]byte{nil, []byte("prekey-7")} {
			sealed, err := EncryptAESGCM(key, plaintext, ad)
			if err != nil {
				t.Fatalf("n=%d: encrypt: %v", n, err)
			}
			if len(sealed) != n+AESGCMOverhead {
				t.Errorf("n=%d: sealed length %d, want %d", n, len(sealed), n+AESGCMOverhead)
			}
			if !IsAESGCMFormat(sealed) {
				t.Errorf("n=%d: IsAESGCMFormat false on EncryptAESGCM's output", n)
			}
			opened, err := DecryptAESGCM(key, sealed, ad)
			if err != nil {
				t.Fatalf("n=%d: decrypt: %v", n, err)
			}
			if !bytes.Equal(opened, plaintext) {
				t.Errorf("n=%d: round trip changed the plaintext", n)
			}
		}
	}
}

func TestAESGCMNonceDiffersPerCall(t *testing.T) {
	key := randomBytes(t, AESGCMKeySize)
	a, err := EncryptAESGCM(key, []byte("same"), nil)
	if err != nil {
		t.Fatal(err)
	}
	b, err := EncryptAESGCM(key, []byte("same"), nil)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(a[1:1+AESGCMNonceSize], b[1:1+AESGCMNonceSize]) {
		t.Error("two encryptions used the same nonce")
	}
	if bytes.Equal(a, b) {
		t.Error("two encryptions of the same plaintext gave the same output")
	}
}

// Every byte of the output is covered by the authentication: a change in the
// format byte is refused as a format error, a change anywhere else as an
// authentication failure, and so are the wrong key and the wrong associated
// data.
func TestAESGCMRefusesTampering(t *testing.T) {
	key := randomBytes(t, AESGCMKeySize)
	ad := []byte("header")
	sealed, err := EncryptAESGCM(key, []byte("the plaintext under test"), ad)
	if err != nil {
		t.Fatal(err)
	}
	regions := map[string]int{
		"format byte": 0,
		"nonce":       1,
		"ciphertext":  1 + AESGCMNonceSize,
		"tag":         len(sealed) - 1,
	}
	for name, at := range regions {
		tampered := append([]byte(nil), sealed...)
		tampered[at] ^= 0x01
		out, err := DecryptAESGCM(key, tampered, ad)
		if err == nil {
			t.Errorf("%s changed: decrypt succeeded with %q", name, out)
			continue
		}
		if out != nil {
			t.Errorf("%s changed: plaintext returned beside the error", name)
		}
		want := "AES_GCM_AUTHENTICATION_FAILED"
		if name == "format byte" {
			want = "AES_GCM_INVALID_FORMAT"
		}
		if !strings.Contains(err.Error(), want) {
			t.Errorf("%s changed: error %q, want it to name %s", name, err, want)
		}
	}
	for i := 0; i < 8; i++ {
		truncated := sealed[:len(sealed)-1-i]
		if out, err := DecryptAESGCM(key, truncated, ad); err == nil {
			t.Errorf("truncated by %d: decrypt succeeded with %q", i+1, out)
		}
	}
	wrongKey := randomBytes(t, AESGCMKeySize)
	if _, err := DecryptAESGCM(wrongKey, sealed, ad); err == nil || !strings.Contains(err.Error(), "AES_GCM_AUTHENTICATION_FAILED") {
		t.Errorf("wrong key: err = %v", err)
	}
	if _, err := DecryptAESGCM(key, sealed, []byte("other header")); err == nil || !strings.Contains(err.Error(), "AES_GCM_AUTHENTICATION_FAILED") {
		t.Errorf("wrong associated data: err = %v", err)
	}
	if _, err := DecryptAESGCM(key, sealed, nil); err == nil {
		t.Error("associated data dropped: decrypt succeeded")
	}
}

// No input length panics, and nothing shorter than the overhead or without
// the format byte is accepted.
func TestAESGCMRefusesBadShapes(t *testing.T) {
	key := randomBytes(t, AESGCMKeySize)
	for n := 0; n < AESGCMOverhead; n++ {
		data := make([]byte, n)
		if n > 0 {
			data[0] = FormatAESGCM
		}
		if IsAESGCMFormat(data) {
			t.Errorf("%d bytes: IsAESGCMFormat true below the overhead", n)
		}
		_, err := DecryptAESGCM(key, data, nil)
		if err == nil || !strings.Contains(err.Error(), "AES_GCM_INVALID_FORMAT") {
			t.Errorf("%d bytes: err = %v, want AES_GCM_INVALID_FORMAT", n, err)
		}
	}
	noFormat := make([]byte, AESGCMOverhead+4)
	if IsAESGCMFormat(noFormat) {
		t.Error("IsAESGCMFormat true without the format byte")
	}
	if _, err := DecryptAESGCM(key, noFormat, nil); err == nil || !strings.Contains(err.Error(), "AES_GCM_INVALID_FORMAT") {
		t.Errorf("no format byte: err = %v, want AES_GCM_INVALID_FORMAT", err)
	}
	if _, err := DecryptAESGCM(key, nil, nil); err == nil {
		t.Error("nil input: decrypt succeeded")
	}
}

func TestAESGCMKeySize(t *testing.T) {
	for _, n := range []int{0, 16, 24, 31, 33} {
		key := randomBytes(t, n)
		if _, err := EncryptAESGCM(key, []byte("x"), nil); err == nil || !strings.Contains(err.Error(), "AES_GCM_INVALID_KEY_SIZE") {
			t.Errorf("encrypt with a %d-byte key: err = %v", n, err)
		}
		if _, err := DecryptAESGCM(key, make([]byte, AESGCMOverhead), nil); err == nil || !strings.Contains(err.Error(), "AES_GCM_INVALID_KEY_SIZE") {
			t.Errorf("decrypt with a %d-byte key: err = %v", n, err)
		}
	}
}
