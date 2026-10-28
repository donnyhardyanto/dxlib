package datablock

import (
	"bytes"
	stdaes "crypto/aes"
	"crypto/cipher"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/donnyhardyanto/dxlib/utils/crypto/aes"
	"github.com/donnyhardyanto/dxlib/utils/lv"
)

type peer struct {
	public, private []byte
}

func newPeer(t *testing.T) peer {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return peer{public: pub, private: priv}
}

func randomKey(t *testing.T) []byte {
	t.Helper()
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatal(err)
	}
	return key
}

func mustLV(t *testing.T, s string) *lv.LV {
	t.Helper()
	l, err := lv.NewLV([]byte(s))
	if err != nil {
		t.Fatal(err)
	}
	return l
}

func wantPayloads(t *testing.T, got []*lv.LV, want ...string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %d payloads, want %d", len(got), len(want))
	}
	for i := range want {
		if string(got[i].Value) != want[i] {
			t.Errorf("payload %d = %q, want %q", i, got[i].Value, want[i])
		}
	}
}

// Both writers round-trip through the one reader.
func TestPackUnpackBothFormats(t *testing.T) {
	sender := newPeer(t)
	key := randomKey(t)
	packers := map[string]func(string, []byte, []byte, ...*lv.LV) (string, error){
		"PackLVPayload":       PackLVPayload,
		"PackLVPayloadAESGCM": PackLVPayloadAESGCM,
	}
	for name, pack := range packers {
		t.Run(name, func(t *testing.T) {
			envelope, err := pack("7", sender.private, key, mustLV(t, "login"), mustLV(t, "password-hash"))
			if err != nil {
				t.Fatalf("pack: %v", err)
			}
			got, err := UnpackLVPayload("7", sender.public, key, envelope)
			if err != nil {
				t.Fatalf("unpack: %v", err)
			}
			wantPayloads(t, got, "login", "password-hash")
		})
	}
}

// The envelope's encrypted block is in the format each writer names: the
// CBC one is an IV and whole blocks, the GCM one starts with the format byte.
func encryptedBlock(t *testing.T, envelope string) []byte {
	t.Helper()
	raw, err := hex.DecodeString(envelope)
	if err != nil {
		t.Fatal(err)
	}
	outer := lv.LV{}
	if err := outer.UnmarshalBinary(raw); err != nil {
		t.Fatal(err)
	}
	parts, err := outer.Expand()
	if err != nil {
		t.Fatal(err)
	}
	return parts[0].Value
}

func TestPackFormats(t *testing.T) {
	sender := newPeer(t)
	key := randomKey(t)
	cbc, err := PackLVPayload("1", sender.private, key, mustLV(t, "x"))
	if err != nil {
		t.Fatal(err)
	}
	block := encryptedBlock(t, cbc)
	if !isCBCShaped(block) {
		t.Errorf("PackLVPayload wrote %d bytes, not an IV and whole blocks", len(block))
	}
	gcm, err := PackLVPayloadAESGCM("1", sender.private, key, mustLV(t, "x"))
	if err != nil {
		t.Fatal(err)
	}
	block = encryptedBlock(t, gcm)
	if !aes.IsAESGCMFormat(block) {
		t.Errorf("PackLVPayloadAESGCM wrote a block without the format byte")
	}
}

// A CBC block whose random IV starts with the format byte is still read as
// CBC: the GCM attempt fails to authenticate and the reader falls back.
func TestDecryptDataBlockCBCWithFormatByteIV(t *testing.T) {
	key := randomKey(t)
	plaintext := aes.Pad([]byte("a data block whose IV collides"), stdaes.BlockSize)
	block, err := stdaes.NewCipher(key)
	if err != nil {
		t.Fatal(err)
	}
	iv := make([]byte, stdaes.BlockSize)
	if _, err := rand.Read(iv); err != nil {
		t.Fatal(err)
	}
	iv[0] = aes.FormatAESGCM
	sealed := make([]byte, stdaes.BlockSize+len(plaintext))
	copy(sealed, iv)
	cipher.NewCBCEncrypter(block, iv).CryptBlocks(sealed[stdaes.BlockSize:], plaintext)
	if !aes.IsAESGCMFormat(sealed) {
		t.Fatal("the test did not make a colliding block")
	}
	got, err := decryptDataBlock(key, sealed)
	if err != nil {
		t.Fatalf("decrypt: %v", err)
	}
	if !bytes.Equal(got, plaintext) {
		t.Errorf("decrypt = %x, want %x", got, plaintext)
	}
}

// A GCM block of a length that is also CBC-shaped is read as GCM first.
func TestDecryptDataBlockGCMWithCBCLength(t *testing.T) {
	key := randomKey(t)
	plaintext := make([]byte, 64-aes.AESGCMOverhead) // 64 bytes sealed: whole blocks
	sealed, err := aes.EncryptAESGCM(key, plaintext, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !isCBCShaped(sealed) {
		t.Fatalf("the test did not make a CBC-shaped GCM block (%d bytes)", len(sealed))
	}
	got, err := decryptDataBlock(key, sealed)
	if err != nil {
		t.Fatalf("decrypt: %v", err)
	}
	if !bytes.Equal(got, plaintext) {
		t.Error("decrypt did not return the GCM plaintext")
	}
}

// No block length panics: too short for either format, or GCM-shaped but
// not whole blocks and failing to authenticate, is an error.
func TestDecryptDataBlockRefusesBadShapes(t *testing.T) {
	key := randomKey(t)
	for _, n := range []int{0, 1, 15, 16, 17, 31, 33} {
		data := make([]byte, n)
		got, err := decryptDataBlock(key, data)
		if err == nil {
			t.Errorf("%d zero bytes: decrypt returned %x", n, got)
			continue
		}
		if !strings.Contains(err.Error(), "INVALID_DATA") && !strings.Contains(err.Error(), "AES_GCM") {
			t.Errorf("%d zero bytes: err = %v", n, err)
		}
	}
	gcmShaped := make([]byte, aes.AESGCMOverhead+5) // 34 bytes: format byte, not whole blocks
	gcmShaped[0] = aes.FormatAESGCM
	if _, err := decryptDataBlock(key, gcmShaped); err == nil || !strings.Contains(err.Error(), "AES_GCM_AUTHENTICATION_FAILED") {
		t.Errorf("unauthenticated GCM-shaped block: err = %v", err)
	}
}

// What the reader checks before and after decrypting: the signature, the
// pre-key index and a block that is too short.
func TestUnpackRefuses(t *testing.T) {
	sender, other := newPeer(t), newPeer(t)
	key := randomKey(t)
	for name, pack := range map[string]func(string, []byte, []byte, ...*lv.LV) (string, error){
		"PackLVPayload":       PackLVPayload,
		"PackLVPayloadAESGCM": PackLVPayloadAESGCM,
	} {
		t.Run(name, func(t *testing.T) {
			envelope, err := pack("7", sender.private, key, mustLV(t, "login"))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := UnpackLVPayload("7", other.public, key, envelope); err == nil || !strings.Contains(err.Error(), "INVALID_SIGNATURE") {
				t.Errorf("other peer's key: err = %v", err)
			}
			if _, err := UnpackLVPayload("8", sender.public, key, envelope); err == nil || !strings.Contains(err.Error(), "INVALID_PREKEY") {
				t.Errorf("other pre-key index: err = %v", err)
			}
			if _, err := UnpackLVPayload("7", sender.public, randomKey(t), envelope); err == nil {
				t.Error("other encryption key: unpack succeeded")
			}
			raw, _ := hex.DecodeString(envelope)
			raw[len(raw)-1] ^= 0x01
			if _, err := UnpackLVPayload("7", sender.public, key, hex.EncodeToString(raw)); err == nil {
				t.Error("tampered envelope: unpack succeeded")
			}
		})
	}
	// A signed envelope whose encrypted block is 5 bytes: refused, no panic.
	short, err := lv.NewLV([]byte{1, 2, 3, 4, 5})
	if err != nil {
		t.Fatal(err)
	}
	sig, err := lv.NewLV(ed25519.Sign(sender.private, short.Value))
	if err != nil {
		t.Fatal(err)
	}
	outer, err := lv.CombineLV(short, sig)
	if err != nil {
		t.Fatal(err)
	}
	envelope, err := outer.AsHexString()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := UnpackLVPayload("7", sender.public, key, envelope); err == nil || !strings.Contains(err.Error(), "INVALID_DATA") {
		t.Errorf("5-byte block: err = %v", err)
	}
}
