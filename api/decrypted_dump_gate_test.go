package api

import (
	"strings"
	"testing"

	"github.com/donnyhardyanto/dxlib/utils"
)

// BUG-SEC-122: the decrypted dump must be OFF by default and reveal nothing when off.
func TestDecryptedDumpGate(t *testing.T) {
	orig := logDecryptedBody
	t.Cleanup(func() { SetLogDecryptedBody(orig) })

	aepr := &DXAPIEndPointRequest{
		EffectiveRequestHeader: map[string]string{"X-Cif": "12345"},
		DecryptedRequestBody:   utils.JSON{"nik": "3175012345678901", "nama": "Budi"},
	}

	// OFF (default): must NOT contain any decrypted content.
	SetLogDecryptedBody(false)
	off := aepr.DecryptedRequestDumpAsString()
	for _, leak := range []string{"3175012345678901", "Budi", "12345", "X-Cif"} {
		if strings.Contains(off, leak) {
			t.Fatalf("gate OFF leaked %q: %s", leak, off)
		}
	}
	if !strings.Contains(off, "disabled") {
		t.Errorf("gate OFF should say disabled, got: %s", off)
	}

	// ON: dumps (with masking applied by MaskSensitiveValue — verified in utils test).
	SetLogDecryptedBody(true)
	on := aepr.DecryptedRequestDumpAsString()
	if !strings.Contains(on, "Decrypted") {
		t.Errorf("gate ON should produce a dump, got: %s", on)
	}
}
