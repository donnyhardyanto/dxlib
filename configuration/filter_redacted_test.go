package configuration

import (
	"testing"

	"github.com/donnyhardyanto/dxlib/utils"
)

// A config dump shows a secret as the redaction marker, whether a credential keyword or the
// explicit SensitiveDataKey list names it.
func TestFilterSensitiveDataRedactsSecrets(t *testing.T) {
	c := &DXConfiguration{
		Data:             &utils.JSON{"db": map[string]any{"host": "db.example.test", "password": "hunter2"}, "smtp_relay": "relay-secret"},
		SensitiveDataKey: []string{"smtp_relay"},
	}
	got := c.FilterSensitiveData()
	db := got["db"].(utils.JSON)
	if db["password"] != utils.MaskRedactedMarker {
		t.Errorf("db.password: got %v", db["password"])
	}
	if db["host"] != "db.example.test" {
		t.Errorf("db.host: got %v", db["host"])
	}
	if got["smtp_relay"] != utils.MaskRedactedMarker {
		t.Errorf("smtp_relay: got %v", got["smtp_relay"])
	}
}
