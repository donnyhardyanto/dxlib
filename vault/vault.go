package vault

import (
	"context"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/donnyhardyanto/dxlib/core"
	"github.com/donnyhardyanto/dxlib/errors"
	"github.com/donnyhardyanto/dxlib/log"
	dxlibOtel "github.com/donnyhardyanto/dxlib/otel"
	"github.com/donnyhardyanto/dxlib/utils"
	vault "github.com/hashicorp/vault/api"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

/*
	type DXVaultInterface interface {
		Start() (err error)
		GetStringOrDefault(v string, d string) string
		GetIntOrDefault(v string, d int) int
		GetInt64OrDefault(v string, d int64) int64
		GetBoolOrDefault(v string, d bool) bool
	}
*/
type DXVault struct {
	Vendor  string
	Address string
	Token   string
	Prefix  string
	Path    string
}

type Prefix map[string]*DXVault

func NewVaultVendor(vendor string, address string, token string, prefix string, path string) *DXVault {
	return &DXVault{
		Vendor:  vendor,
		Address: address,
		Token:   token,
		Prefix:  prefix,
		Path:    path,
	}
}

type DXHashicorpVault struct {
	DXVault
	Client *vault.Client
}

/*
func NewHashiCorpVault(address string, token string, prefix string, path string) *DXHashicorpVault {
	v := &DXHashicorpVault{
		DXVault: DXVault{
			Vendor:  "HASHICORP-VAULT",
			Address: address,
			Token:   token,
			Prefix:  prefix,
			Path:    path,
		},
	}
	return v
}*/

func NewHashiCorpVault(address string, token string, prefix string, path string) *DXHashicorpVault {
	v := &DXHashicorpVault{
		DXVault: *NewVaultVendor(
			"HASHICORP-VAULT",
			address,
			token,
			prefix,
			path,
		),
	}
	return v
}

func (hv *DXHashicorpVault) Start() (err error) {
	config := vault.DefaultConfig()
	config.Address = hv.Address
	hv.Client, err = vault.NewClient(config)
	if err != nil {
		return errors.Wrap(err, "ERROR_IN_HASHICORP_VAULT_CLIENT_CREATION")
	}
	hv.Client.SetToken(hv.Token)
	return nil
}

func (hv *DXHashicorpVault) vaultOtelStart(ctx context.Context, opName string) (context.Context, func(err error)) {
	if !core.IsOtelEnabled {
		return ctx, func(error) { /* no-op: OTel disabled */ }
	}
	ctx, s := otel.Tracer("dxlib.vault").Start(ctx, "vault."+opName,
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("peer.service", "vault"),
			attribute.String("vault.operation", opName),
			attribute.String("vault.path", hv.Path),
			attribute.String("server.address", hv.Address),
		),
	)
	start := time.Now()
	attrs := metric.WithAttributes(
		attribute.String("peer.service", "vault"),
		attribute.String("vault.operation", opName),
	)
	return ctx, func(err error) {
		dxlibOtel.HTTPClientDuration.Record(ctx, time.Since(start).Seconds(), attrs)
		dxlibOtel.HTTPClientCount.Add(ctx, 1, attrs)
		if err != nil {
			s.SetStatus(codes.Error, err.Error())
		}
		s.End()
	}
}

/*func (hv *DXHashicorpVault) ResolveAsInt64(v string) (int64, error) {
	s, err := hv.VaultMapString(&log.Log, v)
	if err != nil {
		return 0, err
	}
	if s != "" {
		parsedValue, parseErr := strconv.ParseInt(s, 10, 64)
		if parseErr != nil {
			return 0, errors.Wrapf(parseErr, "failed to parse int64 from vault value: %s", v)
		}
		return parsedValue, nil
	}
	return 0, nil
}

func (hv *DXHashicorpVault) ResolveAsInt(v string) (int, error) {
	s, err := hv.VaultMapString(&log.Log, v)
	if err != nil {
		return 0, err
	}
	if s != "" {
		parsedValue, parseErr := strconv.ParseInt(s, 10, 32)
		if parseErr != nil {
			return 0, errors.Wrapf(parseErr, "failed to parse int from vault value: %s", v)
		}
		return int(parsedValue), nil
	}
	return 0, nil
}

func (hv *DXHashicorpVault) ResolveAsBool(v string) (bool, error) {
	s, err := hv.VaultMapString(&log.Log, v)
	if err != nil {
		return false, err
	}
	if s == "" {
		return false, nil
	}
	s = strings.TrimSpace(s)
	s = strings.ToLower(s)
	if slices.Contains([]string{"true", "yes", "on", "1"}, s) {
		return true, nil
	}
	if slices.Contains([]string{"false", "no", "off", "0"}, s) {
		return false, nil
	}
	parsedValue, parseErr := strconv.ParseInt(s, 10, 32)
	if parseErr != nil {
		return false, errors.Wrapf(parseErr, "failed to parse bool from vault value: %s", v)
	}
	return parsedValue > 0, nil
}

func (hv *DXHashicorpVault) ResolveAsString(v string) (string, error) {
	return hv.VaultMapString(&log.Log, v)
}
*/

func (hv *DXHashicorpVault) GetString(ctx context.Context, key string) (string, error) {
	data, err := hv.VaultGetData(ctx, &log.Log)
	if err != nil {
		return "", err
	}

	// Use utils.GetStringFromKV for safe type conversion
	dvv, err := utils.GetStringFromKV(data, key)
	if err != nil {
		return "", err
	}
	return dvv, nil
}

// envOrDefault is the FALLBACK used after a Vault miss: return os.Getenv(key) when set
// (non-empty), else the literal default d. Precedence is VAULT-FIRST, env-fallback —
// a key present in Vault always wins; the process env (docker run.env/key.env) only
// fills what Vault lacks. The Vault key name IS the env var name (e.g. DB_POSTGRES_ADDRESS),
// so infra connection config can live with the deploy while secrets stay in Vault.
func envOrDefault(key, d string) string {
	if e := os.Getenv(key); e != "" {
		return e
	}
	return d
}

func envIntOrDefault(key string, d int) int {
	if e := os.Getenv(key); e != "" {
		if n, err := strconv.Atoi(e); err == nil {
			return n
		}
	}
	return d
}

func envInt64OrDefault(key string, d int64) int64 {
	if e := os.Getenv(key); e != "" {
		if n, err := strconv.ParseInt(e, 10, 64); err == nil {
			return n
		}
	}
	return d
}

func envBoolOrDefault(key string, d bool) bool {
	if e := os.Getenv(key); e != "" {
		if b, err := strconv.ParseBool(e); err == nil {
			return b
		}
	}
	return d
}

// ── Get*OrDefault: PURE Vault → literal default (no env). Unchanged semantics. ──

func (hv *DXHashicorpVault) GetStringOrDefault(ctx context.Context, v string, d string) string {
	data, err := hv.VaultGetData(ctx, &log.Log)
	if err == nil {
		if dvv, err2 := utils.GetStringFromKV(data, v); err2 == nil {
			return dvv
		}
	}
	maskedDefault := d
	if utils.IsSensitiveField(v) && d != "" {
		maskedDefault = "********"
	} else if d == "" {
		maskedDefault = "(empty)"
	}
	log.Log.Infof("Vault key not found: %s, using default: %s", v, maskedDefault)
	return d
}

func (hv *DXHashicorpVault) GetIntOrDefault(ctx context.Context, v string, d int) int {
	data, err := hv.VaultGetData(ctx, &log.Log)
	if err == nil {
		if dvv, err2 := utils.ConvertIntFromKV(data, v); err2 == nil {
			return dvv
		}
	}
	log.Log.Infof("Vault key not found: %s, using default: %d", v, d)
	return d
}

func (hv *DXHashicorpVault) GetInt64OrDefault(ctx context.Context, v string, d int64) int64 {
	data, err := hv.VaultGetData(ctx, &log.Log)
	if err == nil {
		if dvv, err2 := utils.ConvertInt64FromKV(data, v); err2 == nil {
			return dvv
		}
	}
	log.Log.Infof("Vault key not found: %s, using default: %d", v, d)
	return d
}

func (hv *DXHashicorpVault) GetBoolOrDefault(ctx context.Context, v string, d bool) bool {
	data, err := hv.VaultGetData(ctx, &log.Log)
	if err == nil {
		if dvv, err2 := utils.ConvertToBoolFromKV(data, v); err2 == nil {
			return dvv
		}
	}
	log.Log.Infof("Vault key not found: %s, using default: %t", v, d)
	return d
}

// ── Get*OrEnvOrDefault: Vault → env (os.Getenv, key name == env var) → literal default. ──
// Use these for INFRA connection config (DB/Redis addresses) that may be sourced from the
// deploy env (run.env/key.env) instead of Vault. A key present in Vault still wins; env only
// fills what Vault lacks. Do NOT use for secrets — keep those on the pure Get*OrDefault path.

func (hv *DXHashicorpVault) GetStringOrEnvOrDefault(ctx context.Context, v string, d string) string {
	data, err := hv.VaultGetData(ctx, &log.Log)
	if err == nil {
		if dvv, err2 := utils.GetStringFromKV(data, v); err2 == nil {
			return dvv // Vault wins
		}
	}
	return envOrDefault(v, d) // else env, else default
}

func (hv *DXHashicorpVault) GetIntOrEnvOrDefault(ctx context.Context, v string, d int) int {
	data, err := hv.VaultGetData(ctx, &log.Log)
	if err == nil {
		if dvv, err2 := utils.ConvertIntFromKV(data, v); err2 == nil {
			return dvv
		}
	}
	return envIntOrDefault(v, d)
}

func (hv *DXHashicorpVault) GetInt64OrEnvOrDefault(ctx context.Context, v string, d int64) int64 {
	data, err := hv.VaultGetData(ctx, &log.Log)
	if err == nil {
		if dvv, err2 := utils.ConvertInt64FromKV(data, v); err2 == nil {
			return dvv
		}
	}
	return envInt64OrDefault(v, d)
}

func (hv *DXHashicorpVault) GetBoolOrEnvOrDefault(ctx context.Context, v string, d bool) bool {
	data, err := hv.VaultGetData(ctx, &log.Log)
	if err == nil {
		if dvv, err2 := utils.ConvertToBoolFromKV(data, v); err2 == nil {
			return dvv
		}
	}
	return envBoolOrDefault(v, d)
}

func (hv *DXHashicorpVault) VaultMapping(ctx context.Context, log *log.DXLog, texts ...string) (r []string, err error) {
	check := false
	for _, text := range texts {
		if strings.Contains(text, hv.Prefix) {
			check = true
			break
		}
	}
	if check {
		_, endOtel := hv.vaultOtelStart(ctx, "READ")
		secret, err := hv.Client.Logical().Read(hv.Path)
		endOtel(err)
		if err != nil {
			log.Errorf(err, "Unable to read credentials from Vault")
			return nil, err
		}
		var results []string
		data, ok := secret.Data["data"].(map[string]any)
		if !ok {
			err = log.ErrorAndCreateErrorf("unable to read path from Vault")
			return nil, err
		}
		for _, text := range texts {
			if strings.Contains(text, hv.Prefix) {
				key := strings.TrimPrefix(text, hv.Prefix)
				// Use utils.GetStringFromKV for safe type conversion
				value, err := utils.GetStringFromKV(data, key)
				if err != nil {
					return nil, errors.Wrapf(err, "vault key %s not found or not a string", key)
				}
				results = append(results, value)
			} else {
				results = append(results, text)
			}
		}
		return results, nil
	}
	return texts, nil
}

func (hv *DXHashicorpVault) VaultMapString(ctx context.Context, log *log.DXLog, text string) (string, error) {
	if strings.Contains(text, hv.Prefix) {
		mapString := text
		_, endOtel := hv.vaultOtelStart(ctx, "READ")
		secret, err := hv.Client.Logical().Read(hv.Path)
		endOtel(err)
		if err != nil {
			return "", errors.Wrapf(err, "unable to read credentials from Vault")
		}
		data, ok := secret.Data["data"].(map[string]any)
		if !ok {
			return "", errors.Errorf("unable to read path from Vault")
		}
		for key, value := range data {
			placeholder := hv.Prefix + key
			// Safe type assertion
			valueStr, ok := value.(string)
			if !ok {
				return "", errors.Errorf("vault value for key %s is not a string: %v", key, value)
			}
			mapString = strings.Replace(mapString, placeholder, valueStr, -1)
		}
		return mapString, nil
	}
	return text, nil
}

func (hv *DXHashicorpVault) VaultGetData(ctx context.Context, log *log.DXLog) (r utils.JSON, err error) {
	_, endOtel := hv.vaultOtelStart(ctx, "READ")
	secret, err := hv.Client.Logical().Read(hv.Path)
	endOtel(err)
	if err != nil {
		log.Fatalf("Unable to read credentials from Vault: %v", err.Error())
		return nil, err
	}
	data, ok := secret.Data["data"].(map[string]any)
	if !ok {
		err = log.ErrorAndCreateErrorf("unable to read path from Vault:%s", hv.Path)
		return nil, err
	}
	return data, nil
}
