package vault

import (
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/donnyhardyanto/dxlib/errors"
	"github.com/donnyhardyanto/dxlib/log"
	"github.com/donnyhardyanto/dxlib/utils"
	vault "github.com/hashicorp/vault/api"
)

type DXVaultInterface interface {
	Start() (err error)
	ResolveAsString(v string) (string, error)
	ResolveAsInt(v string) (int, error)
	ResolveAsInt64(v string) (int64, error)
	ResolveAsBool(v string) (bool, error)
	GetStringOrDefault(v string, d string) string
	GetIntOrDefault(v string, d int) int
	GetInt64OrDefault(v string, d int64) int64
	GetBoolOrDefault(v string, d bool) bool
}

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

func (hv *DXHashicorpVault) ResolveAsInt64(v string) (int64, error) {
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

func (hv *DXHashicorpVault) GetStringOrDefault(v string, d string) string {
	data, err := hv.VaultGetData(&log.Log)
	if err != nil {
		fmt.Print(err, "failed to get vault data for key: %s %+v", v, err)
		return d
	}

	// Use utils.GetStringFromKV for safe type conversion
	dvv, err := utils.GetStringFromKV(data, v)
	if err != nil {
		// Key not found or type mismatch - return default
		fmt.Println(err, "failed to get vault data for key: %s %+v", v, err)
		return d
	}
	return dvv
}

func (hv *DXHashicorpVault) GetIntOrDefault(v string, d int) int {
	data, err := hv.VaultGetData(&log.Log)
	if err != nil {
		// Key not found or type mismatch - return default
		fmt.Print(err, "failed to get vault data for key: %s %+v", v, err)
		return d
	}

	// Use utils.GetIntFromKV for safe type conversion
	dvv, err := utils.ConvertIntFromKV(data, v)
	if err != nil {
		// Key not found or type mismatch - return default
		fmt.Println(err, "failed to get vault data for key: %s %+v", v, err)
		return d
	}
	return dvv
}

func (hv *DXHashicorpVault) GetInt64OrDefault(v string, d int64) int64 {
	data, err := hv.VaultGetData(&log.Log)
	if err != nil {
		// Key not found or type mismatch - return default
		fmt.Println(err, "failed to get vault data for key: %s %+v", v, err)
		return d
	}

	// Use utils.GetInt64FromKV for safe type conversion
	dvv, err := utils.ConvertInt64FromKV(data, v)
	if err != nil {
		// Key not found or type mismatch - return default
		fmt.Println(err, "failed to get vault data for key: %s %+v", v, err)
		return d
	}
	return dvv
}

func (hv *DXHashicorpVault) GetBoolOrDefault(v string, d bool) bool {
	data, err := hv.VaultGetData(&log.Log)
	if err != nil {
		// Key not found or type mismatch - return default
		fmt.Println(err, "failed to get vault data for key: %s %+v", v, err)
		return d
	}

	// Use utils.ConvertToBoolFromKV for safe type conversion
	dvv, err := utils.ConvertToBoolFromKV(data, v)
	if err != nil {
		// Key not found or type mismatch - return default
		fmt.Println(err, "failed to get vault data for key: %s %+v", v, err)
		return d
	}
	return dvv
}

func (hv *DXHashicorpVault) VaultMapping(log *log.DXLog, texts ...string) (r []string, err error) {
	check := false
	for _, text := range texts {
		if strings.Contains(text, hv.Prefix) {
			check = true
			break
		}
	}
	if check {
		secret, err := hv.Client.Logical().Read(hv.Path)
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

func (hv *DXHashicorpVault) VaultMapString(log *log.DXLog, text string) (string, error) {
	if strings.Contains(text, hv.Prefix) {
		mapString := text
		secret, err := hv.Client.Logical().Read(hv.Path)
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

func (hv *DXHashicorpVault) VaultGetData(log *log.DXLog) (r utils.JSON, err error) {
	secret, err := hv.Client.Logical().Read(hv.Path)
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
