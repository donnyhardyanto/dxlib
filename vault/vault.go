package vault

import (
	"fmt"
	"github.com/donnyhardyanto/dxlib/log"
	"github.com/donnyhardyanto/dxlib/utils"
	vault "github.com/hashicorp/vault/api"
	"github.com/pkg/errors"
	"strconv"
	"strings"
)

type DXVaultInterface interface {
	Start() (err error)
	ResolveAsString(v string) string
	ResolveAsInt(v string) int
	ResolveAsInt64(v string) int64
	ResolveAsBool(v string) bool
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
		return err
	}
	hv.Client.SetToken(hv.Token)
	return nil
}

func (hv *DXHashicorpVault) ResolveAsInt64(v string) int64 {
	vi := int64(0)
	s := hv.VaultMapString(&log.Log, v)
	if s != "" {
		parsedValue, parseErr := strconv.ParseInt(s, 10, 64)
		if parseErr != nil {
			panic(parseErr)
			return 0
		}
		vi = parsedValue
	}
	return vi
}

func (hv *DXHashicorpVault) ResolveAsInt(v string) int {
	vi := 0
	s := hv.VaultMapString(&log.Log, v)
	if s != "" {
		parsedValue, parseErr := strconv.ParseInt(s, 10, 32)
		if parseErr != nil {
			panic(parseErr)
			return 0
		}
		vi = int(parsedValue)
	}
	return vi
}

func (hv *DXHashicorpVault) ResolveAsBool(v string) bool {
	vi := 0
	s := hv.VaultMapString(&log.Log, v)
	if s != "" {
		parsedValue, parseErr := strconv.ParseInt(s, 10, 32)
		if parseErr != nil {
			panic(parseErr)
			return false
		}
		vi = int(parsedValue)
	}
	if vi == 0 {
		return false
	} else {
		return true
	}
}

func (hv *DXHashicorpVault) ResolveAsString(v string) string {
	return hv.VaultMapString(&log.Log, v)
}

func (hv *DXHashicorpVault) GetStringOrDefault(v string, d string) string {
	data, err := hv.VaultGetData(&log.Log)
	if err != nil {
		fmt.Sprintf("GetStringOrDefault/hv.VaultGetData=%s", err.Error())
		panic(err)
	}
	dv, ok := data[v]
	if !ok {
		return d
	}
	dvv, ok := dv.(string)
	if !ok {
		err = errors.Errorf("vault data is not string: %s=%v", v, dv)
		panic(err)
	}
	return dvv
}

func (hv *DXHashicorpVault) GetIntOrDefault(v string, d int) int {
	data, err := hv.VaultGetData(&log.Log)
	if err != nil {
		panic(err)
	}
	dv, ok := data[v]
	if !ok {
		return d
	}
	dvv, err := strconv.ParseInt(dv.(string), 10, 32)
	if err != nil {
		panic(err)
	}
	return int(dvv)
}

func (hv *DXHashicorpVault) GetInt64OrDefault(v string, d int64) int64 {
	data, err := hv.VaultGetData(&log.Log)
	if err != nil {
		panic(err)
	}
	dv, ok := data[v]
	if !ok {
		return d
	}
	dvv, err := strconv.ParseInt(dv.(string), 10, 64)
	if err != nil {
		panic(err)
	}
	return dvv
}

func (hv *DXHashicorpVault) GetBoolOrDefault(v string, d bool) bool {
	data, err := hv.VaultGetData(&log.Log)
	if err != nil {
		panic(err)
	}
	dv, ok := data[v]
	if !ok {
		return d
	}
	dvv, err := strconv.ParseInt(dv.(string), 10, 64)
	if err != nil {
		panic(err)
	}
	if dvv == 0 {
		return false
	} else {
		return true
	}
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
			log.Errorf("Unable to read credentials from Vault: %v", err.Error())
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
				results = append(results, data[key].(string))
			} else {
				results = append(results, text)
			}
		}
		return results, nil
	}
	return texts, nil
}

func (hv *DXHashicorpVault) VaultMapString(log *log.DXLog, text string) string {
	if strings.Contains(text, hv.Prefix) {
		mapString := text
		secret, err := hv.Client.Logical().Read(hv.Path)
		if err != nil {
			log.Fatalf("Unable to read credentials from Vault: %v", err.Error())
			return ""
		}
		data, ok := secret.Data["data"].(map[string]any)
		if !ok {
			log.Fatalf("unable to read path from Vault")
			return ""
		}
		for key, value := range data {
			placeholder := hv.Prefix + key
			mapString = strings.Replace(mapString, placeholder, value.(string), -1)
		}
		return mapString
	}
	return text
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
