package vault

import (
	"dxlib/v3/log"
	vault "github.com/hashicorp/vault/api"
	"strings"
)

type DXVaultInterface interface {
	Start() (err error)
	ResolveAsString(v string) string
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
	v := &DXVault{
		Vendor:  vendor,
		Address: address,
		Token:   token,
		Prefix:  prefix,
		Path:    path,
	}
	return v
}

type DXHashicorpVault struct {
	DXVault
	Client *vault.Client
}

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

func (hv *DXHashicorpVault) ResolveAsString(v string) string {
	return hv.VaultMapString(&log.Log, v)
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
			log.Errorf("Unable to read credentials from Vault: %v", err)
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
			log.Fatalf("Unable to read credentials from Vault: %v", err)
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
