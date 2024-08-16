package vault

import (
	dxlibv3Valult "dxlib/v3/utils/vault"
)

type DXVaultInterface interface {
	Start()
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
	Client dxlibv3Valult.VaultServer
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

func (hv *DXHashicorpVault) Start() {
	hv.Client = dxlibv3Valult.VaultServer{
		Address: hv.Address,
		Token:   hv.Token,
	}
	hv.Client.Setup()
}

func (hv *DXHashicorpVault) ResolveAsString(v string) string {
	return hv.Client.VaultMapString(hv.Path, v)
}
