package hashicorp_vault

import (
	"log"
	"strings"

	vault "github.com/hashicorp/vault/api"
)

type VaultServer struct {
	Address string
	Token   string
	Prefix  string
	Client  *vault.Client
}

func (v *VaultServer) Setup() *vault.Client {
	config := vault.DefaultConfig()
	config.Address = v.Address
	client, err := vault.NewClient(config)
	if err != nil {
		log.Fatalf("Unable to initialize Vault client: %v", err)
	}
	token := v.Token
	client.SetToken(token)
	v.Client = client
	return client
}

func (v *VaultServer) VaultMapping(path string, texts ...string) []string {
	check := false
	//prefix := "__VAULT__"
	for _, text := range texts {
		if strings.Contains(text, v.Prefix) {
			check = true
			break
		}
	}
	if check {
		secret, err := v.Client.Logical().Read(path)
		if err != nil {
			log.Fatalf("Unable to read credentials from Vault Mapping: %v", err)
		}
		var results []string
		data := secret.Data["data"].(map[string]any)
		for _, text := range texts {
			if strings.Contains(text, v.Prefix) {
				key := strings.TrimPrefix(text, v.Prefix)
				results = append(results, data[key].(string))
			} else {
				results = append(results, text)
			}
		}
		return results
	}
	return texts
}

func (v *VaultServer) VaultMapString(path string, text string) string {
	if strings.Contains(text, v.Prefix) {
		mapString := text
		secret, err := v.Client.Logical().Read(path)
		if err != nil {
			log.Fatalf("Unable to read credentials from Vault Mapping: %v", err)
		}
		data := secret.Data["data"].(map[string]any)
		for key, value := range data {
			placeholder := v.Prefix + key
			mapString = strings.Replace(mapString, placeholder, value.(string), -1)
		}
		return mapString
	}
	return text
}
