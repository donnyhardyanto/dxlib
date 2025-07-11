package manager

import (
	"context"
	"github.com/go-redis/redis/v8"
)

type DXRemoteProcessClient struct {
	Owner        *DXRemoteProcessManager
	NameId       string
	IsConfigured bool
	Address      string
	Connection   *redis.Ring
	Connected    bool
	Context      context.Context
}

type DXRemoteProcessManagerInstance struct {
	Owner   *DXRemoteProcessManager
	Address string
}

type DXRemoteProcessManager struct {
	Clients          map[string]*DXRemoteProcessClient
	ManagerInstances map[string]*DXRemoteProcessManagerInstance
}

func (m *DXRemoteProcessManagerInstance) Execute() {

}

var Manager DXRemoteProcessManager

func init() {
	Manager = DXRemoteProcessManager{Clients: map[string]*DXRemoteProcessClient{}}
}
