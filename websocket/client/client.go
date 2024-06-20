package client

import (
	"context"
	"dxlib/v3/core"
	"golang.org/x/sync/errgroup"
)

type DXWSClient struct {
	NameId string
}

type DXWSClientManager struct {
	Context           context.Context
	Cancel            context.CancelFunc
	WSClient          map[string]*DXWSClient
	ErrorGroup        *errgroup.Group
	ErrorGroupContext context.Context
}

var Manager DXWSClientManager

func init() {
	ctx, cancel := context.WithCancel(core.RootContext)
	Manager = DXWSClientManager{
		Context:  ctx,
		Cancel:   cancel,
		WSClient: map[string]*DXWSClient{},
	}
}
