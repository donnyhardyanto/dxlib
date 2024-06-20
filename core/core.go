package core

import (
	"context"
	dxlib_os "dxlib/v3/utils/os"
	"os"
	"os/signal"
	"syscall"
)

var RootContext context.Context
var RootContextCancel context.CancelFunc

func init() {
	_ = dxlib_os.LoadEnvFile(`./run.env`)
	_ = dxlib_os.LoadEnvFile(`./key.env`)
	_ = dxlib_os.LoadEnvFile(`./.env`)
	RootContext, RootContextCancel = signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
}
