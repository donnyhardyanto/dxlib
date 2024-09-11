package core

import (
	"context"
	dxlibOs "dxlib/utils/os"
	"os"
	"os/signal"
	"syscall"
)

var RootContext context.Context
var RootContextCancel context.CancelFunc

func init() {
	_ = dxlibOs.LoadEnvFile(`./run.env`)
	_ = dxlibOs.LoadEnvFile(`./key.env`)
	_ = dxlibOs.LoadEnvFile(`./.env`)
	RootContext, RootContextCancel = signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
}
