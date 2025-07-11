package core

import (
	"context"
	dxlibOs "github.com/donnyhardyanto/dxlib/utils/os"
	"github.com/newrelic/go-agent/v3/newrelic"
	"os"
	"os/signal"
	"syscall"
)

var RootContext context.Context
var RootContextCancel context.CancelFunc
var IsNewRelicEnabled = false
var NewRelicApplication *newrelic.Application
var NewRelicLicense = ""

func init() {
	_ = dxlibOs.LoadEnvFile("./run.env")
	_ = dxlibOs.LoadEnvFile("./key.env")
	_ = dxlibOs.LoadEnvFile("./.env")
	IsNewRelicEnabled = dxlibOs.GetEnvDefaultValue("NEW_RELIC_ENABLED", "false") == "true"
	NewRelicLicense = dxlibOs.GetEnvDefaultValue("NEW_RELIC_LICENSE", "")
	RootContext, RootContextCancel = signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
}
