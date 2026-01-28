package app

import (
	"context"
	"fmt"

	"github.com/donnyhardyanto/dxlib"
	"github.com/donnyhardyanto/dxlib/database"
	"github.com/donnyhardyanto/dxlib/errors"
	"github.com/donnyhardyanto/dxlib/object_storage"
	"github.com/donnyhardyanto/dxlib/secure_memory"
	"github.com/donnyhardyanto/dxlib/tables"
	"github.com/donnyhardyanto/dxlib/vault"
	"github.com/newrelic/go-agent/v3/newrelic"

	"golang.org/x/sync/errgroup"

	"github.com/donnyhardyanto/dxlib/api"
	"github.com/donnyhardyanto/dxlib/configuration"
	"github.com/donnyhardyanto/dxlib/core"
	"github.com/donnyhardyanto/dxlib/log"
	"github.com/donnyhardyanto/dxlib/redis"
	"github.com/donnyhardyanto/dxlib/task"
	"github.com/donnyhardyanto/dxlib/utils/os"
)

type DXAppArgCommandFunc func(s *DXApp, ac *DXAppArgCommand, T any) (err error)

type DXAppArgCommand struct {
	name     string
	command  string
	callback *DXAppArgCommandFunc
}

type DXAppArgOptionFunc func(s *DXApp, ac *DXAppArgOption, T any) (err error)

type DXAppArgOption struct {
	name     string
	option   string
	callback *DXAppArgOptionFunc
}

type DXAppArgs struct {
	Commands map[string]*DXAppArgCommand
	Options  map[string]*DXAppArgOption
}

type DXAppCallbackFunc func() (err error)
type DXAppEvent func() (err error)

type DXApp struct {
	NameId                   string
	Title                    string
	Description              string
	Version                  string
	Args                     DXAppArgs
	IsLoop                   bool
	RuntimeErrorGroup        *errgroup.Group
	RuntimeErrorGroupContext context.Context
	LocalData                map[string]any

	IsRedisExist         bool
	IsStorageExist       bool
	IsObjectStorageExist bool
	IsAPIExist           bool
	IsTaskExist          bool

	DebugKey                     string
	DebugValue                   string
	OnDefine                     DXAppEvent
	OnDefineConfiguration        DXAppEvent
	OnDefineSetVariables         DXAppEvent
	OnDefineAPIEndPoints         DXAppEvent
	OnAfterConfigurationStartAll DXAppEvent
	OnExecute                    DXAppEvent
	OnStartStorageReady          DXAppEvent
	OnStopping                   DXAppEvent
	InitVault                    vault.DXVaultInterface
	EncryptionVault              vault.DXVaultInterface
}

func (a *DXApp) Run() (err error) {

	if a.InitVault != nil {
		err = a.InitVault.Start()
		if err != nil {
			log.Log.Error(err.Error(), err)
			return err
		}
	}

	if a.EncryptionVault != nil {
		err = a.EncryptionVault.Start()
		if err != nil {
			log.Log.Error(err.Error(), err)
			return err
		}
	}

	if a.OnDefine != nil {
		err := a.OnDefine()
		if err != nil {
			log.Log.Error(err.Error(), err)
			return errors.Wrap(err, "ERROR_ON_DEFINE_CALLBACK")
		}
	}
	if a.OnDefineConfiguration != nil {
		err := a.OnDefineConfiguration()
		if err != nil {
			log.Log.Error(err.Error(), err)
			return errors.Wrap(err, "ERROR_ON_DEFINE_CONFIGURATION_CALLBACK")
		}
	}

	err = a.execute()
	if err != nil {
		log.Log.Error(err.Error(), err)
		return err
	}
	return nil
}

func (a *DXApp) loadConfiguration() (err error) {
	err = configuration.Manager.Load()
	if err != nil {
		return err
	}
	_, a.IsRedisExist = configuration.Manager.Configurations["redis"]
	if a.IsRedisExist {
		err = redis.Manager.LoadFromConfiguration("redis")
		if err != nil {
			return err
		}
	}
	_, a.IsStorageExist = configuration.Manager.Configurations["storage"]
	if a.IsStorageExist {
		err = database.Manager.LoadFromConfiguration("storage")
		if err != nil {
			return err
		}
	}
	_, a.IsObjectStorageExist = configuration.Manager.Configurations["object_storage"]
	if a.IsObjectStorageExist {
		err = object_storage.Manager.LoadFromConfiguration("object_storage")
		if err != nil {
			return err
		}
	}
	_, a.IsAPIExist = configuration.Manager.Configurations["api"]
	if a.IsAPIExist {
		err = api.Manager.LoadFromConfiguration("api")
		if err != nil {
			return err
		}
	}
	return nil
}
func (a *DXApp) start() (err error) {
	log.Log.Info(fmt.Sprintf("%v %v %v", a.Title, a.Version, a.Description))
	err = a.loadConfiguration()
	if err != nil {
		return err
	}

	if a.IsRedisExist {
		err = redis.Manager.ConnectAllAtStart()
		if err != nil {
			return err
		}
	}
	if a.IsStorageExist {
		err = database.Manager.ConnectAllAtStart()
		if err != nil {
			return err
		}
		err = tables.Manager.ConnectAll()
		if err != nil {
			return err
		}
		if a.OnStartStorageReady != nil {
			err = a.OnStartStorageReady()
			if err != nil {
				return err
			}
		}
	}
	if a.IsObjectStorageExist {
		err = object_storage.Manager.ConnectAllAtStart()
		if err != nil {
			return err
		}
	}

	if a.OnDefineSetVariables != nil {
		err = a.OnDefineSetVariables()
		if err != nil {
			log.Log.Error(err.Error(), err)
			return err
		}
	}

	if a.OnDefineAPIEndPoints != nil {
		err = a.OnDefineAPIEndPoints()
		if err != nil {
			log.Log.Error(err.Error(), err)
			return err
		}
	}

	if a.IsAPIExist {
		err = api.Manager.StartAll(a.RuntimeErrorGroup, a.RuntimeErrorGroupContext)
		if err != nil {
			return err
		}
	}

	_, a.IsTaskExist = configuration.Manager.Configurations["tasks"]

	if a.IsTaskExist {
		err = task.Manager.StartAll(a.RuntimeErrorGroup, a.RuntimeErrorGroupContext)
		if err != nil {
			return err
		}
	}

	if a.OnAfterConfigurationStartAll != nil {
		err = a.OnAfterConfigurationStartAll()
		if err != nil {
			return err
		}
	}

	return nil
}

func (a *DXApp) Stop() (err error) {
	log.Log.Info("Stopping")
	if a.OnStopping != nil {
		err := a.OnStopping()
		if err != nil {
			return errors.Wrap(err, "ERROR_ON_STOPPING_CALLBACK")
		}
	}
	if a.IsTaskExist {
		err = task.Manager.StopAll()
		if err != nil {
			return err
		}
	}
	if a.IsAPIExist {
		err = api.Manager.StopAll()
		if err != nil {
			return err
		}
	}
	if a.IsRedisExist {
		err = redis.Manager.DisconnectAll()
		if err != nil {
			return err
		}
	}
	if a.IsStorageExist {
		err = database.Manager.DisconnectAll()
		if err != nil {
			return err
		}
	}
	if a.IsObjectStorageExist {
		err = object_storage.Manager.DisconnectAll()
		if err != nil {
			return err
		}
	}
	// Destroy all secure memory (keys, sensitive data)
	secure_memory.Manager.DestroyAll()
	log.Log.Info("Stopped")
	return nil
}

func (a *DXApp) execute() (err error) {
	defer core.RootContextCancel()
	a.RuntimeErrorGroup, a.RuntimeErrorGroupContext = errgroup.WithContext(core.RootContext)
	err = a.start()
	if err != nil {
		return err
	}
	if a.IsLoop {
		defer func() {
			err2 := a.Stop()
			if err2 != nil {
				log.Log.Infof("Error in Stopping.Stop(): (%v)", err2.Error())
			}
			log.Log.Info("Stopping...")
		}()
	}

	if a.OnExecute != nil {
		log.Log.Info("Starting")
		err = a.OnExecute()
		if err != nil {
			log.Log.Infof("onExecute error %+v", err)
			return err
		}
	}

	if a.IsLoop {
		log.Log.Info("Waiting...")
		err = a.RuntimeErrorGroup.Wait()
		if err != nil {
			log.Log.Infof("Exit reason: %v", err.Error())
			return err
		}
	}
	return nil
}

func (a *DXApp) SetupNewRelicApplication() {
	var err error
	if core.IsNewRelicEnabled {
		core.NewRelicApplication, err = newrelic.NewApplication(
			newrelic.ConfigAppName(App.NameId),
			newrelic.ConfigLicense(core.NewRelicLicense),
			newrelic.ConfigDistributedTracerEnabled(true),
		)
		if err != nil {
			log.Log.Panic("New Relic Application Error: ", err)
		}
	}
	return
}

var App DXApp

func Set(nameId, title, description string, isLoop bool, debugKey string, debugValue string) {
	App.NameId = nameId
	App.Title = title
	App.Description = description
	App.IsLoop = isLoop
	App.DebugKey = debugKey
	App.DebugValue = debugValue
	if App.DebugKey != "" {
		dxlib.IsDebug = os.GetEnvDefaultValue(App.DebugKey, "") == App.DebugValue
	}
	log.Log.Prefix = nameId
	App.SetupNewRelicApplication()
}

func GetNameId() string {
	return App.NameId
}

func init() {
	App = DXApp{
		Args: DXAppArgs{
			Commands: map[string]*DXAppArgCommand{},
			Options:  map[string]*DXAppArgOption{},
		},
		LocalData: map[string]any{},
	}
}
