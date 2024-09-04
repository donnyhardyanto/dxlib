package app

import (
	"context"
	"dxlib/v3/object_storage"
	"dxlib/v3/vault"
	"fmt"
	"os"

	"golang.org/x/sync/errgroup"

	v3 "dxlib/v3"
	"dxlib/v3/api"
	"dxlib/v3/configuration"
	"dxlib/v3/core"
	"dxlib/v3/database"
	"dxlib/v3/log"
	"dxlib/v3/redis"
	"dxlib/v3/table"
	"dxlib/v3/task"
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
	nameId                   string
	Title                    string
	Description              string
	Version                  string
	Args                     DXAppArgs
	IsLoop                   bool
	RuntimeErrorGroup        *errgroup.Group
	RuntimeErrorGroupContext context.Context

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
}

func (a *DXApp) Run() (err error) {

	if a.InitVault != nil {
		err = a.InitVault.Start()
		if err != nil {
			log.Log.Error(err.Error())
			return err
		}
	}

	if a.OnDefine != nil {
		err := a.OnDefine()
		if err != nil {
			log.Log.Error(err.Error())
			return err
		}
	}
	if a.OnDefineConfiguration != nil {
		err := a.OnDefineConfiguration()
		if err != nil {
			log.Log.Error(err.Error())
			return err
		}
	}

	err = a.execute()
	if err != nil {
		log.Log.Error(err.Error())
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
		err := table.Manager.ConnectAll()
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
			log.Log.Error(err.Error())
			return err
		}
	}

	if a.OnDefineAPIEndPoints != nil {
		err = a.OnDefineAPIEndPoints()
		if err != nil {
			log.Log.Error(err.Error())
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
			return err
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

			//log.Log.Info("Stopped")
		}()
	}

	if a.OnExecute != nil {
		log.Log.Info("Starting")
		err = a.OnExecute()
		if err != nil {
			log.Log.Infof("onExecute error (%v)", err.Error())
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

var App DXApp

func Set(nameId, title, description string, isLoop bool, debugKey string, debugValue string) {
	App.nameId = nameId
	App.Title = title
	App.Description = description
	App.IsLoop = isLoop
	App.DebugKey = debugKey
	App.DebugValue = debugValue
	if App.DebugKey != "" {
		v3.IsDebug = os.Getenv(App.DebugKey) == App.DebugValue
	}
	log.Log.Prefix = nameId
}

func GetNameId() string {
	return App.nameId
}
func init() {
	App = DXApp{
		Args: DXAppArgs{
			Commands: map[string]*DXAppArgCommand{},
			Options:  map[string]*DXAppArgOption{},
		},
	}
}
