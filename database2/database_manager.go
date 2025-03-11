package database2

import (
	dxlibv3Configuration "github.com/donnyhardyanto/dxlib/configuration"
	"github.com/donnyhardyanto/dxlib/database/protected/db"
	"github.com/donnyhardyanto/dxlib/log"
	"github.com/donnyhardyanto/dxlib/utils"
)

type DXDatabaseSQLExpression = db.SQLExpression

type DXDatabaseManager struct {
	Databases map[string]*DXDatabase
	Scripts   map[string]*DXDatabaseScript
}

func (dm *DXDatabaseManager) NewDatabase(nameId string, isConnectAtStart, mustBeConnected bool) *DXDatabase {
	if dm.Databases[nameId] != nil {
		return dm.Databases[nameId]
	}
	dbSemaphore := make(chan struct{}, 10)

	d := DXDatabase{
		NameId:               nameId,
		IsConfigured:         false,
		IsConnectAtStart:     isConnectAtStart,
		MustConnected:        mustBeConnected,
		Connected:            false,
		ConcurrencySemaphore: dbSemaphore,
	}
	dm.Databases[nameId] = &d
	return &d
}

func (dm *DXDatabaseManager) LoadFromConfiguration(configurationNameId string) (err error) {
	configuration := dxlibv3Configuration.Manager.Configurations[configurationNameId]
	isConnectAtStart := false
	mustConnected := false
	for k, v := range *configuration.Data {
		d, ok := v.(utils.JSON)
		if !ok {
			err := log.Log.ErrorAndCreateErrorf("Cannot read %s as JSON", k)
			return err
		}
		isConnectAtStart, ok = d[`is_connect_at_start`].(bool)
		if !ok {
			isConnectAtStart = false
		}
		mustConnected, ok = d[`must_connected`].(bool)
		if !ok {
			mustConnected = false
		}
		databaseObject := dm.NewDatabase(k, isConnectAtStart, mustConnected)
		err = databaseObject.ApplyFromConfiguration()
		if err != nil {
			return err
		}
	}
	return nil
}

func (dm *DXDatabaseManager) ConnectAllAtStart() (err error) {
	if len(dm.Databases) > 0 {
		log.Log.Info("Connecting to Database Manager... start")
		for _, v := range dm.Databases {
			err := v.ApplyFromConfiguration()
			if err != nil {
				err = log.Log.ErrorAndCreateErrorf("Cannot configure to database %s to connect", v.NameId)
				return err
			}
			if v.IsConnectAtStart {
				err = v.Connect()
				if err != nil {
					return err
				}
			}
		}
		log.Log.Info("Connecting to Database Manager... done")
	}
	return err
}

func (dm *DXDatabaseManager) ConnectAll(configurationNameId string) (err error) {
	for _, v := range dm.Databases {
		err := v.ApplyFromConfiguration()
		if err != nil {
			err = log.Log.ErrorAndCreateErrorf("Cannot configure to database %s to connect", v.NameId)
			return err
		}
		err = v.Connect()
		if err != nil {
			return err
		}
	}
	return err
}

func (dm *DXDatabaseManager) DisconnectAll() (err error) {
	for _, v := range dm.Databases {
		err = v.Disconnect()
		if err != nil {
			return err
		}
	}
	return err
}

var Manager DXDatabaseManager

func init() {
	Manager = DXDatabaseManager{
		Databases: map[string]*DXDatabase{},
		Scripts:   map[string]*DXDatabaseScript{},
	}
}
