package databases

import (
	dxlibv3Configuration "github.com/donnyhardyanto/dxlib/configuration"
	"github.com/donnyhardyanto/dxlib/log"
	"github.com/donnyhardyanto/dxlib/utils"
	"sync"
)

type DXDatabaseManager struct {
	// mu guards Databases. GetOrCreate is reached from request goroutines --
	// the first request for a tenant after a restart takes the create branch
	// while other requests are reading -- and a concurrent map read and write
	// is a fatal error in Go that no recover can catch.
	mu        sync.RWMutex
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
	dm.mu.Lock()
	defer dm.mu.Unlock()
	// Checked again under the write lock: two goroutines can both miss the
	// read above, and the loser must not replace a handle the winner has
	// already handed out and possibly connected.
	if existing, exists := dm.Databases[nameId]; exists {
		return existing
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
		isConnectAtStart, ok = d["is_connect_at_start"].(bool)
		if !ok {
			isConnectAtStart = false
		}
		mustConnected, ok = d["must_connected"].(bool)
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
				err = log.Log.ErrorAndCreateErrorf("Cannot configure to databases %s to connect", v.NameId)
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
			err = log.Log.ErrorAndCreateErrorf("Cannot configure to databases %s to connect", v.NameId)
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

// Get returns an already-registered database, or nil. Unlike GetOrCreate it
// registers nothing, which is what a caller wants when a missing entry means
// "not configured yet" rather than "make me one".
func (dm *DXDatabaseManager) Get(nameId string) *DXDatabase {
	dm.mu.RLock()
	defer dm.mu.RUnlock()
	return dm.Databases[nameId]
}

// GetOrCreate gets an existing databases or creates a new one with default settings
func (dm *DXDatabaseManager) GetOrCreate(nameId string) *DXDatabase {
	dm.mu.RLock()
	d, exists := dm.Databases[nameId]
	dm.mu.RUnlock()
	if exists {
		return d
	}
	return dm.NewDatabase(nameId, false, false)
}

func (dm *DXDatabaseManager) NewDatabaseScript(nameId string, files []string) *DXDatabaseScript {
	ds := DXDatabaseScript{
		Owner:  dm,
		NameId: nameId,
		Files:  files,
	}
	dm.Scripts[nameId] = &ds
	return &ds
}

var Manager DXDatabaseManager

func init() {
	Manager = DXDatabaseManager{
		Databases: map[string]*DXDatabase{},
		Scripts:   map[string]*DXDatabaseScript{},
	}
}
