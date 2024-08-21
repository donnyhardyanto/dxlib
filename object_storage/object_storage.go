package object_storage

import (
	"context"
	dxlibv3Configuration "dxlib/v3/configuration"
	"dxlib/v3/core"
	"dxlib/v3/log"
	"dxlib/v3/utils"
	"fmt"
)

type DXObjectStorageType int64

const (
	UnknownObjectStorageType DXObjectStorageType = iota
	Minio
)

func (t DXObjectStorageType) String() string {
	switch t {
	case Minio:
		return "minio"
	default:
		return "unknown"
	}
}

func StringToDXObjectStorageType(v string) DXObjectStorageType {
	switch v {
	case "minio":
		return Minio
	default:
		return UnknownObjectStorageType
	}
}

type DXObjectStorage struct {
	Owner             *DXObjectStorageManager
	NameId            string
	ObjectStorageType DXObjectStorageType
	IsConfigured      bool
	Address           string
	UserName          string
	HasUserName       bool
	Password          string
	HasPassword       bool
	UseSSL            bool
	BucketName        string
	IsConnectAtStart  bool
	MustConnected     bool
	Connected         bool
	Context           context.Context
}

type DXObjectStorageManager struct {
	ObjectStoragees map[string]*DXObjectStorage
}

func (rs *DXObjectStorageManager) NewObjectStorage(nameId string, isConnectAtStart, mustConnected bool) *DXObjectStorage {
	r := DXObjectStorage{
		Owner:            rs,
		NameId:           nameId,
		IsConfigured:     false,
		IsConnectAtStart: isConnectAtStart,
		MustConnected:    mustConnected,
		Connected:        false,
		HasUserName:      false,
		HasPassword:      false,
		UseSSL:           false,
		Context:          core.RootContext,
	}
	rs.ObjectStoragees[nameId] = &r
	return &r
}

func (rs *DXObjectStorageManager) LoadFromConfiguration(configurationNameId string) (err error) {
	configuration, ok := dxlibv3Configuration.Manager.Configurations[configurationNameId]
	if !ok {
		return fmt.Errorf("configuration '%s' not found", configurationNameId)
	}
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
		ObjectStorageObject := rs.NewObjectStorage(k, isConnectAtStart, mustConnected)
		err := ObjectStorageObject.ApplyFromConfiguration()
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *DXObjectStorage) ApplyFromConfiguration() (err error) {
	if !r.IsConfigured {
		log.Log.Infof("Configuring to ObjectStorage %s... start", r.NameId)
		configurationData, ok := dxlibv3Configuration.Manager.Configurations[`ObjectStorage`]
		if !ok {
			err = log.Log.PanicAndCreateErrorf("DXObjectStorage/ApplyFromConfiguration/1", "ObjectStoragees configuration not found")
			return err
		}
		m := *(configurationData.Data)
		ObjectStorageConfiguration, ok := m[r.NameId].(utils.JSON)
		if !ok {
			if r.MustConnected {
				err := log.Log.PanicAndCreateErrorf("ObjectStorage %s configuration not found", r.NameId)
				return err
			} else {
				err := log.Log.WarnAndCreateErrorf("Manager is unusable, ObjectStorage %s configuration not found", r.NameId)
				return err
			}
		}
		r.Address, ok = ObjectStorageConfiguration[`address`].(string)
		if !ok {
			if r.MustConnected {
				err := log.Log.PanicAndCreateErrorf("Mandatory address field in ObjectStorage %s configuration not exist", r.NameId)
				return err
			} else {
				err := log.Log.WarnAndCreateErrorf("configuration is unusable, mandatory address field in ObjectStorage %s configuration not exist", r.NameId)
				return err
			}
		}
		r.UserName, r.HasUserName = ObjectStorageConfiguration[`user_name`].(string)
		r.Password, r.HasPassword = ObjectStorageConfiguration[`password`].(string)
		//r.DatabaseIndex, err = json2.GetInt(ObjectStorageConfiguration, `database_index`)
		if err != nil {
			if r.MustConnected {
				err := log.Log.PanicAndCreateErrorf("Mandatory database_index field in ObjectStorage %s configuration not exist, check configuration and make sure it was integer not a string", r.NameId)
				return err
			} else {
				err := log.Log.WarnAndCreateErrorf("configuration is unusable, mandatory address field in ObjectStorage %s configuration not exist", r.NameId)
				return err
			}
		}
		r.IsConfigured = true
		log.Log.Infof("Configuring to ObjectStorage %s... done", r.NameId)
	}
	return nil
}

var Manager DXObjectStorageManager

func init() {
	Manager = DXObjectStorageManager{ObjectStoragees: map[string]*DXObjectStorage{}}
}
