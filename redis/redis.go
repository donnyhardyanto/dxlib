package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/go-redis/redis/v8"
	"time"

	dxlibv3Configuration "github.com/donnyhardyanto/dxlib/configuration"
	"github.com/donnyhardyanto/dxlib/core"
	"github.com/donnyhardyanto/dxlib/log"
	"github.com/donnyhardyanto/dxlib/utils"
	json2 "github.com/donnyhardyanto/dxlib/utils/json"
)

type DXRedis struct {
	Owner            *DXRedisManager
	NameId           string
	IsConfigured     bool
	Address          string
	UserName         string
	HasUserName      bool
	Password         string
	HasPassword      bool
	DatabaseIndex    int
	IsConnectAtStart bool
	MustConnected    bool
	Connection       *redis.Ring
	Connected        bool
	Context          context.Context
}

type DXRedisManager struct {
	Redises map[string]*DXRedis
}

func (rs *DXRedisManager) NewRedis(nameId string, isConnectAtStart, mustConnected bool) *DXRedis {
	r := DXRedis{
		Owner:            rs,
		NameId:           nameId,
		IsConfigured:     false,
		IsConnectAtStart: isConnectAtStart,
		MustConnected:    mustConnected,
		Connected:        false,
		HasUserName:      false,
		HasPassword:      false,
		DatabaseIndex:    0,
		Context:          core.RootContext,
	}
	rs.Redises[nameId] = &r
	return &r
}

func (rs *DXRedisManager) LoadFromConfiguration(configurationNameId string) (err error) {
	configuration, ok := dxlibv3Configuration.Manager.Configurations[configurationNameId]
	if !ok {
		return fmt.Errorf("CONFIGURATION_NOT_FOUND:%s", configurationNameId)
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
		redisObject := rs.NewRedis(k, isConnectAtStart, mustConnected)
		err := redisObject.ApplyFromConfiguration()
		if err != nil {
			return err
		}
	}
	return nil
}

func (rs *DXRedisManager) ConnectAllAtStart() (err error) {
	if len(rs.Redises) > 0 {
		log.Log.Info("Connecting to Redis Manager... start")
		for _, v := range rs.Redises {
			if v.IsConnectAtStart {
				err = v.Connect()
				if err != nil {
					return err
				}
			}
		}
		log.Log.Info("Connecting to Redis Manager... done")
	}
	return nil
}

func (rs *DXRedisManager) ConnectAll() (err error) {
	for _, v := range rs.Redises {
		err = v.Connect()
		if err != nil {
			return err
		}
	}
	return nil

}

func (rs *DXRedisManager) DisconnectAll() (err error) {
	for _, v := range rs.Redises {
		err = v.Disconnect()
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *DXRedis) ApplyFromConfiguration() (err error) {
	if !r.IsConfigured {
		log.Log.Infof("Configuring to Redis %s... start", r.NameId)
		configurationData, ok := dxlibv3Configuration.Manager.Configurations[`redis`]
		if !ok {
			err = log.Log.PanicAndCreateErrorf("DXRedis/ApplyFromConfiguration/1", "Redises configuration not found")
			return err
		}
		m := *(configurationData.Data)
		redisConfiguration, ok := m[r.NameId].(utils.JSON)
		if !ok {
			if r.MustConnected {
				err := log.Log.PanicAndCreateErrorf("Redis %s configuration not found", r.NameId)
				return err
			} else {
				err := log.Log.WarnAndCreateErrorf("Manager is unusable, Redis %s configuration not found", r.NameId)
				return err
			}
		}
		r.Address, ok = redisConfiguration[`address`].(string)
		if !ok {
			if r.MustConnected {
				err := log.Log.PanicAndCreateErrorf("Mandatory address field in Redis %s configuration not exist", r.NameId)
				return err
			} else {
				err := log.Log.WarnAndCreateErrorf("configuration is unusable, mandatory address field in Redis %s configuration not exist", r.NameId)
				return err
			}
		}
		r.UserName, r.HasUserName = redisConfiguration[`user_name`].(string)
		r.Password, r.HasPassword = redisConfiguration[`password`].(string)
		r.DatabaseIndex, err = json2.GetInt(redisConfiguration, `database_index`)
		if err != nil {
			if r.MustConnected {
				err := log.Log.PanicAndCreateErrorf("Mandatory database_index field in Redis %s configuration not exist, check configuration and make sure it was integer not a string", r.NameId)
				return err
			} else {
				err := log.Log.WarnAndCreateErrorf("configuration is unusable, mandatory address field in Redis %s configuration not exist", r.NameId)
				return err
			}
		}
		r.IsConfigured = true
		log.Log.Infof("Configuring to Redis %s... done", r.NameId)
	}
	return nil
}

func (r *DXRedis) Connect() (err error) {
	if !r.Connected {
		err := r.ApplyFromConfiguration()
		if err != nil {
			log.Log.Errorf("Cannot configure to Redis %s to connect (%s)", r.NameId, err.Error())
			return err
		}
		log.Log.Infof("Connecting to Redis %s at %s/%d... start", r.NameId, r.Address, r.DatabaseIndex)
		redisRingOptions := &redis.RingOptions{
			Addrs: map[string]string{
				"shard1": r.Address,
			},
			DB: r.DatabaseIndex,
		}
		if r.HasUserName {
			redisRingOptions.Username = r.UserName
		}
		if r.HasPassword {
			redisRingOptions.Password = r.Password
		}
		connection := redis.NewRing(redisRingOptions)
		err = connection.Ping(r.Context).Err()
		if err != nil {
			if r.MustConnected {
				log.Log.Fatalf("Cannot connect to Redis %s at %s/%d (%s)", r.NameId, r.Address, r.DatabaseIndex, err.Error())
				return nil
			} else {
				log.Log.Errorf("Cannot connect to Redis %s at %s/%d (%s)", r.NameId, r.Address, r.DatabaseIndex, err.Error())
				return err
			}
		}
		r.Connection = connection
		r.Connected = true
		log.Log.Infof("Connecting to Redis %s at %s/%d... done CONNECTED", r.NameId, r.Address, r.DatabaseIndex)
	}
	return nil
}

func (r *DXRedis) Ping() (err error) {
	err = r.Connection.Ping(r.Context).Err()
	if err != nil {
		return err
	}

	return nil
}

func (r *DXRedis) Set(key string, value utils.JSON, expirationDuration time.Duration) (err error) {
	valueAsBytes, err := json.Marshal(value)
	if err != nil {
		log.Log.Errorf("Cannot save to Redis %s k/v (%v) %s/%v", r.NameId, err, key, value)
		return err
	}

	err = r.Connection.Set(r.Context, key, valueAsBytes, expirationDuration).Err()
	if err != nil {
		log.Log.Errorf("Cannot save to Redis %s k/v (%v) %s/%v", r.NameId, err, key, value)
		return err
	}
	return nil
}

func (r *DXRedis) Get(key string) (value utils.JSON, err error) {
	valueAsBytes, err := r.Connection.Get(r.Context, key).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, nil
		}
		log.Log.Errorf("Cannot get to Redis %s k/v (%s) %s", r.NameId, err.Error(), key)
		return nil, err
	}
	err = json.Unmarshal(valueAsBytes, &value)
	if err != nil {
		log.Log.Errorf("Cannot unmarshall from bytes in Redis %s k/v (%s) %s/%v", r.NameId, err.Error(), key, valueAsBytes)
		return nil, err
	}
	return value, nil
}

func (r *DXRedis) GetEx(key string, duration time.Duration) (value utils.JSON, err error) {
	valueAsBytes, err := r.Connection.GetEx(r.Context, key, duration).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, nil
		}
		log.Log.Errorf("Cannot get to Redis %s k/v (%s) %s", r.NameId, err.Error(), key)
		return nil, err
	}
	err = json.Unmarshal(valueAsBytes, &value)
	if err != nil {
		log.Log.Errorf("Cannot unmarshall from bytes in Redis %s k/v (%s) %s/%v", r.NameId, err.Error(), key, valueAsBytes)
		return nil, err
	}
	return value, nil
}
func (r *DXRedis) MustGet(key string) (value utils.JSON, err error) {
	valueAsBytes, err := r.Connection.Get(r.Context, key).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			log.Log.Errorf("Cannot find keyin Redis %s (%s) %s", r.NameId, err.Error(), key)
			return nil, err
		} else {
			log.Log.Errorf("Cannot get k/v to Redis %s k/v (%s) %s", r.NameId, err.Error(), key)
			return nil, err
		}
	}
	err = json.Unmarshal(valueAsBytes, &value)
	if err != nil {
		log.Log.Errorf("Cannot unmarshall from bytes in Redis %s k/v (%s) %s/%v", r.NameId, err.Error(), key, valueAsBytes)
		return nil, err
	}
	return value, nil
}

func (r *DXRedis) Delete(key string) (err error) {
	_, err = r.Connection.Del(r.Context, key).Result()
	if err != nil {
		log.Log.Errorf("Error in deleting key Redis %s k/v (%v) %s", r.NameId, err, key)
		return err
	}
	return nil
}

func (r *DXRedis) Disconnect() (err error) {
	if r.Connected {
		log.Log.Infof("Disconnecting to Redis %s at %s/%d... start", r.NameId, r.Address, r.DatabaseIndex)
		c := r.Connection
		err := c.Close()
		if err != nil {
			log.Log.Errorf("Disconnecting to Redis %s at %s/%d error (%s)", r.NameId, r.Address, r.DatabaseIndex, err.Error())
			return err
		}
		r.Connection = nil
		r.Connected = false
		log.Log.Infof("Disconnecting to Redis %s at %s/%d... done DISCONNECTED", r.NameId, r.Address, r.DatabaseIndex)
	}
	return nil
}

var Manager DXRedisManager

func init() {
	Manager = DXRedisManager{Redises: map[string]*DXRedis{}}
}
