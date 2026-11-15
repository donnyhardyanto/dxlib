package redis

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"strings"
	"time"
	_ "time/tzdata"

	dxlibv3Configuration "github.com/donnyhardyanto/dxlib/configuration"
	"github.com/donnyhardyanto/dxlib/core"
	"github.com/donnyhardyanto/dxlib/errors"
	"github.com/donnyhardyanto/dxlib/log"
	dxlibOtel "github.com/donnyhardyanto/dxlib/otel"
	"github.com/donnyhardyanto/dxlib/utils"
	json2 "github.com/donnyhardyanto/dxlib/utils/json"
	utilsTLS "github.com/donnyhardyanto/dxlib/utils/tls"
	"github.com/go-redis/redis/v8"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
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
	Connection       redis.UniversalClient // *Ring (single-node) or *Client (Sentinel) — both satisfy this
	Connected        bool
	Context          context.Context

	// Pool configuration
	PoolSize           int // Maximum number of socket connections (0 = go-redis default: 10 * runtime.NumCPU)
	MinIdleConns       int // Minimum number of idle connections to keep ready
	MaxConnAgeMinutes  int // Maximum connection lifetime in minutes before recycling (0 = no limit)
	IdleTimeoutMinutes int // Close idle connections after this many minutes (0 = go-redis default: 5 minutes)

	// Sentinel (HA). When SentinelMasterName != "" AND SentinelAddresses is non-empty,
	// Connect() builds NewFailoverClient (automatic master failover) instead of NewRing.
	// Empty = single-node (unchanged). Password (above) doubles as the data-node AUTH.
	SentinelMasterName string
	SentinelAddresses  []string
	SentinelPassword   string // AUTH to the sentinels themselves (optional)
	HasSentinel        bool

	// TLS. nil = plain TCP. Set directly when the struct is built by hand
	// (IsConfigured=true skips the configuration), or filled from the "tls"
	// block of the configuration, or from a block through SetTLSFromBlock.
	// Connect() applies it to the single-node Ring and, under Sentinel, to the
	// sentinels and the data nodes alike; a MinVersion below TLS 1.2 is raised
	// to TLS 1.2. An empty ServerName is filled per dial from the address's
	// host, so a certificate that names an IP needs that IP SAN or ServerName.
	TLSConfig *tls.Config
	// TLS is the parsed block behind TLSConfig when it came from a block; nil
	// when TLSConfig was set directly. Kept for its Summary() in the log.
	TLS *utilsTLS.DXClientTLS
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
		return errors.Errorf("CONFIGURATION_NOT_FOUND:%s", configurationNameId)
	}
	isConnectAtStart := false
	mustConnected := false
	for k, v := range *configuration.Data {
		d, ok := v.(utils.JSON)
		if !ok {
			err := log.Log.ErrorAndCreateErrorf("Cannot read %s as JSON", k)
			return errors.Wrap(err, "ERROR_IN_REDIS_MANAGER_LOAD_FROM_CONFIGURATION")
		}
		isConnectAtStart, ok = d["is_connect_at_start"].(bool)
		if !ok {
			isConnectAtStart = false
		}
		mustConnected, ok = d["must_connected"].(bool)
		if !ok {
			mustConnected = false
		}
		redisObject := rs.NewRedis(k, isConnectAtStart, mustConnected)
		err := redisObject.ApplyFromConfiguration()
		if err != nil {
			return errors.Wrap(err, "ERROR_IN_REDIS_MANAGER_LOAD_FROM_CONFIGURATION_APPLY_FROM_CONFIGURATION")
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
					return errors.Wrap(err, "ERROR_IN_REDIS_MANAGER_CONNECT_ALL_AT_START_CONNECT")
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
			return errors.Wrap(err, "ERROR_IN_REDIS_MANAGER_CONNECT_ALL_CONNECT")
		}
	}
	return nil

}

func (rs *DXRedisManager) DisconnectAll() (err error) {
	for _, v := range rs.Redises {
		err = v.Disconnect()
		if err != nil {
			return errors.Wrap(err, "ERROR_IN_REDIS_MANAGER_DISCONNECT_ALL_DISCONNECT")
		}
	}
	return nil
}

func (r *DXRedis) ApplyFromConfiguration() (err error) {
	if !r.IsConfigured {
		log.Log.Infof("Configuring to Redis %s... start", r.NameId)
		configurationData, ok := dxlibv3Configuration.Manager.Configurations["redis"]
		if !ok {
			err = log.Log.PanicAndCreateErrorf("DXRedis/ApplyFromConfiguration/1", "Redises configuration not found")
			return err
		}
		m := *(configurationData.Data)
		redisConfiguration, ok := m[r.NameId].(utils.JSON)
		if !ok {
			if r.MustConnected {
				err := log.Log.PanicAndCreateErrorf("DXRedis/ApplyConfigurations", "Redis %s configuration not found", r.NameId)
				return err
			} else {
				err := log.Log.WarnAndCreateErrorf("Manager is unusable, Redis %s configuration not found", r.NameId)
				return err
			}
		}
		r.Address, ok = redisConfiguration["address"].(string)
		if !ok {
			if r.MustConnected {
				err := log.Log.PanicAndCreateErrorf("DXRedis/ApplyConfigurations", "Mandatory address field in Redis %s configuration not exist", r.NameId)
				return err
			} else {
				err := log.Log.WarnAndCreateErrorf("configuration is unusable, mandatory address field in Redis %s configuration not exist", r.NameId)
				return err
			}
		}
		r.UserName, err = configurationData.GetStringFromSubMap(r.NameId, "user_name")
		r.HasUserName = (err == nil && r.UserName != "")
		if err != nil {
			r.UserName, r.HasUserName = redisConfiguration["user_name"].(string)
		}
		r.Password, err = configurationData.GetStringFromSubMap(r.NameId, "password")
		r.HasPassword = (err == nil && r.Password != "")
		if err != nil {
			r.Password, r.HasPassword = redisConfiguration["password"].(string)
		}
		r.DatabaseIndex, err = json2.GetInt(redisConfiguration, "database_index")
		if err != nil {
			if r.MustConnected {
				err := log.Log.PanicAndCreateErrorf("DXRedis/ApplyConfigurations", "Mandatory database_index field in Redis %s configuration not exist, check configuration and make sure it was integer not a string", r.NameId)
				return err
			} else {
				err := log.Log.WarnAndCreateErrorf("configuration is unusable, mandatory address field in Redis %s configuration not exist", r.NameId)
				return err
			}
		}

		// Pool configuration with defaults
		if v, ok := redisConfiguration["pool_size"].(int); ok {
			r.PoolSize = v
		} else {
			r.PoolSize = 0 // Default: 0 means go-redis default (10 * runtime.NumCPU)
		}
		if v, ok := redisConfiguration["min_idle_conns"].(int); ok {
			r.MinIdleConns = v
		} else {
			r.MinIdleConns = 0 // Default: 0 means go-redis default
		}
		if v, ok := redisConfiguration["max_conn_age_minutes"].(int); ok {
			r.MaxConnAgeMinutes = v
		} else {
			r.MaxConnAgeMinutes = 0 // Default: 0 means no max age
		}
		if v, ok := redisConfiguration["idle_timeout_minutes"].(int); ok {
			r.IdleTimeoutMinutes = v
		} else {
			r.IdleTimeoutMinutes = 0 // Default: 0 means go-redis default (5 minutes)
		}

		// Sentinel (HA) — optional. sentinel_addresses accepts a comma-separated string
		// (env) OR a JSON array. Empty master name / addresses => single-node (Ring).
		r.SentinelMasterName, _ = redisConfiguration["sentinel_master_name"].(string)
		switch sa := redisConfiguration["sentinel_addresses"].(type) {
		case string:
			for _, s := range strings.Split(sa, ",") {
				if s = strings.TrimSpace(s); s != "" {
					r.SentinelAddresses = append(r.SentinelAddresses, s)
				}
			}
		case []interface{}:
			for _, a := range sa {
				if s, ok := a.(string); ok {
					if s = strings.TrimSpace(s); s != "" {
						r.SentinelAddresses = append(r.SentinelAddresses, s)
					}
				}
			}
		case []string:
			for _, s := range sa {
				if s = strings.TrimSpace(s); s != "" {
					r.SentinelAddresses = append(r.SentinelAddresses, s)
				}
			}
		}
		r.SentinelPassword, _ = redisConfiguration["sentinel_password"].(string)
		r.HasSentinel = r.SentinelMasterName != "" && len(r.SentinelAddresses) > 0

		// TLS — optional block, the same keys as the API and HTTP client tls
		// blocks (utils/tls): tls-policy and ca-trust are required inside it.
		if tlsBlock, present := redisConfiguration["tls"]; present && tlsBlock != nil {
			tlsKV, isObject := tlsBlock.(utils.JSON)
			if !isObject {
				if r.MustConnected {
					return log.Log.PanicAndCreateErrorf("DXRedis/ApplyConfigurations", "TLS_CONFIG_ERROR:redis.%s.tls:WRONG_TYPE:%T:EXPECTED_OBJECT", r.NameId, tlsBlock)
				}
				return log.Log.WarnAndCreateErrorf("configuration is unusable, TLS_CONFIG_ERROR:redis.%s.tls:WRONG_TYPE:%T:EXPECTED_OBJECT", r.NameId, tlsBlock)
			}
			if err := r.SetTLSFromBlock(tlsKV); err != nil {
				if r.MustConnected {
					return log.Log.PanicAndCreateErrorf("DXRedis/ApplyConfigurations", "TLS_CONFIG_ERROR:redis.%s.tls/%v", r.NameId, err)
				}
				return log.Log.WarnAndCreateErrorf("configuration is unusable, TLS_CONFIG_ERROR:redis.%s.tls/%v", r.NameId, err)
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
			return errors.Wrapf(err, "Cannot configure to Redis %s to connect (%s)", r.NameId, err.Error())
		}
		var connection redis.UniversalClient
		if r.HasSentinel {
			log.Log.Infof("Connecting to Redis %s via Sentinel master=%q sentinels=%v db=%d %s... start",
				r.NameId, r.SentinelMasterName, r.SentinelAddresses, r.DatabaseIndex, r.tlsSummary())
			connection = redis.NewFailoverClient(r.failoverOptions())
			err = connection.Ping(r.Context).Err()
		} else {
			log.Log.Infof("Connecting to Redis %s at %s/%d %s... start", r.NameId, r.Address, r.DatabaseIndex, r.tlsSummary())
			// A go-redis v8 Ring starts a heartbeat goroutine that Close does
			// not stop, so the Ring is built only once a plain client with
			// the same address, credentials and TLS has answered: a failed
			// Connect then leaves nothing running (BUG-003).
			err = r.probe()
			if err == nil {
				connection = redis.NewRing(r.ringOptions())
				err = connection.Ping(r.Context).Err()
			}
		}
		log.Log.Infof("Pool config for Redis %s: PoolSize=%d, MinIdleConns=%d, MaxConnAge=%dm, IdleTimeout=%dm",
			r.NameId, r.PoolSize, r.MinIdleConns, r.MaxConnAgeMinutes, r.IdleTimeoutMinutes)
		if err != nil {
			if connection != nil {
				_ = connection.Close()
			}
			if r.MustConnected {
				log.Log.Fatalf("Cannot connect to Redis %s at %s/%d %s (%s)", r.NameId, r.Address, r.DatabaseIndex, r.tlsSummary(), err.Error())
				return nil
			} else {
				return errors.Wrapf(err, "Cannot connect to Redis %s at %s/%d %s (%s)", r.NameId, r.Address, r.DatabaseIndex, r.tlsSummary(), err.Error())
			}
		}
		r.Connection = connection
		r.Connected = true
		if r.HasSentinel {
			log.Log.Infof("Connecting to Redis %s via Sentinel master=%q db=%d... done CONNECTED", r.NameId, r.SentinelMasterName, r.DatabaseIndex)
		} else {
			log.Log.Infof("Connecting to Redis %s at %s/%d... done CONNECTED", r.NameId, r.Address, r.DatabaseIndex)
		}
	}
	return nil
}

// SetTLSFromBlock parses a tls block (the utils/tls client vocabulary:
// tls-policy, ca-trust, ca-files, cert-file, key-file, server-name, ...),
// loads its files and sets TLSConfig and TLS. A block with enabled=false is
// validated and leaves TLS off. For a struct built by hand as well as for the
// configuration path.
func (r *DXRedis) SetTLSFromBlock(kv utils.JSON) error {
	c, err := utilsTLS.NewClientTLS(kv)
	if err != nil {
		return err
	}
	r.TLS = c
	r.TLSConfig = c.Config // nil under enabled=false
	return nil
}

// effectiveTLSConfig is TLSConfig with its floor raised to TLS 1.2. The
// caller's config is cloned, never changed in place.
func (r *DXRedis) effectiveTLSConfig() *tls.Config {
	if r.TLSConfig == nil {
		return nil
	}
	if r.TLSConfig.MinVersion >= tls.VersionTLS12 {
		return r.TLSConfig
	}
	c := r.TLSConfig.Clone()
	c.MinVersion = tls.VersionTLS12
	if c.MaxVersion != 0 && c.MaxVersion < tls.VersionTLS12 {
		c.MaxVersion = tls.VersionTLS12
	}
	return c
}

// tlsSummary is the TLS posture as it goes into the connect log lines.
func (r *DXRedis) tlsSummary() string {
	switch {
	case r.TLSConfig == nil:
		return "tls=off"
	case r.TLS != nil:
		return "tls=on (" + r.TLS.Summary() + ")"
	default:
		c := r.effectiveTLSConfig()
		return "tls=on (tls.Config set directly, server-name=" + orURLHost(c.ServerName) + ", insecure-skip-verify=" + boolWord(c.InsecureSkipVerify) + ", min-version=" + tls.VersionName(c.MinVersion) + ")"
	}
}

func orURLHost(s string) string {
	if s == "" {
		return "<host of the address>"
	}
	return s
}

func boolWord(b bool) string {
	if b {
		return "true"
	}
	return "false"
}

func (r *DXRedis) ringOptions() *redis.RingOptions {
	o := &redis.RingOptions{
		Addrs:     map[string]string{"shard1": r.Address},
		DB:        r.DatabaseIndex,
		TLSConfig: r.effectiveTLSConfig(),
	}
	if r.HasUserName {
		o.Username = r.UserName
	}
	if r.HasPassword {
		o.Password = r.Password
	}
	if r.PoolSize > 0 {
		o.PoolSize = r.PoolSize
	}
	if r.MinIdleConns > 0 {
		o.MinIdleConns = r.MinIdleConns
	}
	if r.MaxConnAgeMinutes > 0 {
		o.MaxConnAge = time.Duration(r.MaxConnAgeMinutes) * time.Minute
	}
	if r.IdleTimeoutMinutes > 0 {
		o.IdleTimeout = time.Duration(r.IdleTimeoutMinutes) * time.Minute
	}
	return o
}

// probe pings the single node on a one-connection client that is closed
// before it returns, so nothing of it outlives the call.
func (r *DXRedis) probe() error {
	o := r.ringOptions()
	c := redis.NewClient(&redis.Options{
		Addr:      r.Address,
		DB:        o.DB,
		Username:  o.Username,
		Password:  o.Password,
		TLSConfig: o.TLSConfig,
		PoolSize:  1,
	})
	defer func() { _ = c.Close() }()
	return c.Ping(r.Context).Err()
}

func (r *DXRedis) failoverOptions() *redis.FailoverOptions {
	o := &redis.FailoverOptions{
		MasterName:    r.SentinelMasterName,
		SentinelAddrs: r.SentinelAddresses,
		DB:            r.DatabaseIndex,
		TLSConfig:     r.effectiveTLSConfig(), // go-redis uses it for the sentinels and the data nodes
	}
	if r.HasUserName {
		o.Username = r.UserName
	}
	if r.HasPassword {
		o.Password = r.Password // AUTH to the data nodes (master/replicas)
	}
	if r.SentinelPassword != "" {
		o.SentinelPassword = r.SentinelPassword // AUTH to the sentinels
	}
	if r.PoolSize > 0 {
		o.PoolSize = r.PoolSize
	}
	if r.MinIdleConns > 0 {
		o.MinIdleConns = r.MinIdleConns
	}
	if r.MaxConnAgeMinutes > 0 {
		o.MaxConnAge = time.Duration(r.MaxConnAgeMinutes) * time.Minute
	}
	if r.IdleTimeoutMinutes > 0 {
		o.IdleTimeout = time.Duration(r.IdleTimeoutMinutes) * time.Minute
	}
	return o
}

func (r *DXRedis) redisOtelStart(ctx context.Context, opName string) (context.Context, func(err error)) {
	if !core.IsOtelEnabled {
		return ctx, func(error) { /* no-op: OTel disabled */ }
	}
	ctx, s := otel.Tracer("dxlib.redis").Start(ctx, "redis."+opName,
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(attribute.String("peer.service", "redis")),
	)
	start := time.Now()
	attrs := metric.WithAttributes(
		attribute.String("db.system", "redis"),
		attribute.String("db.operation", opName),
		attribute.String("server.address", r.Address),
	)
	return ctx, func(err error) {
		dxlibOtel.RedisOpDuration.Record(ctx, time.Since(start).Seconds(), attrs)
		dxlibOtel.RedisOpCount.Add(ctx, 1, attrs)
		if err != nil {
			s.SetStatus(codes.Error, err.Error())
		}
		s.End()
	}
}

func (r *DXRedis) Ping(ctx context.Context) (err error) {
	ctx, endOtel := r.redisOtelStart(ctx, "PING")
	defer func() { endOtel(err) }()

	err = r.Connection.Ping(ctx).Err()
	if err != nil {
		return errors.Wrap(err, "ERROR_IN_REDIS_PING")
	}

	return nil
}

func (r *DXRedis) Set(ctx context.Context, key string, value utils.JSON, expirationDuration time.Duration) (err error) {
	ctx, endOtel := r.redisOtelStart(ctx, "SET")
	defer func() { endOtel(err) }()

	valueAsBytes, err := json.Marshal(value)
	if err != nil {
		return errors.Wrapf(err, "Cannot save to Redis %s k/v (%v) %s/%v", r.NameId, err, key, value)
	}

	err = r.Connection.Set(ctx, key, valueAsBytes, expirationDuration).Err()
	if err != nil {
		return errors.Wrapf(err, "Cannot save to Redis %s k/v (%v) %s/%v", r.NameId, err, key, value)
	}
	return nil
}

func (r *DXRedis) Get(ctx context.Context, key string) (value utils.JSON, err error) {
	ctx, endOtel := r.redisOtelStart(ctx, "GET")
	defer func() { endOtel(err) }()

	valueAsBytes, err := r.Connection.Get(ctx, key).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, nil
		}
		return nil, errors.Wrapf(err, "Cannot get to Redis %s k/v (%s) %s", r.NameId, err.Error(), key)
	}
	err = json.Unmarshal(valueAsBytes, &value)
	if err != nil {
		return nil, errors.Wrapf(err, "Cannot unmarshall from bytes in Redis %s k/v (%s) %s/%v", r.NameId, err.Error(), key, valueAsBytes)
	}
	return value, nil
}

func (r *DXRedis) GetEx(ctx context.Context, key string, duration time.Duration) (value utils.JSON, err error) {
	ctx, endOtel := r.redisOtelStart(ctx, "GETEX")
	defer func() { endOtel(err) }()

	valueAsBytes, err := r.Connection.GetEx(ctx, key, duration).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, nil
		}
		return nil, errors.Wrapf(err, "Cannot get to Redis %s k/v (%s) %s", r.NameId, err.Error(), key)
	}
	err = json.Unmarshal(valueAsBytes, &value)
	if err != nil {
		return nil, errors.Wrapf(err, "Cannot unmarshall from bytes in Redis %s k/v (%s) %s/%v", r.NameId, err.Error(), key, valueAsBytes)
	}
	return value, nil
}

func (r *DXRedis) MustGet(ctx context.Context, key string) (value utils.JSON, err error) {
	ctx, endOtel := r.redisOtelStart(ctx, "GET")
	defer func() { endOtel(err) }()

	valueAsBytes, err := r.Connection.Get(ctx, key).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, errors.Wrapf(err, "Cannot find keyin Redis %s (%s) %s", r.NameId, err.Error(), key)
		} else {
			return nil, errors.Wrapf(err, "Cannot get k/v to Redis %s k/v (%s) %s", r.NameId, err.Error(), key)
		}
	}
	err = json.Unmarshal(valueAsBytes, &value)
	if err != nil {
		return nil, errors.Wrapf(err, "Cannot unmarshall from bytes in Redis %s k/v (%s) %s/%v", r.NameId, err.Error(), key, valueAsBytes)
	}
	return value, nil
}

func (r *DXRedis) Delete(ctx context.Context, key string) (err error) {
	ctx, endOtel := r.redisOtelStart(ctx, "DEL")
	defer func() { endOtel(err) }()

	_, err = r.Connection.Del(ctx, key).Result()
	if err != nil {
		return errors.Wrapf(err, "Error in deleting key Redis %s k/v (%v) %s", r.NameId, err, key)
	}
	return nil
}

func (r *DXRedis) Disconnect() (err error) {
	if r.Connected {
		log.Log.Infof("Disconnecting to Redis %s at %s/%d... start", r.NameId, r.Address, r.DatabaseIndex)
		c := r.Connection
		err := c.Close()
		if err != nil {
			return errors.Wrapf(err, "Disconnecting to Redis %s at %s/%d error (%s)", r.NameId, r.Address, r.DatabaseIndex, err.Error())
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
