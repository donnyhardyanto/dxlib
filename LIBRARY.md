# dxlib — Library Reference

**Module:** `github.com/donnyhardyanto/dxlib`

This document lists every exported identifier in dxlib — types, constants, variables, and functions — with a brief description of purpose and how each relates to the rest of the library. Intended as a quick reference for AI agents and developers working with dxlib.

---

## Package Map

| Package | Import path | Purpose |
|---|---|---|
| `core` | `github.com/donnyhardyanto/dxlib/core` | Root context, signal handling, global flags |
| `errors` | `github.com/donnyhardyanto/dxlib/errors` | Stack-traced error wrapping (drop-in for pkg/errors) |
| `log` | `github.com/donnyhardyanto/dxlib/log` | Structured logging, file output, Telegram alerts |
| `utils` | `github.com/donnyhardyanto/dxlib/utils` | JSON type, type conversions, collections, network utils |
| `secure_memory` | `github.com/donnyhardyanto/dxlib/secure_memory` | Secure in-memory storage for secrets (memguard-backed) |
| `configuration` | `github.com/donnyhardyanto/dxlib/configuration` | JSON/YAML config files, dot-path access, sensitive masking |
| `base` | `github.com/donnyhardyanto/dxlib/base` | Database type enum shared by databases and tables packages |
| `databases` | `github.com/donnyhardyanto/dxlib/databases` | Multi-DB connectivity (PostgreSQL, MySQL/MariaDB, Oracle, MSSQL) |
| `tables` | `github.com/donnyhardyanto/dxlib/tables` | ORM-like table abstraction over databases |
| `redis` | `github.com/donnyhardyanto/dxlib/redis` | Redis client with pool configuration and OTel support |
| `api` | `github.com/donnyhardyanto/dxlib/api` | HTTP API server, endpoint routing, E2E encryption, WebSocket |
| `task` | `github.com/donnyhardyanto/dxlib/task` | Background task scheduler (once / always / none) |
| `app` | `github.com/donnyhardyanto/dxlib/app` | Application lifecycle — wires all subsystems, handles start/stop |
| `module` | `github.com/donnyhardyanto/dxlib/module` | Base type for extensible components |
| `object_storage` | `github.com/donnyhardyanto/dxlib/object_storage` | MinIO-based object storage client |
| `language` | `github.com/donnyhardyanto/dxlib/language` | Multi-language translation (id/en) |
| `time` | `github.com/donnyhardyanto/dxlib/time` | Timezone-aware day boundary calculations |
| `types` | `github.com/donnyhardyanto/dxlib/types` | API parameter type constants shared across packages |
| `state_diagram` | `github.com/donnyhardyanto/dxlib/state_diagram` | Finite state machine |
| `vault` | `github.com/donnyhardyanto/dxlib/vault` | HashiCorp Vault client for secrets |
| `otel` | `github.com/donnyhardyanto/dxlib/otel` | OpenTelemetry setup, metrics (histograms and counters) |
| `endpoint_rate_limiter` | `github.com/donnyhardyanto/dxlib/endpoint_rate_limiter` | Redis-backed per-endpoint rate limiting |
| `login_system` | `github.com/donnyhardyanto/dxlib/login_system` | Session and token management (Redis + DB) |
| `captcha` | `github.com/donnyhardyanto/dxlib/captcha` | CAPTCHA image generation |
| `sso` | `github.com/donnyhardyanto/dxlib/sso` | Single Sign-On organization and JWT/API key generation |
| `assets` | `github.com/donnyhardyanto/dxlib/assets` | Embedded static assets (CAPTCHA font) |
| `testing` | `github.com/donnyhardyanto/dxlib/testing` | HTTP test helpers |

---

## Integration Patterns

Before per-package details: the most common assembly pattern across all dxlib apps:

```
core (RootContext, signals)
  └── log (structured logging, uses core.RootContext)
       └── errors (stack-traced errors, used by log)
            └── configuration (reads config files, uses log + secure_memory)
                 └── databases / redis (connect from configuration, use log)
                      └── tables (wraps databases, ORM-style ops)
                           └── api (HTTP endpoints, uses tables + log + redis)
                                └── task (background workers, uses log + configuration)
                                     └── app (wires everything, exposes lifecycle hooks)
```

`secure_memory.Secure(value)` stores a secret in locked RAM and returns a `*SecureValue` token. `configuration.GetString(dotPath)` transparently resolves that token — the caller never sees the raw secret. This is the canonical pattern for API keys, DB passwords, and tokens.

---

## `dxlib` (root package)

**Import:** `github.com/donnyhardyanto/dxlib`

| Identifier | Kind | Description |
|---|---|---|
| `IsDebug` | `var bool` | Global debug flag. Set true when `app.DebugKey` env var matches `app.DebugValue`. Read by other packages to enable verbose output. |

---

## `core`

**Import:** `github.com/donnyhardyanto/dxlib/core`

Initializes the application root context and loads environment files. All other packages that need a base context use `core.RootContext`. Signal handling (SIGTERM/SIGINT) cancels `RootContext`, which propagates to every goroutine watching it.

| Identifier | Kind | Description |
|---|---|---|
| `RootContext` | `var context.Context` | Application root context. Cancelled on SIGTERM/SIGINT. Pass to goroutines so they shut down cleanly. |
| `RootContextCancel` | `var context.CancelFunc` | Cancels `RootContext`. Called by `app` on shutdown. |
| `IsNewRelicEnabled` | `var bool` | True when New Relic APM is configured. |
| `NewRelicApplication` | `var *newrelic.Application` | New Relic application instance, nil if disabled. |
| `NewRelicLicense` | `var string` | New Relic license key. |
| `IsOtelEnabled` | `var bool` | True when OpenTelemetry is configured. |

**Init behavior:** On package init, loads `.env`, `key.env`, and `run.env` files from the working directory (if present), then creates `RootContext` with OS signal cancellation.

---

## `errors`

**Import:** `github.com/donnyhardyanto/dxlib/errors`

Drop-in replacement for `github.com/pkg/errors`. Errors carry a lazy stack trace — captured at creation, printed only when formatted with `%+v`. Use instead of `fmt.Errorf` everywhere in dxlib.

| Identifier | Kind | Description |
|---|---|---|
| `Frame` | `type` | Single program counter frame. `Format(s, verb)` prints file:line or function name. |
| `StackTrace` | `type []Frame` | Stack of frames from innermost to outermost. `Format(s, verb)` prints full stack. |
| `New(message string) error` | `func` | Creates error with stack trace. |
| `Errorf(format string, args ...any) error` | `func` | Formatted error with stack trace. |
| `Wrap(err error, message string) error` | `func` | Wraps existing error with context message + stack trace. |
| `Wrapf(err error, format string, args ...any) error` | `func` | Formatted wrap with stack trace. |
| `WithStack(err error) error` | `func` | Adds stack trace to error without adding a message. |
| `WithMessage(err error, message string) error` | `func` | Adds message without new stack trace. |
| `WithMessagef(err error, format string, args ...any) error` | `func` | Formatted message without stack trace. |
| `Cause(err error) error` | `func` | Returns root cause by unwrapping all layers. |
| `Is`, `As`, `Unwrap`, `Join` | `func` | Re-exported from standard `errors` package. |

---

## `log`

**Import:** `github.com/donnyhardyanto/dxlib/log`

Structured logging on top of `log/slog`. Every log call includes a prefix and optional location. Severity-based: Trace → Debug → Info → Warn → Error → Fatal → Panic. Fatal and Panic call `os.Exit(1)`. Has an `OnError` hook for side-effects (file logging, Telegram alerts).

### Types

**`DXLogLevel`** — Severity enum.
| Constant | Description |
|---|---|
| `DXLogLevelTrace` | Finest detail — hot path tracing |
| `DXLogLevelDebug` | Development-time detail |
| `DXLogLevelInfo` | Normal operation events |
| `DXLogLevelWarn` | Recoverable problems |
| `DXLogLevelError` | Non-fatal errors |
| `DXLogLevelFatal` | Config/input errors that prevent startup |
| `DXLogLevelPanic` | Programming bugs — always exits |

**`DXLogFormat`** — Output format enum: `DXLogFormatText`, `DXLogFormatJSON`, `DXLogFormatSimple`.

**`DXLog`** — Logging context. Create one per goroutine/component via `NewLog()`.
| Field | Type | Description |
|---|---|---|
| `Context` | `context.Context` | Associated context |
| `Prefix` | `string` | Prepended to every log message from this logger |
| `RequestURL` | `string` | Current HTTP request URL (for API loggers) |
| `LastErrorLogId` | `int64` | ID of last error written to DB (for correlation) |
| `LastErrorLogUid` | `string` | UID of last error written to DB |

**`DXLog` methods:**
| Method | Description |
|---|---|
| `Trace(text)` / `Tracef(text, v...)` | Trace-level log |
| `Debug(text)` / `Debugf(text, v...)` | Debug-level log |
| `Info(text)` / `Infof(text, v...)` | Info-level log |
| `Warn(text)` / `Warnf(text, v...)` | Warn-level log |
| `WarnAndCreateError(text) error` | Logs at Warn and returns a new error |
| `WarnAndCreateErrorf(text, v...) error` | Formatted version |
| `Error(text, err)` / `Errorf(err, text, v...)` | Error-level log with error attached |
| `ErrorAndCreateErrorf(text, v...) error` | Logs at Error and returns a new error |
| `Fatal(text)` / `Fatalf(text, v...)` | Logs and calls `os.Exit(1)` |
| `FatalAndCreateErrorf(text, v...) error` | Returns error then exits |
| `Panic(location, err)` | Logs with stack, calls `os.Exit(1)` |
| `PanicAndCreateErrorf(location, text, v...) error` | Returns error then panics |
| `LogText(err, severity, location, text, v...)` | Low-level dispatcher — rarely called directly |

### Package-level variables and functions

| Identifier | Kind | Description |
|---|---|---|
| `Log` | `var DXLog` | Global logger. Use directly: `log.Log.Infof(...)`. |
| `Format` | `var DXLogFormat` | Current output format. |
| `OnError` | `var func(l *DXLog, errPrev error, severity DXLogLevel, location string, text string, stack string) error` | Called after every log write. Set to add side effects (file, Telegram). Chainable. |
| `ConsoleLogLevel` | `var slog.Level` | Minimum level shown on stdout. Default: `LevelTrace` (show everything). |
| `DXLogLevelAsString` | `var map[DXLogLevel]string` | Maps level enum to string ("TRACE", "DEBUG", etc.). |
| `LevelTrace` | `const slog.Level` | Custom slog level for Trace (below slog.Debug). |
| `LevelFatal` | `const slog.Level` | Custom slog level for Fatal (above slog.Error). |
| `LevelPanic` | `const slog.Level` | Custom slog level for Panic (above Fatal). |
| `NewLog(parentLog *DXLog, ctx context.Context, prefix string) DXLog` | `func` | Creates child logger. If `parentLog` has a prefix, it is prepended: `parent.Prefix + " | " + prefix`. |
| `SanitizeForPostgreSQL(s string) string` | `func` | Replaces null bytes and control characters with hex escapes, safe for PostgreSQL TEXT fields. |
| `SetFormatJSON()` | `func` | Switches output to structured JSON (default on init). |
| `SetFormatText()` | `func` | Switches output to key=value text format. |
| `SetFormatSimple()` | `func` | Switches output to `"YYYY-MM-DD HH:MM:SS Message"` — human-readable, no field labels. |
| `SetConsoleLogLevel(level slog.Level)` | `func` | Sets minimum console log level. Calls SetFormat* to reapply with new level. |
| `SetConsoleLogLevelFromString(levelStr string)` | `func` | Parses "TRACE"/"DEBUG"/"INFO"/"WARN"/"ERROR" and calls `SetConsoleLogLevel`. Invalid/empty defaults to INFO. |
| `SetFileLogger(directory string) error` | `func` | Enables daily rotating file logging to `directory/YYYY-MM-DD.log`. Hooks into `OnError`. |
| `SetTelegramBot(token string, chatIDs []string)` | `func` | Sends WARN+ log messages to Telegram chat IDs via raw HTTP POST (no extra dependency). Chains with existing `OnError`. |

---

## `utils`

**Import:** `github.com/donnyhardyanto/dxlib/utils`

General utility functions. The most important export is `JSON = map[string]any`, used as the standard untyped data map throughout dxlib.

### Type aliases

| Identifier | Description |
|---|---|
| `JSON = map[string]any` | Standard map type used everywhere in dxlib for untyped JSON data. |

### Variables

| Identifier | Description |
|---|---|
| `OverrideShowPasswordOnLog` | `var bool` — When true, disables sensitive field masking in log output. Default false. |

### Functions — JSON / type conversion

| Function | Description |
|---|---|
| `ArrayToJSON[T any](arr []T) (string, error)` | Marshals any slice to JSON string. |
| `StringsToJSON(arr []string) string` | Marshals string slice to JSON string. |
| `IntsToJSON(arr []int) string` | Marshals int slice to JSON string. |
| `Int64sToJSON(arr []int64) string` | Marshals int64 slice to JSON string. |
| `Float64sToJSON(arr []float64) string` | Marshals float64 slice to JSON string. |
| `Int64sToStrings(arr []int64) []string` | Converts int64 slice to string slice. |
| `StringToJSON(s string) (JSON, error)` | Unmarshals JSON string to `map[string]any`. |
| `JSONToString(v JSON) (string, error)` | Marshals map to JSON string. |
| `ConvertToInt64(value interface{}) (int64, error)` | Converts any numeric type to int64. |
| `ConvertToInt(value interface{}) (int, error)` | Converts any numeric type to int. |
| `ConvertToFloat32(value interface{}) (float32, error)` | Converts any numeric type to float32. |
| `ConvertToFloat64(value interface{}) (float64, error)` | Converts any numeric type to float64. |
| `GetVFromKV[T any](kv map[string]any, key string) (T, error)` | Generic type-safe value retrieval from map. |
| `GetStringFromKV(kv map[string]any, key string) (string, error)` | Gets string from map, returns error if missing or wrong type. |
| `GetInt64FromKV(kv map[string]any, key string) (int64, error)` | Gets int64 from map. |
| `GetBoolFromKV(kv map[string]any, key string) (bool, error)` | Gets bool from map. |
| `GetValueFromNestedMap(m map[string]any, dotPath string) (any, error)` | Retrieves value from nested map using dot-separated path (e.g. `"db.host"`). |
| `SetValueInNestedMap(m map[string]any, dotPath string, value any)` | Sets value in nested map using dot-separated path, creating intermediate maps as needed. |

### Functions — sensitive data masking

| Function | Description |
|---|---|
| `IsSensitiveField(fieldName string) bool` | Returns true if fieldName contains keywords like "password", "token", "secret", "key", "credential". |
| `MaskSensitiveValue(fieldName string, value interface{}) interface{}` | Returns `"********"` if field is sensitive, original value otherwise. |
| `MaskSensitiveDataInJSON(data JSON) JSON` | Deep-copies a `JSON` map, replacing sensitive values with `"********"`. Used by `configuration.FilterSensitiveData()`. |

### Functions — collections

| Function | Description |
|---|---|
| `TsIsContain[T comparable](arr []T, v T) bool` | Returns true if slice contains value. Generic version. |
| `StringsIsContain(arr []string, v string) bool` | Returns true if string slice contains value. |
| `Diff[T comparable](first []T, second []T) (included, missing []T)` | Returns elements in `first` that are also in `second` (included), and those only in `first` (missing). |
| `RemoveDuplicates[T comparable](slice []T) []T` | Returns new slice with duplicates removed, preserving order. |
| `FindCommonValues[K comparable, V comparable](arrays1, arrays2 []map[K]any, key K) []V` | Returns values of `key` that appear in both slices of maps. |

### Functions — network

| Function | Description |
|---|---|
| `GetAllMachineIP4s() []string` | Returns all IPv4 addresses of the local machine (excluding loopback). |
| `GetAllActualBindingAddress(configuredBindingAddress string) []string` | Expands `"0.0.0.0:port"` to all machine IPs; returns as-is for specific IPs. |
| `TCPIPPortCanConnect(ip string, port string) bool` | Returns true if TCP connection to ip:port succeeds within 1 second. |
| `TCPAddressCanConnect(address string) bool` | Returns true if TCP connection to `"host:port"` address succeeds. |

### Functions — miscellaneous

| Function | Description |
|---|---|
| `NowAsString() string` | Returns current UTC time as RFC3339 string. |
| `GetBuildTime() string` | Returns VCS build time from Go build info, or empty string if unavailable. |
| `AskForConfirmation(key1 string, key2 string) error` | Reads stdin, returns error if input does not match `key1` or `key2`. Used for CLI confirmation prompts. |

---

## `secure_memory`

**Import:** `github.com/donnyhardyanto/dxlib/secure_memory`

Stores sensitive data (API keys, passwords, tokens) in locked, non-swappable RAM using `memguard`. Integrates with `configuration.GetString()` via `SecureValue` — the config layer returns the raw string transparently, without the caller ever holding a pointer to the locked buffer.

### Types

**`DXSecureMemoryType`** — Storage variant enum.
| Constant | Description |
|---|---|
| `DXSecureMemoryTypeLockedBuffer` | Plaintext bytes in locked RAM (non-swappable). |
| `DXSecureMemoryTypeEnclave` | Bytes encrypted in RAM (decrypted on access). |

**`DXSecureMemory`** — Single entry. Rarely used directly; go through `Manager`.
| Field | Type | Description |
|---|---|---|
| `Owner` | `*DXSecureMemoryManager` | Parent manager |
| `Key` | `string` | Lookup key |
| `StorageType` | `DXSecureMemoryType` | Which variant |
| `LockedBuffer` | `*memguard.LockedBuffer` | Set if LockedBuffer type |
| `Enclave` | `*memguard.Enclave` | Set if Enclave type |

| Method | Description |
|---|---|
| `Get() ([]byte, error)` | Returns plaintext bytes (decrypts if Enclave). |
| `Destroy()` | Zeros and frees locked memory. |

**`DXSecureMemoryManager`** — Manages all secure entries.
| Method | Description |
|---|---|
| `Store(key string, data []byte) error` | Stores bytes as LockedBuffer. |
| `StoreEnclave(key string, data []byte) error` | Stores bytes as encrypted Enclave. |
| `StoreFromVault(v *vault.DXHashicorpVault, vaultKey string, secureMemoryKey string) error` | Fetches secret from HashiCorp Vault, stores as LockedBuffer. |
| `StoreEnclaveFromVault(v *vault.DXHashicorpVault, vaultKey string, secureMemoryKey string) error` | Fetches from Vault, stores as Enclave. |
| `Get(key string) ([]byte, error)` | Retrieves plaintext bytes. |
| `MustGet(key string) []byte` | Like `Get` but panics on miss. |
| `Exists(key string) bool` | Returns true if key is stored. |
| `Delete(key string)` | Destroys and removes entry. |
| `DestroyAll()` | Destroys all entries. Called by `app.Stop()`. |
| `Count() int` | Number of stored entries. |
| `Keys() []string` | List of all stored keys. |

**`SecureValue`** — Token that `configuration.GetString()` resolves transparently.
| Method | Description |
|---|---|
| `Resolve() (string, error)` | Returns the stored plaintext string. |
| `MarshalJSON() ([]byte, error)` | Always marshals as `"********"` to prevent accidental logging. |

### Variables

| Identifier | Description |
|---|---|
| `Manager` | `var DXSecureMemoryManager` — Global instance. Used by `Secure()` and `configuration`. |

### Functions

| Function | Description |
|---|---|
| `Secure(value string) *SecureValue` | Stores `value` in `Manager` as a LockedBuffer and returns a `*SecureValue` token. The token resolves back to the string via `Resolve()`. Use in configuration to store sensitive defaults. |

---

## `configuration`

**Import:** `github.com/donnyhardyanto/dxlib/configuration`

Loads JSON or YAML config files into `map[string]any` structures. Supports dot-path access (`GetString("db.host")`), sensitive data masking, and transparent `*SecureValue` resolution. `app` calls `configuration.Manager.Load()` during startup.

### Types

**`DXConfiguration`** — One configuration document (maps to one file or one in-memory block).
| Field | Type | Description |
|---|---|---|
| `Owner` | `*DXConfigurationManager` | Parent manager |
| `NameId` | `string` | Logical name used to retrieve this config |
| `Filename` | `string` | File path to load from |
| `FileFormat` | `string` | `"json"` or `"yaml"` |
| `MustExist` | `bool` | Fatal if file missing when true |
| `MustLoadFile` | `bool` | Whether to load from file on `Manager.Load()` |
| `Data` | `*utils.JSON` | Loaded data (merged from file + defaults) |
| `SensitiveDataKey` | `[]string` | Dot-path keys whose values are always masked in logs |

| Method | Description |
|---|---|
| `GetString(dotPath string) (string, error)` | Gets string value at dot-path. Transparently resolves `*SecureValue`. |
| `GetSecureString(dotPath string) (string, error)` | Alias for `GetString`. |
| `GetInt(dotPath string) (int, error)` | Gets int at dot-path. |
| `GetInt64(dotPath string) (int64, error)` | Gets int64 at dot-path. |
| `GetBool(dotPath string) (bool, error)` | Gets bool at dot-path. |
| `GetFloat64(dotPath string) (float64, error)` | Gets float64 at dot-path. |
| `GetStringFromSubMap(subMapKey, fieldKey string) (string, error)` | Shorthand for `GetString(subMapKey + "." + fieldKey)`. |
| `FilterSensitiveData() utils.JSON` | Returns deep copy with sensitive fields masked as `"********"`. |
| `ShowToLog()` | Logs the filtered (masked) config at Info level. |
| `AsString() string` | Returns full config as indented JSON string. |
| `AsNonSensitiveString() string` | Returns masked config as indented JSON string. |
| `LoadFromFile() error` | Reads and merges file into `Data`. Deep-merges: file values override defaults. |

**`DXConfigurationManager`** — Manages a named set of configurations.

| Method | Description |
|---|---|
| `NewConfiguration(nameId, filename, fileFormat string, mustExist, mustLoadFile bool, data utils.JSON, sensitiveDataKey []string) *DXConfiguration` | Creates and registers a new config. `data` provides default values; file values override on load. |
| `NewIfNotExistConfiguration(...)` | Same as `NewConfiguration` but updates existing entry's defaults if nameId already registered. |
| `GetConfigurationData(nameId string) (*utils.JSON, error)` | Returns raw data pointer for a registered config. |
| `Load() error` | Loads all configs with `MustLoadFile=true`. Called by `app` at startup. |
| `ShowToLog() error` | Calls `ShowToLog()` on every registered config. |
| `AsString() string` | Concatenates `AsString()` for all configs. |
| `AsNonSensitiveString() string` | Concatenates masked strings for all configs. |

### Variables

| Identifier | Description |
|---|---|
| `Manager` | `var DXConfigurationManager` — Global instance. All configs registered here. |

---

## `base`

**Import:** `github.com/donnyhardyanto/dxlib/base`

Database type enum shared by `databases` and `tables`.

### Types

**`DXDatabaseType`** — Enum.
| Constant | Description |
|---|---|
| `UnknownDatabaseType` | Unrecognized / not configured |
| `DXDatabaseTypePostgreSQL` | PostgreSQL (pgx driver) |
| `DXDatabaseTypeMariaDB` | MySQL / MariaDB (go-sql-driver) |
| `DXDatabaseTypeOracle` | Oracle (go-ora) |
| `DXDatabaseTypeSQLServer` | Microsoft SQL Server (go-mssqldb) |
| `DXDatabaseTypePostgresSQLV2` | PostgreSQL with alternate driver config |

| Method | Description |
|---|---|
| `String() string` | Returns human-readable name. |
| `IsValid() bool` | Returns true if not `UnknownDatabaseType`. |
| `Driver() string` | Returns the Go SQL driver name string. |

### Functions

| Function | Description |
|---|---|
| `StringToDXDatabaseType(v string) DXDatabaseType` | Parses `"postgresql"`, `"mariadb"`, etc. to enum. |
| `NormalizeDriverName(driverName string) string` | Maps `"mysql"` → `"mariadb"` for consistency. |

---

## `databases`

**Import:** `github.com/donnyhardyanto/dxlib/databases`

Multi-database connectivity via `jmoiron/sqlx`. Supports PostgreSQL (pgx), MySQL/MariaDB, Oracle, and MSSQL. Does **not** support SQLite. Provides connection pooling, transaction management, and query helpers.

### Types

**`DXDatabase`** — One database connection.
| Field | Type | Description |
|---|---|---|
| `NameId` | `string` | Logical name |
| `DatabaseType` | `base.DXDatabaseType` | Which DB engine |
| `Address` | `string` | `host:port` |
| `UserName`, `UserPassword`, `DatabaseName` | `string` | Connection credentials |
| `IsConnectAtStart` | `bool` | Connect during `Manager.ConnectAllAtStart()` |
| `MustConnected` | `bool` | Fatal if connection fails |
| `Connected` | `bool` | Current connection state |
| `Connection` | `*sqlx.DB` | Active connection (nil if not connected) |
| `PgxPool` | `*pgxpool.Pool` | PostgreSQL connection pool (PostgreSQL only) |

| Method | Description |
|---|---|
| `EnsureConnection() error` | Reconnects if disconnected. Called before every query. |
| `TransactionBegin(ctx context.Context, isolationLevel DXDatabaseTxIsolationLevel) (*DXDatabaseTx, error)` | Starts a transaction. |

**`DXDatabaseTx`** — Active transaction.
| Field | Description |
|---|---|
| `*sqlx.Tx` | Embedded sqlx transaction |
| `Database *DXDatabase` | Parent connection |
| `Log *log.DXLog` | Logger for this transaction |
| `Ctx context.Context` | Transaction context |

**`DXDatabaseManager`** — Manages multiple databases.
| Method | Description |
|---|---|
| `NewDatabase(nameId string, ...) *DXDatabase` | Creates and registers a database. |
| `LoadFromConfiguration(configurationNameId string) error` | Reads DB config from `configuration.Manager`. |
| `ConnectAllAtStart() error` | Connects all databases with `IsConnectAtStart=true`. |
| `ConnectAll() error` | Connects all registered databases. |
| `DisconnectAll() error` | Disconnects all databases. |

**`DXOrderByDirection`** — `"ASC"` or `"DESC"`.
Constants: `DXOrderByDirectionAsc`, `DXOrderByDirectionDesc`.

**`DXOrderByNullPlacement`** — `"FIRST"` or `"LAST"`.
Constants: `DXOrderByNullPlacementFirst`, `DXOrderByNullPlacementLast`.

**`DXDatabaseTxIsolationLevel`** — Transaction isolation.
Constants: `LevelDefault`, `LevelReadUncommitted`, `LevelReadCommitted`, `LevelWriteCommitted`, `LevelRepeatableRead`, `LevelSnapshot`, `LevelSerializable`, `LevelLinearizable`.

### Variables

| Identifier | Description |
|---|---|
| `Manager` | `var DXDatabaseManager` — Global instance. |

---

## `tables`

**Import:** `github.com/donnyhardyanto/dxlib/tables`

ORM-like table abstraction over `databases`. Provides CRUD operations, auto-generated queries, encrypted fields, and export (XLS/XLSX/CSV). Used by `api` endpoints for standard REST operations.

### Types

**`DXTableManager`** — Manages all registered tables.
| Field | Type | Description |
|---|---|---|
| `Tables` | `map[string]*DXTable` | Standard tables |
| `RawTables` | `map[string]*DXRawTable` | Raw SQL tables (less abstraction) |
| `AuditOnlyTables` | `map[string]*DXTableAuditOnly` | Audit-only tables (insert + select, no update/delete) |
| `StandardOperationResponsePossibility` | `map[string]*api.DXAPIEndPointResponsePossibilities` | Canned API response templates |

| Method | Description |
|---|---|
| `ConnectAll() error` | Links all registered tables to their `DXDatabase` connections. Called by `app.start()`. |

### Constants

| Constant | Description |
|---|---|
| `DXTableExportFormatXLS` | Export format: legacy Excel |
| `DXTableExportFormatXLSX` | Export format: modern Excel |
| `DXTableExportFormatCSV` | Export format: CSV |

### Standard API response variables

Pre-built response possibility sets for common CRUD endpoints:
| Variable | Description |
|---|---|
| `DXAPIEndPointResponsePossibilityCreate` | Standard create response (201 + error codes) |
| `DXAPIEndPointResponsePossibilityCreateByUid` | Create by UID response |
| `DXAPIEndPointResponsePossibilityRead` | Standard read response |
| `DXAPIEndPointResponsePossibilityUpdate` | Standard update response |
| `DXAPIEndPointResponsePossibilityDelete` | Standard delete response |

### Variables

| Identifier | Description |
|---|---|
| `Manager` | `var DXTableManager` — Global instance. |

---

## `redis`

**Import:** `github.com/donnyhardyanto/dxlib/redis`

Redis client wrapper using `go-redis/v8`. Supports ring topology, OTel instrumentation, and JSON-native get/set. Managed by `DXRedisManager`.

### Types

**`DXRedis`** — One Redis connection.
| Field | Type | Description |
|---|---|---|
| `NameId` | `string` | Logical name |
| `Address` | `string` | `host:port` |
| `UserName`, `Password` | `string` | Auth credentials |
| `HasUserName`, `HasPassword` | `bool` | Whether credentials are set |
| `DatabaseIndex` | `int` | Redis database number |
| `IsConnectAtStart` | `bool` | Auto-connect on startup |
| `MustConnected` | `bool` | Fatal if connection fails |
| `Connected` | `bool` | Current state |
| `Connection` | `*redis.Ring` | Active client |

| Method | Description |
|---|---|
| `Connect() error` | Opens connection. |
| `Disconnect() error` | Closes connection. |
| `Ping(ctx context.Context) error` | Tests connectivity. |
| `Set(ctx context.Context, key string, value utils.JSON, expirationDuration time.Duration) error` | Stores JSON value with TTL. |
| `Get(ctx context.Context, key string) (utils.JSON, error)` | Retrieves JSON value. Returns error if missing. |
| `GetEx(ctx context.Context, key string, duration time.Duration) (utils.JSON, error)` | Gets value and resets TTL. |
| `MustGet(ctx context.Context, key string) (utils.JSON, error)` | Like `Get` but logs fatal on miss. |
| `Delete(ctx context.Context, key string) error` | Deletes key. |
| `ApplyFromConfiguration() error` | Reads config from `configuration.Manager`. |

**`DXRedisManager`** — Manages multiple Redis instances.
| Method | Description |
|---|---|
| `NewRedis(nameId string, isConnectAtStart, mustConnected bool) *DXRedis` | Creates and registers instance. |
| `LoadFromConfiguration(configurationNameId string) error` | Reads Redis config block. |
| `ConnectAllAtStart() error` | Connects all with `IsConnectAtStart=true`. |
| `ConnectAll() error` | Connects all registered instances. |
| `DisconnectAll() error` | Disconnects all. |

### Variables

| Identifier | Description |
|---|---|
| `Manager` | `var DXRedisManager` — Global instance. |

---

## `api`

**Import:** `github.com/donnyhardyanto/dxlib/api`

HTTP API server. Endpoints are registered with parameter definitions and handler functions. Supports plain JSON, file upload/download streams, WebSocket, and E2E encryption (V1/V2/V3). Integrates with `tables` for standard CRUD, and with `log` for audit trails.

### Types

**`DXAPIEndPointType`** — Endpoint variant enum.
| Constant | Description |
|---|---|
| `EndPointTypeHTTPJSON` | Standard JSON request/response |
| `EndPointTypeHTTPUploadStream` | Multipart file upload |
| `EndPointTypeHTTPDownloadStream` | File download |
| `EndPointTypeHTTPDownloadStreamV2` | Chunked download variant |
| `EndPointTypeWS` | WebSocket |
| `EndPointTypeHTTPEndToEndEncryptionV1/V2/V3` | E2E encrypted variants |

**`DXAPIEndPointParameter`** — Declares one expected request parameter.
| Field | Type | Description |
|---|---|---|
| `NameId` | `string` | Parameter name |
| `Type` | `types.APIParameterType` | Expected type (string, int64, bool, json, etc.) |
| `Description` | `string` | Human-readable description |
| `IsMustExist` | `bool` | Returns 400 if missing |
| `IsNullable` | `bool` | Allows null value |
| `Children` | `[]DXAPIEndPointParameter` | Nested parameters for JSON objects |
| `Enum` | `[]any` | Valid values — returns 400 if value not in list |

| Method | Description |
|---|---|
| `PrintSpec(leftIndent int64) string` | Returns human-readable parameter spec. |

**`DXAPIUser`** — Authenticated user attached to a request.
| Field | Description |
|---|---|
| `Id`, `Uid` | Internal and public user identifiers |
| `LoginId` | User's login credential |
| `FullName` | Display name |
| `OrganizationId`, `OrganizationUid`, `OrganizationName` | Org membership |

**`DXAPIEndPointRequest`** — The request context passed to every endpoint handler.
| Field | Type | Description |
|---|---|---|
| `Id` | `string` | Unique request ID |
| `Context` | `context.Context` | Request-scoped context |
| `EndPoint` | `*DXAPIEndPoint` | Endpoint definition |
| `ParameterValues` | `map[string]*DXAPIEndPointRequestParameterValue` | Parsed parameters |
| `Log` | `log.DXLog` | Per-request logger |
| `Request` | `*http.Request` | Raw HTTP request |
| `RequestBodyAsBytes` | `[]byte` | Raw request body |
| `ResponseWriter` | `http.ResponseWriter` | Response writer |
| `ResponseStatusCode` | `int` | HTTP status to send |
| `ErrorMessage` | `[]string` | Accumulated error messages |
| `CurrentUser` | `DXAPIUser` | Authenticated user |
| `LocalData` | `utils.JSON` | Per-request scratch space for handler data |
| `ResponseHeaderSent` | `bool` | Whether headers have been flushed |
| `ResponseBodySent` | `bool` | Whether body has been written |
| `SuppressLogDump` | `bool` | When true, skips full request dump in logs |
| `WSClient` | `*websocket.Conn` | WebSocket connection (WebSocket endpoints only) |

| Method | Description |
|---|---|
| `GetParameterValues() utils.JSON` | Returns all parsed parameter values as a flat `utils.JSON` map. |
| `TranslateMessage(messageKey string) string` | Translates key using the user's detected language. |
| `TranslateMessageWithArgs(messageKey string, args ...any) string` | Formatted translation. |
| `RequestDump() ([]byte, error)` | Returns full HTTP request dump for logging. |

**`DXAPIAuditLogEntry`** — Audit record written per API call.
| Field | Description |
|---|---|
| `StartTime`, `EndTime` | Request duration |
| `IPAddress` | Client IP |
| `UserId`, `UserUid`, `UserLoginId`, `UserFullName` | User identity |
| `APIURL`, `APITitle`, `Method` | Endpoint details |
| `StatusCode`, `ErrorMessage` | Response outcome |

### Constants

| Constant | Description |
|---|---|
| `DXAPIDefaultWriteTimeoutSec = 300` | Default HTTP write timeout (5 min) |
| `DXAPIDefaultReadTimeoutSec = 300` | Default HTTP read timeout (5 min) |
| `UseResponseDataObject = true` | Whether responses wrap data in a `{data: ...}` envelope |

### Functions

| Function | Description |
|---|---|
| `LogExecutionTrace(ctx, phase, requestId, endpoint, method string, startTime time.Time, statusCode int, errMsg string)` | Writes a structured trace log entry for API execution phases. |
| `LogExecutionTraceWithStack(...)` | Same with stack trace attached. |

### Variables

| Identifier | Description |
|---|---|
| `Manager` | `var DXAPIManager` — Global API server manager. |

---

## `task`

**Import:** `github.com/donnyhardyanto/dxlib/task`

Background task scheduler. Tasks run in goroutines managed by `errgroup`. Integrates with `configuration.Manager` for per-task config (start_at, after_delay_sec). Started by `app` when a `"tasks"` config block exists.

### Types

**`DXTaskOnExecute`** — `type func(task *DXTask) error`. Handler signature for all tasks.

**`DXTask`** — One background task.
| Field | Type | Description |
|---|---|---|
| `NameId` | `string` | Logical name |
| `StartAt` | `string` | Execution mode: `"once"`, `"always"`, `"none"` |
| `AfterDelaySec` | `int64` | Seconds to sleep between `"always"` iterations |
| `OnExecute` | `DXTaskOnExecute` | The task function |
| `Log` | `log.DXLog` | Per-task logger |
| `RuntimeIsActive` | `bool` | True while running |
| `Context` | `context.Context` | Derived from manager context — cancelled on shutdown |
| `Cancel` | `context.CancelFunc` | Cancels this task's context |

| Method | Description |
|---|---|
| `ApplyConfigurations() error` | Reads `start_at` and `after_delay_sec` from `configuration.Manager["tasks"]`. |
| `StartAndWait(errorGroup *errgroup.Group) error` | Launches task goroutine and registers in errgroup. |
| `StartShutdown() error` | Calls `Cancel()` to stop a running task. |

**`DXTaskManager`** — Manages multiple tasks.
| Method | Description |
|---|---|
| `NewTask(nameId string, startAt string, afterDelaySec int64, onExecute DXTaskOnExecute) (*DXTask, error)` | Creates and registers a task. |
| `StartAll(errorGroup *errgroup.Group, errorGroupContext context.Context) error` | Starts all tasks and registers a shutdown listener goroutine. |
| `StopAll() error` | Signals all tasks to stop and waits. |

### Constants

| Constant | Description |
|---|---|
| `DXTaskDefaultAfterDelaySec = 1` | Default sleep between `"always"` iterations (1 second). |

### Variables

| Identifier | Description |
|---|---|
| `Manager` | `var DXTaskManager` — Global instance. Context derived from `core.RootContext`. |

---

## `app`

**Import:** `github.com/donnyhardyanto/dxlib/app`

Application lifecycle orchestrator. One `DXApp` per process. `app.Run()` calls lifecycle hooks in order: `OnDefine` → `OnDefineConfiguration` → load configs → connect databases/redis/api → `OnDefineSetVariables` → `OnDefineAPIEndPoints` → start API/tasks → `OnAfterConfigurationStartAll` → `OnExecute` → wait → `OnStopping` → stop everything.

### Types

**`DXApp`** — The application.
| Field | Type | Description |
|---|---|---|
| `NameId` | `string` | App identifier — used as log prefix and monitoring name |
| `Title` | `string` | Display name |
| `Description` | `string` | Human-readable description |
| `Version` | `string` | Version string — `BuildTime` injected via ldflags |
| `IsLoop` | `bool` | If true, app runs `OnExecute` in an infinite loop |
| `LoopInterval` | `time.Duration` | Sleep between loop iterations (IsLoop=true only) |
| `RuntimeErrorGroup` | `*errgroup.Group` | Goroutine group for all background workers |
| `RuntimeErrorGroupContext` | `context.Context` | Context for the errgroup |
| `LocalData` | `map[string]any` | Free-form scratch space for app-specific data |
| `IsRedisExist` | `bool` | Set true if `"redis"` config block found |
| `IsStorageExist` | `bool` | Set true if `"storage"` config block found |
| `IsObjectStorageExist` | `bool` | Set true if `"object_storage"` config block found |
| `IsAPIExist` | `bool` | Set true if `"api"` config block found |
| `IsTaskExist` | `bool` | Set true if `"tasks"` config block found |
| `OnDefine` | `DXAppEvent` | First hook — define structure before configuration |
| `OnDefineConfiguration` | `DXAppEvent` | Register configs with `configuration.Manager` |
| `OnDefineSetVariables` | `DXAppEvent` | Set package-level vars after config is loaded |
| `OnDefineAPIEndPoints` | `DXAppEvent` | Register HTTP endpoints |
| `OnAfterConfigurationStartAll` | `DXAppEvent` | Called after all subsystems have started |
| `OnExecute` | `DXAppEvent` | Main work — called once (or in loop if `IsLoop=true`) |
| `OnStopping` | `DXAppEvent` | Called before shutdown; save state here |
| `InitVault` | `*vault.DXHashicorpVault` | Optional Vault for init-time secrets |
| `EncryptionVault` | `*vault.DXHashicorpVault` | Optional Vault for encryption keys |

| Method | Description |
|---|---|
| `Run() error` | Entry point. Call from `main()`. Orchestrates full lifecycle. |
| `Stop() error` | Called internally on shutdown. Calls `OnStopping`, stops tasks, APIs, databases, Redis, destroys secure memory. |
| `SetupNewRelicApplication()` | Initializes New Relic APM from config. Called by `Set()`. |
| `SetupOpenTelemetry()` | Initializes OTel from config. Called by `Set()`. |

### Type aliases

| Alias | Description |
|---|---|
| `DXAppCallbackFunc = func() error` | General callback |
| `DXAppEvent = func() error` | Lifecycle hook signature |
| `DXAppArgCommandFunc = func(s *DXApp, ac *DXAppArgCommand, T any) error` | CLI command handler |
| `DXAppArgOptionFunc = func(s *DXApp, ac *DXAppArgOption, T any) error` | CLI option handler |

### Package-level functions and variables

| Identifier | Kind | Description |
|---|---|---|
| `App` | `var DXApp` | Global application instance. Use directly: `app.App.LocalData[...]`. |
| `BuildTime` | `var string` | Injected via `-ldflags "-X github.com/donnyhardyanto/dxlib/app.BuildTime=..."`. Appended to `App.Version`. |
| `Set(nameId, title, description string, isLoop bool, debugKey, debugValue string)` | `func` | Initializes `App` fields. Call before `App.Run()`. Also sets log prefix and initializes monitoring. |
| `GetNameId() string` | `func` | Returns `App.NameId`. |

---

## `module`

**Import:** `github.com/donnyhardyanto/dxlib/module`

Base type for extensible components (handlers, domain modules). Embed `DXModule` to inherit database association fields.

| Type | Field | Description |
|---|---|---|
| `DXModuleInterface` | — | Empty interface. Type-assert to check module behavior. |
| `DXModule` | `DXModuleInterface` | Embedded interface |
| | `NameId string` | Module identifier |
| | `DatabaseNameId string` | Primary database logical name |
| | `ReadOnlyDatabaseNameId string` | Read-only database logical name (for replicas) |

---

## `object_storage`

**Import:** `github.com/donnyhardyanto/dxlib/object_storage`

MinIO object storage client. Manages bucket connections. Integrates with `configuration.Manager`.

### Types

**`DXObjectStorageType`** — `UnknownObjectStorageType`, `Minio`.

**`DXObjectStorage`** — One storage connection.
| Field | Description |
|---|---|
| `NameId` | Logical name |
| `ObjectStorageType` | Storage backend type |
| `Address` | MinIO server address |
| `UserName`, `Password` | Auth credentials |
| `BasePath` | Path prefix for all operations |
| `UseSSL` | TLS connection |
| `BucketName` | Target bucket |
| `IsConnectAtStart`, `MustConnected` | Connection behavior |
| `Connected` | Current state |
| `Client *minio.Client` | Active MinIO client |

**`DXObjectStorageManager`** — Manages multiple storage instances. Methods parallel `DXDatabaseManager`: `NewObjectStorage`, `LoadFromConfiguration`, `ConnectAllAtStart`, `ConnectAll`, `DisconnectAll`.

### Functions

| Function | Description |
|---|---|
| `StringToDXObjectStorageType(v string) DXObjectStorageType` | Parses `"minio"` to enum. |

### Variables

| Identifier | Description |
|---|---|
| `Manager` | `var DXObjectStorageManager` — Global instance. |

---

## `language`

**Import:** `github.com/donnyhardyanto/dxlib/language`

Simple dictionary-based translation. Supports Indonesian (`id`) and English (`en`). Used by `api.DXAPIEndPointRequest.TranslateMessage()`.

### Types

| Type | Description |
|---|---|
| `DXLanguage` | String type for language code (`"id"`, `"en"`). |
| `DXTranslateFallbackMode` | What to return when translation key is missing. |

### Constants

| Constant | Description |
|---|---|
| `DXLanguageIndonesian` | `"id"` |
| `DXLanguageEnglish` | `"en"` |
| `DXTranslateFallbackModeOriginal` | Return the key as-is |
| `DXTranslateFallbackModeEmpty` | Return empty string |
| `DXTranslateFallbackModeTitleCase` | Return key in Title Case |

### Variables

| Identifier | Description |
|---|---|
| `DXLanguageDefault` | `var DXLanguage` — Defaults to `DXLanguageEnglish`. Override at startup. |
| `Dictionaries` | `var map[DXLanguage]map[string]string` — Preloaded translations. |

### Functions

| Function | Description |
|---|---|
| `LoadDictionary(lang DXLanguage, content string)` | Parses and loads a `key=value` dictionary for the given language. |
| `ParseDictionary(content string) map[string]string` | Parses newline-separated `key=value` pairs. |
| `Translate(key string, lang DXLanguage, fallback DXTranslateFallbackMode) string` | Returns translation for key, or applies fallback. |

---

## `time`

**Import:** `github.com/donnyhardyanto/dxlib/time`

Timezone-aware day boundary calculations. Useful for generating date-range queries in the correct local time.

### Functions

| Function | Description |
|---|---|
| `LocationStartOfDayInUTC(timezone string) (time.Time, error)` | Returns 00:00:00 of today in the given timezone, expressed as UTC. |
| `LocationEndOfDayInUTC(timezone string) (time.Time, error)` | Returns 23:59:59 of today in the given timezone, expressed as UTC. |

---

## `types`

**Import:** `github.com/donnyhardyanto/dxlib/types`

Type definitions for API parameter declarations. Used by `api.DXAPIEndPointParameter.Type`.

### Types

| Type | Description |
|---|---|
| `APIParameterType` | String type for parameter kind declarations. |
| `JSONType` | JSON schema type (string, number, boolean, object, array). |
| `GoType` | Go type representation. |
| `DataType` | Complete type mapping: API type → JSON type → Go type → DB-specific types. |

### Constants (selection of `APIParameterType` values)

| Constant | Description |
|---|---|
| `APIParameterTypeString` | String parameter |
| `APIParameterTypeInt64` | 64-bit integer |
| `APIParameterTypeFloat32` / `Float64` | Floating point |
| `APIParameterTypeBoolean` | Boolean |
| `APIParameterTypeEmail` | Email string (validated) |
| `APIParameterTypeJSON` | Nested JSON object |
| `APIParameterTypeArray` | JSON array |
| `APIParameterTypeDate` | Date string |
| `APIParameterTypeISO8601` | ISO 8601 datetime string |

---

## `state_diagram`

**Import:** `github.com/donnyhardyanto/dxlib/state_diagram`

Finite state machine. States connected by named actions. Records full action history.

### Types

**`State`** — `{ NameId string }`

**`StateConnection`** — One valid transition.
| Field | Description |
|---|---|
| `FromStateNameId` | Source state |
| `ToStateNameId` | Target state |
| `ActionNameId` | Trigger action name |

**`ActionHistory`** — Record of one transition that occurred.
| Field | Description |
|---|---|
| `At` | Timestamp |
| `ActionNameId` | Action that triggered this transition |
| `ActorNameId` | Who performed it |
| `FromStateNameId` | Previous state |
| `ToStateNameId` | New state |

**`StateDiagram`** — The state machine.
| Field | Description |
|---|---|
| `States []State` | All valid states |
| `Connections []StateConnection` | All valid transitions |
| `ActionHistory []ActionHistory` | Recorded history |
| `CurrentState string` | Current state name |

| Method | Description |
|---|---|
| `SetState(stateNameId string) error` | Sets current state without recording history. |
| `Action(actionNameId string, actorNameId string) error` | Executes named action from current state. Fails if transition is not defined. Records history. |
| `ActionTo(actionNameId string, actorNameId string, toStateNameId string) error` | Like `Action` but explicitly names the target state. |
| `GetState() string` | Returns current state name. |
| `GetStateAsStrings() []string` | Returns all state names as string slice. |
| `IsStateNameIdExist(s string) bool` | Returns true if state name is registered. |

### Functions

| Function | Description |
|---|---|
| `NewStateDiagram() *StateDiagram` | Creates empty state machine. |

---

## `vault`

**Import:** `github.com/donnyhardyanto/dxlib/vault`

HashiCorp Vault client for fetching secrets at startup. Used by `secure_memory.StoreFromVault()` and `app.InitVault`.

### Types

**`DXVault`** — Base vault config.
| Field | Description |
|---|---|
| `Vendor` | `"hashicorp"` |
| `Address` | Vault server URL |
| `Token` | Auth token |
| `Prefix` | Key prefix in Vault |
| `Path` | Base path in Vault |

**`DXHashicorpVault`** — HashiCorp Vault implementation. Embeds `DXVault`.
| Field | Description |
|---|---|
| `Client *vault.Client` | Active Vault client |

| Method | Description |
|---|---|
| `Start() error` | Connects to Vault server. |

### Functions

| Function | Description |
|---|---|
| `NewVaultVendor(vendor, address, token, prefix, path string) *DXVault` | Creates base vault config. |
| `NewHashiCorpVault(address, token, prefix, path string) *DXHashicorpVault` | Creates and returns a HashiCorp Vault client. Call `Start()` before use. |

---

## `otel`

**Import:** `github.com/donnyhardyanto/dxlib/otel`

OpenTelemetry setup and pre-built metrics. Initialized by `app.SetupOpenTelemetry()`. Metrics are used internally by `databases`, `redis`, and `api` packages for automatic instrumentation.

### Functions

| Function | Description |
|---|---|
| `SetupOpenTelemetry(serviceName string) error` | Initializes OTel trace and metric exporters (OTLP HTTP). Sets `core.IsOtelEnabled = true` on success. |
| `ShutdownOpenTelemetry() error` | Flushes pending traces and metrics. Called by `app.Stop()`. |
| `InitMetrics() error` | Creates all standard metric instruments (called internally by setup). |

### Metrics variables (used by other dxlib packages internally)

| Variable | Type | Description |
|---|---|---|
| `HTTPRequestDuration` | `metric.Float64Histogram` | HTTP server request duration in seconds |
| `HTTPRequestCount` | `metric.Int64Counter` | Total HTTP server requests |
| `DBQueryDuration` | `metric.Float64Histogram` | Database operation duration in seconds |
| `DBQueryCount` | `metric.Int64Counter` | Total database operations |
| `RedisOpDuration` | `metric.Float64Histogram` | Redis operation duration in seconds |
| `RedisOpCount` | `metric.Int64Counter` | Total Redis operations |
| `HTTPClientDuration` | `metric.Float64Histogram` | Outbound HTTP client request duration in seconds |
| `HTTPClientCount` | `metric.Int64Counter` | Total outbound HTTP client requests |

---

## `endpoint_rate_limiter`

**Import:** `github.com/donnyhardyanto/dxlib/endpoint_rate_limiter`

Redis-backed rate limiter per API endpoint group and user identifier.

### Types

**`RateLimitConfig`** — Limits for one group.
| Field | Description |
|---|---|
| `MaxAttempts int` | Allowed requests before blocking |
| `TimeWindow time.Duration` | Window over which attempts are counted |
| `BlockDuration time.Duration` | How long to block after limit exceeded |

**`EndpointRateLimiter`** — Rate limiter instance.
| Field | Description |
|---|---|
| `RedisInstance **DXRedis` | Pointer to Redis connection |
| `KeyPrefix string` | Redis key namespace |
| `Group map[string]RateLimitConfig` | Per-group configs |
| `DefaultConfig RateLimitConfig` | Fallback config for ungrouped endpoints |

| Method | Description |
|---|---|
| `RegisterGroup(groupNameId string, config RateLimitConfig)` | Registers a named group with its config. |
| `IsAllowed(ctx context.Context, groupNameId, identifier string) (bool, error)` | Returns true if identifier has remaining attempts. Decrements counter. |
| `Reset(ctx context.Context, groupNameId, identifier string) error` | Clears attempt count for identifier. |
| `GetRemainingAttempts(ctx context.Context, groupNameId, identifier string) (int, error)` | Returns remaining allowed attempts. |
| `ResetAll(ctx context.Context, groupNameId string) error` | Clears all identifiers in a group. |
| `GetBlockedStatus(ctx context.Context, groupNameId, identifier string) (bool, time.Duration, error)` | Returns whether blocked and remaining block duration. |

### Functions

| Function | Description |
|---|---|
| `NewEndpointRateLimiter(redisInstance **DXRedis, keyPrefix string, defaultConfig RateLimitConfig) *EndpointRateLimiter` | Creates a new rate limiter. |

---

## `login_system`

**Import:** `github.com/donnyhardyanto/dxlib/login_system`

Session/token management with flexible device and storage policies. Supports Redis-only, Redis+DB, or DB-only storage. Handles token expiry, device limits, and pub/sub notifications on session events.

### Types

**`DeviceInstanceType`** — Session multiplicity policy.
| Constant | Description |
|---|---|
| `SingleDeviceSingleInstancePerDevice` | One session per device, one device total |
| `SingleDeviceMultipleInstancePerDevice` | Multiple sessions per device, one device total |
| `MultipleDeviceSingleInstancePerDevice` | One session per device, multiple devices |
| `MultipleDeviceMultipleInstancePerDevice` | Multiple sessions on multiple devices |

**`StorageType`** — Where sessions are persisted.
| Constant | Description |
|---|---|
| `RedisOnly` | Sessions in Redis only |
| `RedisWithDB` | Sessions in Redis, durably backed to DB |
| `DBOnly` | Sessions in DB only |

**`TokenLifetimeType`** — Token expiry policy.
| Constant | Description |
|---|---|
| `ShortLived` | Sliding window — activity extends TTL |
| `LongLived` | Fixed expiry from creation time |

**`TokenRemoveReasonType`** — Why a session ended.
| Constant | Description |
|---|---|
| `LoggedOut` | User explicitly logged out |
| `Replaced` | New login replaced this session |
| `Expired` | TTL elapsed |
| `ServerError` | Internal error |
| `Closed` | Connection closed |
| `Evicted` | Redis eviction |

**`LoginSystem`** — Session manager instance.
| Field | Description |
|---|---|
| `TenantId` | Multi-tenant isolation key |
| `KeyPrefix` | Redis key namespace prefix |
| `Type` | `DeviceInstanceType` |
| `Storage` | `StorageType` |
| `TokenLifetime` | `TokenLifetimeType` |
| `ExpiredTimeDuration` | Session TTL |
| `Log` | Per-instance logger |
| `RedisClient`, `RedisDB` | Redis connections |
| `Db *databases.DXDatabase` | Optional database for durable storage |
| `SyncInterval` | How often to sync Redis→DB |

### Type aliases

| Alias | Description |
|---|---|
| `OnSessionRegisteredFunc = func(sessionKey string, sessionData map[string]any)` | Called when a new session is created |
| `OnSessionUnregisteredFunc = func(sessionKey string, reason TokenRemoveReasonType, reasonData any, sessionData map[string]any)` | Called when a session ends |

### Constants

| Constant | Description |
|---|---|
| `SessionStoreName = "runtime_currently_user_sessions"` | Redis key used for session storage |

### Functions

| Function | Description |
|---|---|
| `NewLoginSystem(tenantId int64, aType DeviceInstanceType, tokenLifetime TokenLifetimeType, ...) *LoginSystem` | Creates a new login system instance. |

---

## `captcha`

**Import:** `github.com/donnyhardyanto/dxlib/captcha`

CAPTCHA image generation. Font embedded from `assets.CaptchaFontBytes`.

### Types

**`ICaptcha`** — Interface.
| Method | Description |
|---|---|
| `GenerateImage(text string) ([]byte, error)` | Generates PNG image bytes for given text. |
| `GenerateID() (string, string)` | Returns a new CAPTCHA ID and its text. |

**`Captcha`** — Concrete implementation of `ICaptcha`.

### Functions

| Function | Description |
|---|---|
| `NewCaptcha() ICaptcha` | Creates and returns a `Captcha` instance. |

---

## `sso`

**Import:** `github.com/donnyhardyanto/dxlib/sso`

Single Sign-On management — organization configuration, JWT access tokens, and HMAC API keys. Integrates with `databases`, `redis`, and `configuration`.

### Types

**`DXOrganization`** — One SSO organization's config.
| Field | Description |
|---|---|
| `NameId` | Logical name |
| `Name` | Display name |
| `HMACSecret` | Secret for JWT signing and API key generation |
| `AuthenticationMethod` | Auth method identifier |
| `Applications utils.JSON` | Map of allowed applications |
| `Database` | Linked `*databases.DXDatabase` |
| `Redis` | Linked `*redis.DXRedis` |
| `AccessTokenTimeoutDurationSec` | JWT TTL in seconds |

| Method | Description |
|---|---|
| `ApplyData(d utils.JSON) error` | Populates fields from a config map. |

**`DXOrganizationsManager`** — Manages multiple organizations.
| Method | Description |
|---|---|
| `NewOrganization(nameid string) *DXOrganization` | Creates and registers an organization. |
| `GetValidOrganizationAndApplication(organizationNameId, applicationNameid string) (*DXOrganization, utils.JSON, error)` | Returns org and app config, error if either not found. |

### Functions

| Function | Description |
|---|---|
| `GenerateAccessToken(hmacSecret string) (string, error)` | Generates a signed JWT access token using the given HMAC secret. |
| `GenerateAPIKey(hmacSecret string) (string, error)` | Generates an HMAC-SHA512 API key. |

### Variables

| Identifier | Description |
|---|---|
| `OrganizationManager` | `var DXOrganizationsManager` — Global instance. |

---

## `assets`

**Import:** `github.com/donnyhardyanto/dxlib/assets`

Embedded static assets (Go embed).

| Variable | Description |
|---|---|
| `CaptchaFontBytes` | `var []byte` — TTF font embedded for CAPTCHA image generation. Used by `captcha.NewCaptcha()`. |

---

## `testing`

**Import:** `github.com/donnyhardyanto/dxlib/testing`

HTTP test helpers for integration tests. Not for production code.

### Functions

| Function | Description |
|---|---|
| `DoHTTPClientTest(t, mustSuccess, testName, method, url, contentType string, body []byte) *http.Response` | Makes HTTP request. Fails test if `mustSuccess=true` and status >= 400. |
| `ResponseBodyToJSON(t, r *http.Response) (utils.JSON, error)` | Parses HTTP response body as JSON. |
| `Style0HTTPClientTest(t, mustSuccess, testName, method, url, contentType string, body []byte) utils.JSON` | HTTP test — response has no `code` field. |
| `Style1HTTPClientTest(t, mustSuccess, testName, method, url, contentType string, body []byte) utils.JSON` | HTTP test — response has a `code` field. |
| `THTTPClient(t, mustStatusCode int, method, url, contentType, body string) string` | Simple HTTP test returning response body string. |

### Variables

| Identifier | Description |
|---|---|
| `Counter` | `var int` — Incrementing test counter for generating unique test data. |
