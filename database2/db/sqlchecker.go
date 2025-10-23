package db

import (
	"fmt"
	"regexp"
	"strings"
	"time"
	_ "time/tzdata"

	"github.com/pkg/errors"
	"github.com/shopspring/decimal"
)

var AllowRisk = false

// Common SQL injection patterns

// Regular expressions for unquoted identifiers by database type
var (
	identifierPatterns = map[DXDatabaseType]*regexp.Regexp{
		PostgreSQL: regexp.MustCompile("^[a-zA-Z_][a-zA-Z0-9_]*$"),
		MariaDB:    regexp.MustCompile("^[a-zA-Z0-9_$]+$"),
		SQLServer:  regexp.MustCompile("^[a-zA-Z@#_][a-zA-Z0-9@#_$]*$"),
		Oracle:     regexp.MustCompile("^[a-zA-Z][a-zA-Z0-9_$#]*$"),
	}

	// QuoteCharacters defines the start and end quote characters for different databases
	QuoteCharacters = map[DXDatabaseType]struct {
		Start []rune
		End   []rune
	}{
		PostgreSQL: {
			Start: []rune{'"'},
			End:   []rune{'"'},
		},
		MariaDB: {
			Start: []rune{'"'},
			End:   []rune{'"'},
		},
		SQLServer: {
			Start: []rune{'[', '"'},
			End:   []rune{']', '"'},
		},
		Oracle: {
			Start: []rune{'"'},
			End:   []rune{'"'},
		},
	}

	// Common SQL keywords across most dialects
	commonKeywords = map[string]bool{
		"SELECT": true, "FROM": true, "WHERE": true, "INSERT": true,
		"UPDATE": true, "DELETE": true, "DROP": true, "CREATE": true,
		"TABLE": true, "INDEX": true, "ALTER": true, "ADD": true,
		"COLUMN": true, "ORDER": true, "BY": true, "GROUP": true,
		"HAVING": true, "JOIN": true, "INNER": true, "OUTER": true,
		"LEFT": true, "RIGHT": true, "FULL": true, "ON": true,
		"AS": true, "DISTINCT": true, "CASE": true, "WHEN": true,
		"THEN": true, "ELSE": true, "END": true, "AND": true,
		"OR": true, "NOT": true, "IN": true, "BETWEEN": true,
		"LIKE": true, "IS": true, "NULL": true, "TRUE": true,
		"FALSE": true, "DESC": true, "ASC": true, "LIMIT": true,
		"OFFSET": true, "WITH": true, "VALUES": true, "INTO": true,
		"PROCEDURE": true, "FUNCTION": true, "TRIGGER": true,
		"VIEW": true, "SEQUENCE": true, "GRANT": true, "REVOKE": true,
		"USER": true, "ROLE": true, "DATABASE": true, "SCHEMA": true,
	}

	postgresKeywords = map[string]bool{
		// PostgreSQL specific reserved words (that aren't in the common list)
		"ABORT": true, "ABSOLUTE": true, "ACCESS": true, "ADMIN": true,
		"AGGREGATE": true, "ALSO": true, "ANALYSE": true, "ANALYZE": true,
		"ASSERTION": true, "ASSIGNMENT": true, "ASYMMETRIC": true, "AT": true,
		"ATOMIC": true, "AUTHORIZATION": true, "BACKWARD": true, "BEFORE": true,
		"BINARY": true, "CACHE": true, "CALLED": true, "CASCADE": true,
		"CATALOG": true, "CHAIN": true, "CHARACTERISTICS": true, "CHECKPOINT": true,
		"CLASS": true, "CLUSTER": true, "COLLATION": true, "COLLATE": true,
		"COLUMN": true, "COMMENT": true, "COMMENTS": true, "COMMIT": true,
		"COMMITTED": true, "CONCURRENTLY": true, "CONFIGURATION": true, "CONNECTION": true,
		"CONSTRAINTS": true, "CONTENT": true, "CONTINUE": true, "CONVERSION": true,
		"COPY": true, "COST": true, "CREATEDB": true, "CREATEROLE": true,
		"CREATEUSER": true, "CSV": true, "CURRENT": true, "CURRENT_CATALOG": true,
		"CURRENT_ROLE": true, "CURRENT_SCHEMA": true, "CYCLE": true, "DATA": true,
		"DATABASE": true, "DAY": true, "DEALLOCATE": true, "DECLARE": true,
		"DEFAULTS": true, "DEFERRED": true, "DEFINER": true, "DELIMITER": true,
		"DELIMITERS": true, "DICTIONARY": true, "DISABLE": true, "DISCARD": true,
		"DOCUMENT": true, "DOMAIN": true, "EACH": true, "ENABLE": true,
		"ENCODING": true, "ENCRYPTED": true, "ENUM": true, "ESCAPE": true,
		"EVENT": true, "EXCLUDE": true, "EXCLUDING": true, "EXCLUSIVE": true,
		"EXECUTE": true, "EXPLAIN": true, "EXTENSION": true, "EXTERNAL": true,
		"FAMILY": true, "FILTER": true, "FIRST": true, "FOLLOWING": true,
		"FORCE": true, "FORWARD": true, "FREEZE": true, "FUNCTIONS": true,
		"GENERATED": true, "GLOBAL": true, "GRANTED": true, "HANDLER": true,
		"HEADER": true, "HOLD": true, "HOUR": true, "IDENTITY": true,
		"IF": true, "ILIKE": true, "IMMEDIATE": true, "IMMUTABLE": true,
		"IMPLICIT": true, "INCLUDING": true, "INCREMENT": true, "INDEX": true,
		"INDEXES": true, "INHERIT": true, "INHERITS": true, "INLINE": true,
		"INPUT": true, "INSENSITIVE": true, "INSTEAD": true,
		"INVOKER": true, "ISOLATION": true, "KEY": true, "LABEL": true,
		"LANGUAGE": true, "LARGE": true, "LAST": true, "LATERAL": true,
		"LEAKPROOF": true, "LEVEL": true, "LISTEN": true, "LOAD": true,
		"LOCAL": true, "LOCATION": true, "LOCK": true, "LOGGED": true,
		"MAPPING": true, "MATCH": true, "MATERIALIZED": true, "MAXVALUE": true,
		"METHOD": true, "MINUTE": true, "MINVALUE": true, "MODE": true,
		"MONTH": true, "MOVE": true, "NAME": true, "NAMES": true,
		"NEXT": true, "NO": true, "NOTHING": true, "NOTIFY": true,
		"NOWAIT": true, "NULLS": true, "OBJECT": true, "OF": true,
		"OFF": true, "OIDS": true, "OPERATOR": true, "OPTION": true,
		"OPTIONS": true, "OWNED": true, "OWNER": true, "PARALLEL": true,
		"PARSER": true, "PARTIAL": true, "PARTITION": true, "PASSING": true,
		"PASSWORD": true, "PLACING": true, "PLANS": true, "POLICY": true,
		"PORTION": true, "PRECEDING": true, "PREPARE": true, "PREPARED": true,
		"PRESERVE": true, "PRIOR": true, "PRIVILEGES": true, "PROCEDURAL": true,
		"PROCEDURE": true, "PROGRAM": true, "PUBLICATION": true, "QUOTE": true,
		"RANGE": true, "READ": true, "REASSIGN": true, "RECHECK": true,
		"RECURSIVE": true, "REF": true, "REFERENCING": true, "REFRESH": true,
		"REINDEX": true, "RELATIVE": true, "RELEASE": true, "RENAME": true,
		"REPEATABLE": true, "REPLACE": true, "REPLICA": true, "RESET": true,
		"RESTART": true, "RESTRICT": true, "RETURNING": true, "RETURNS": true,
		"REVOKE": true, "ROLE": true, "ROLLBACK": true, "ROWS": true,
		"RULE": true, "SAVEPOINT": true, "SCHEMA": true, "SCHEMAS": true,
		"SCROLL": true, "SEARCH": true, "SECOND": true, "SECURITY": true,
		"SEQUENCE": true, "SEQUENCES": true, "SERIALIZABLE": true, "SERVER": true,
		"SESSION": true, "SET": true, "SHARE": true, "SHOW": true,
		"SIMILAR": true, "SIMPLE": true, "SKIP": true, "SNAPSHOT": true,
		"SQL": true, "STABLE": true, "STANDALONE": true, "START": true,
		"STATEMENT": true, "STATISTICS": true, "STDIN": true, "STDOUT": true,
		"STORAGE": true, "STORED": true, "STRICT": true, "STRIP": true,
		"SUBSCRIPTION": true, "SUPPORT": true, "SYSID": true, "SYSTEM": true,
		"TABLES": true, "TABLESPACE": true, "TEMP": true, "TEMPLATE": true,
		"TEMPORARY": true, "TEXT": true, "TIES": true, "TRANSACTION": true,
		"TRANSFORM": true, "TRIGGER": true, "TRUNCATE": true, "TRUSTED": true,
		"TYPE": true, "TYPES": true, "UNBOUNDED": true, "UNCOMMITTED": true,
		"UNENCRYPTED": true, "UNKNOWN": true, "UNLISTEN": true, "UNLOGGED": true,
		"UNTIL": true, "VACUUM": true, "VALID": true, "VALIDATE": true,
		"VALIDATOR": true, "VALUE": true, "VARYING": true, "VERSION": true,
		"VIEW": true, "VOLATILE": true, "WHITESPACE": true, "WITHIN": true,
		"WITHOUT": true, "WORK": true, "WRAPPER": true, "WRITE": true,
		"XML": true, "YEAR": true, "YES": true, "ZONE": true,
	}

	// postgresKeywordsCanBeUsedAsFieldNameDirectly contains PostgreSQL keywords that are
	// non-reserved and can be used as column/table names without double quotes.
	// Based on PostgreSQL official documentation (version 17.6)
	postgresKeywordsCanBeUsedAsFieldNameDirectly = map[string]bool{
		// Non-reserved PostgreSQL keywords - safe to use as identifiers
		"ABORT":           true,
		"ABSOLUTE":        true,
		"ACCESS":          true,
		"ADMIN":           true,
		"AGGREGATE":       true,
		"ALSO":            true,
		"ASSERTION":       true,
		"ASSIGNMENT":      true,
		"AT":              true,
		"BACKWARD":        true,
		"BEFORE":          true,
		"CACHE":           true,
		"CALLED":          true,
		"CASCADE":         true,
		"CATALOG":         true,
		"CHAIN":           true,
		"CHARACTERISTICS": true,
		"CHECKPOINT":      true,
		"CLASS":           true,
		"CLUSTER":         true,
		"COMMENT":         true,
		"COMMENTS":        true,
		"COMMIT":          true,
		"COMMITTED":       true,
		"CONFIGURATION":   true,
		"CONNECTION":      true,
		"CONSTRAINTS":     true,
		"CONTENT":         true,
		"CONTINUE":        true,
		"CONVERSION":      true,
		"COPY":            true,
		"COST":            true,
		"CREATEDB":        true,
		"CREATEROLE":      true,
		"CREATEUSER":      true,
		"CSV":             true,
		"CURRENT":         true,
		"CYCLE":           true,
		"DATA":            true,
		"DATABASE":        true,
		"DAY":             true,
		"DEALLOCATE":      true,
		"DECLARE":         true,
		"DEFAULTS":        true,
		"DEFERRED":        true,
		"DEFINER":         true,
		"DELIMITER":       true,
		"DELIMITERS":      true,
		"DICTIONARY":      true,
		"DISABLE":         true,
		"DISCARD":         true,
		"DOCUMENT":        true,
		"DOMAIN":          true,
		"EACH":            true,
		"ENABLE":          true,
		"ENCODING":        true,
		"ENCRYPTED":       true,
		"ENUM":            true,
		"ESCAPE":          true,
		"EVENT":           true,
		"EXCLUDE":         true,
		"EXCLUDING":       true,
		"EXCLUSIVE":       true,
		"EXECUTE":         true,
		"EXPLAIN":         true,
		"EXTENSION":       true,
		"EXTERNAL":        true,
		"FAMILY":          true,
		"FILTER":          true,
		"FIRST":           true,
		"FOLLOWING":       true,
		"FORCE":           true,
		"FORWARD":         true,
		"FREEZE":          true,
		"FUNCTIONS":       true,
		"GENERATED":       true,
		"GLOBAL":          true,
		"GRANTED":         true,
		"HANDLER":         true,
		"HEADER":          true,
		"HOLD":            true,
		"HOUR":            true,
		"IDENTITY":        true,
		"IF":              true,
		"IMMEDIATE":       true,
		"IMMUTABLE":       true,
		"IMPLICIT":        true,
		"INCLUDING":       true,
		"INCREMENT":       true,
		"INDEX":           true,
		"INDEXES":         true,
		"INHERIT":         true,
		"INHERITS":        true,
		"INLINE":          true,
		"INPUT":           true,
		"INSENSITIVE":     true,
		"INSTEAD":         true,
		"INVOKER":         true,
		"ISOLATION":       true,
		"KEY":             true,
		"LABEL":           true,
		"LANGUAGE":        true,
		"LARGE":           true,
		"LAST":            true,
		"LEAKPROOF":       true,
		"LEVEL":           true,
		"LISTEN":          true,
		"LOAD":            true,
		"LOCAL":           true,
		"LOCATION":        true,
		"LOCK":            true,
		"LOGGED":          true,
		"MAPPING":         true,
		"MATCH":           true,
		"MATERIALIZED":    true,
		"MAXVALUE":        true,
		"METHOD":          true,
		"MINUTE":          true,
		"MINVALUE":        true,
		"MODE":            true,
		"MONTH":           true,
		"MOVE":            true,
		"NAME":            true,
		"NAMES":           true,
		"NEXT":            true,
		"NO":              true,
		"NOTHING":         true,
		"NOTIFY":          true,
		"NOWAIT":          true,
		"NULLS":           true,
		"OBJECT":          true,
		"OF":              true,
		"OFF":             true,
		"OIDS":            true,
		"OPERATOR":        true,
		"OPTION":          true,
		"OPTIONS":         true,
		"OWNED":           true,
		"OWNER":           true,
		"PARALLEL":        true,
		"PARSER":          true,
		"PARTIAL":         true,
		"PARTITION":       true,
		"PASSING":         true,
		"PASSWORD":        true,
		"PLACING":         true,
		"PLANS":           true,
		"POLICY":          true,
		"PORTION":         true,
		"PRECEDING":       true,
		"PREPARE":         true,
		"PREPARED":        true,
		"PRESERVE":        true,
		"PRIOR":           true,
		"PRIVILEGES":      true,
		"PROCEDURAL":      true,
		"PROCEDURE":       true,
		"PROGRAM":         true,
		"PUBLICATION":     true,
		"QUOTE":           true,
		"RANGE":           true,
		"READ":            true,
		"REASSIGN":        true,
		"RECHECK":         true,
		"RECURSIVE":       true,
		"REF":             true,
		"REFERENCING":     true,
		"REFRESH":         true,
		"REINDEX":         true,
		"RELATIVE":        true,
		"RELEASE":         true,
		"RENAME":          true,
		"REPEATABLE":      true,
		"REPLACE":         true,
		"REPLICA":         true,
		"RESET":           true,
		"RESTART":         true,
		"RESTRICT":        true,
		"RETURNING":       true,
		"RETURNS":         true,
		"REVOKE":          true,
		"ROLE":            true,
		"ROLLBACK":        true,
		"ROWS":            true,
		"RULE":            true,
		"SAVEPOINT":       true,
		"SCHEMA":          true,
		"SCHEMAS":         true,
		"SCROLL":          true,
		"SEARCH":          true,
		"SECOND":          true,
		"SECURITY":        true,
		"SEQUENCE":        true,
		"SEQUENCES":       true,
		"SERIALIZABLE":    true,
		"SERVER":          true,
		"SESSION":         true,
		"SET":             true,
		"SHARE":           true,
		"SHOW":            true,
		"SIMPLE":          true,
		"SKIP":            true,
		"SNAPSHOT":        true,
		"SQL":             true,
		"STABLE":          true,
		"STANDALONE":      true,
		"START":           true,
		"STATEMENT":       true,
		"STATISTICS":      true,
		"STDIN":           true,
		"STDOUT":          true,
		"STORAGE":         true,
		"STORED":          true,
		"STRICT":          true,
		"STRIP":           true,
		"SUBSCRIPTION":    true,
		"SUPPORT":         true,
		"SYSID":           true,
		"SYSTEM":          true,
		"TABLES":          true,
		"TABLESPACE":      true,
		"TEMP":            true,
		"TEMPLATE":        true,
		"TEMPORARY":       true,
		"TEXT":            true,
		"TIES":            true,
		"TRANSACTION":     true,
		"TRANSFORM":       true,
		"TRIGGER":         true,
		"TRUNCATE":        true,
		"TRUSTED":         true,
		"TYPE":            true,
		"TYPES":           true,
		"UNBOUNDED":       true,
		"UNCOMMITTED":     true,
		"UNENCRYPTED":     true,
		"UNKNOWN":         true,
		"UNLISTEN":        true,
		"UNLOGGED":        true,
		"UNTIL":           true,
		"VACUUM":          true,
		"VALID":           true,
		"VALIDATE":        true,
		"VALIDATOR":       true,
		"VALUE":           true,
		"VERSION":         true,
		"VIEW":            true,
		"VOLATILE":        true,
		"WHITESPACE":      true,
		"WORK":            true,
		"WRAPPER":         true,
		"WRITE":           true,
		"YEAR":            true,
		"YES":             true,
		"ZONE":            true,
	}

	// MySQL/MariaDB specific keywords
	mysqlKeywords = map[string]bool{
		// MySQL 8.0 reserved keywords (that aren't in the common list)
		"ACCESSIBLE": true, "ACCOUNT": true, "ACTION": true, "AGAINST": true,
		"AGGREGATE": true, "ALGORITHM": true, "ANALYZE": true, "ANY": true,
		"AT": true, "AUTHORS": true, "AUTO_INCREMENT": true, "AUTOEXTEND_SIZE": true,
		"AVG": true, "AVG_ROW_LENGTH": true, "BACKUP": true, "BEGIN": true,
		"BINLOG": true, "BIT": true, "BLOCK": true, "BOOL": true,
		"BOOLEAN": true, "BTREE": true, "CACHE": true, "CASCADED": true,
		"CHAIN": true, "CHANGE": true, "CHANGED": true, "CHANNEL": true,
		"CHARSET": true, "CHECKSUM": true, "CIPHER": true, "CLIENT": true,
		"COALESCE": true, "CODE": true, "COLLATE": true, "COLLATION": true,
		"COLUMNS": true, "COMMENT": true, "COMMIT": true, "COMMITTED": true,
		"COMPACT": true, "COMPLETION": true, "COMPRESSED": true, "COMPRESSION": true,
		"CONCURRENT": true, "CONNECTION": true, "CONSISTENT": true, "CONSTRAINT": true,
		"CONTAINS": true, "CONTEXT": true, "CONTRIBUTORS": true, "COPY": true,
		"CPU": true, "DATA": true, "DATAFILE": true, "DEALLOCATE": true,
		"DEFAULT": true, "DEFINER": true, "DELAY_KEY_WRITE": true, "DELAYED": true,
		"DELIMITER": true, "DES_KEY_FILE": true, "DIRECTORY": true, "DISABLE": true,
		"DISCARD": true, "DISK": true, "DO": true, "DUMPFILE": true,
		"DUPLICATE": true, "DYNAMIC": true, "ENABLE": true, "ENCLOSED": true,
		"ENCRYPTION": true, "ENGINE": true, "ENGINES": true, "ERROR": true,
		"ERRORS": true, "ESCAPE": true, "EVENT": true, "EVENTS": true,
		"EVERY": true, "EXCHANGE": true, "EXECUTE": true, "EXPANSION": true,
		"EXPIRE": true, "EXPORT": true, "EXTENDED": true, "EXTENT_SIZE": true,
		"FAST": true, "FAULTS": true, "FIELDS": true, "FILE": true,
		"FILE_BLOCK_SIZE": true, "FILTER": true, "FIRST": true, "FIXED": true,
		"FLUSH": true, "FOLLOWS": true, "FORMAT": true, "FOUND": true,
		"FULL": true, "GENERAL": true, "GEOMETRY": true, "GEOMETRYCOLLECTION": true,
		"GET_FORMAT": true, "GLOBAL": true, "GRANTS": true, "GROUP_REPLICATION": true,
		"HANDLER": true, "HASH": true, "HELP": true, "HOST": true,
		"HOSTS": true, "IDENTIFIED": true, "IGNORE": true, "IGNORE_SERVER_IDS": true,
		"IMPORT": true, "INDEXES": true, "INITIAL_SIZE": true, "INNOBASE": true,
		"INNODB": true, "IO": true, "IO_THREAD": true, "IPC": true,
		"ISOLATION": true, "ISSUER": true, "JSON": true, "KEY_BLOCK_SIZE": true,
		"LANGUAGE": true, "LAST": true, "LEAVES": true, "LESS": true,
		"LINESTRING": true, "LIST": true, "LOCAL": true, "LOGFILE": true,
		"LOGS": true, "MASTER": true, "MASTER_AUTO_POSITION": true, "MASTER_CONNECT_RETRY": true,
		"MASTER_DELAY": true, "MASTER_HEARTBEAT_PERIOD": true, "MASTER_HOST": true, "MASTER_LOG_FILE": true,
		"MASTER_LOG_POS": true, "MASTER_PASSWORD": true, "MASTER_PORT": true, "MASTER_RETRY_COUNT": true,
		"MASTER_SERVER_ID": true, "MASTER_SSL": true, "MASTER_SSL_CA": true, "MASTER_SSL_CAPATH": true,
		"MASTER_SSL_CERT": true, "MASTER_SSL_CIPHER": true, "MASTER_SSL_CRL": true, "MASTER_SSL_CRLPATH": true,
		"MASTER_SSL_KEY": true, "MASTER_SSL_VERIFY_SERVER_CERT": true, "MASTER_TLS_VERSION": true, "MASTER_USER": true,
		"MAX_CONNECTIONS_PER_HOUR": true, "MAX_QUERIES_PER_HOUR": true, "MAX_ROWS": true, "MAX_SIZE": true,
		"MAX_UPDATES_PER_HOUR": true, "MAX_USER_CONNECTIONS": true, "MEDIUM": true, "MEMORY": true,
		"MERGE": true, "MESSAGE_TEXT": true, "MICROSECOND": true, "MIGRATE": true,
		"MIN_ROWS": true, "MODE": true, "MODIFY": true, "MULTILINESTRING": true,
		"MULTIPOINT": true, "MULTIPOLYGON": true, "MUTEX": true, "MYSQL_ERRNO": true,
		"NAME": true, "NAMES": true, "NATIONAL": true, "NCHAR": true,
		"NDB": true, "NDBCLUSTER": true, "NEVER": true, "NEXT": true,
		"NO": true, "NODEGROUP": true, "NONE": true, "NOWAIT": true,
		"NO_WAIT": true, "NVARCHAR": true, "OFFSET": true, "OJ": true,
		"OLD_PASSWORD": true, "ONE": true, "ONLY": true, "OPEN": true,
		"OPTIMIZER_COSTS": true, "OPTIONS": true, "OWNER": true, "PACK_KEYS": true,
		"PAGE": true, "PARSER": true, "PARTIAL": true, "PARTITIONING": true,
		"PARTITIONS": true, "PASSWORD": true, "PHASE": true, "PLUGIN": true,
		"PLUGIN_DIR": true, "PLUGINS": true, "POINT": true, "POLYGON": true,
		"PORT": true, "PRECEDES": true, "PREPARE": true, "PRESERVE": true,
		"PREV": true, "PROCESSLIST": true, "PROFILE": true, "PROFILES": true,
		"PROXY": true, "QUERY": true, "QUICK": true, "REBUILD": true,
		"RECOVER": true, "REDO_BUFFER_SIZE": true, "REDUNDANT": true, "RELAY": true,
		"RELAYLOG": true, "RELAY_LOG_FILE": true, "RELAY_LOG_POS": true, "REMOVE": true,
		"REORGANIZE": true, "REPAIR": true, "REPEATABLE": true, "REPLICATE_DO_DB": true,
		"REPLICATE_DO_TABLE": true, "REPLICATE_IGNORE_DB": true, "REPLICATE_IGNORE_TABLE": true, "REPLICATE_REWRITE_DB": true,
		"REPLICATE_WILD_DO_TABLE": true, "REPLICATE_WILD_IGNORE_TABLE": true, "REPLICATION": true, "RESET": true,
		"RESTART": true, "RESTORE": true, "RESUME": true, "RETURNS": true,
		"ROLLBACK": true, "ROLLUP": true, "ROUTINE": true, "ROW": true,
		"ROWS": true, "ROW_FORMAT": true, "RTREE": true, "SAVEPOINT": true,
		"SCHEDULE": true, "SCHEDULER": true, "SECURITY": true, "SERIAL": true,
		"SERVER": true, "SESSION": true, "SHARE": true, "SHUTDOWN": true,
		"SIGNED": true, "SIMPLE": true, "SLAVE": true, "SLOW": true,
		"SNAPSHOT": true, "SOCKET": true, "SOME": true, "SONAME": true,
		"SOUNDS": true, "SOURCE": true, "SPATIAL": true, "SQLEXCEPTION": true,
		"SQLSTATE": true, "SQLWARNING": true, "SQL_AFTER_GTIDS": true, "SQL_AFTER_MTS_GAPS": true,
		"SQL_BEFORE_GTIDS": true, "SQL_BIG_RESULT": true, "SQL_BUFFER_RESULT": true, "SQL_CACHE": true,
		"SQL_CALC_FOUND_ROWS": true, "SQL_NO_CACHE": true, "SQL_SMALL_RESULT": true, "SQL_THREAD": true,
		"SSL": true, "STACKED": true, "STARTING": true, "STARTS": true,
		"STATS_AUTO_RECALC": true, "STATS_PERSISTENT": true, "STATS_SAMPLE_PAGES": true, "STATUS": true,
		"STOP": true, "STORAGE": true, "STORED": true, "STRAIGHT_JOIN": true,
		"STRING": true, "SUBJECT": true, "SUBPARTITION": true, "SUBPARTITIONS": true,
		"SUPER": true, "SUSPEND": true, "SWAPS": true, "SWITCHES": true,
		"TABLESPACE": true, "TEMPORARY": true, "TEMPTABLE": true, "THAN": true,
		"TRANSACTION": true, "TRUNCATE": true, "TYPE": true, "TYPES": true,
		"UNCOMMITTED": true, "UNDEFINED": true, "UNDO": true, "UNDOFILE": true,
		"UNDO_BUFFER_SIZE": true, "UNICODE": true, "UNINSTALL": true, "UNKNOWN": true,
		"UNTIL": true, "UPGRADE": true, "USAGE": true, "USE": true,
		"USER": true, "USER_RESOURCES": true, "USE_FRM": true, "VALIDATION": true,
		"VALUE": true, "VARIABLES": true, "VCPU": true, "VIEW": true,
		"VIRTUAL": true, "VISIBLE": true, "WAIT": true, "WARNINGS": true,
		"WEEK": true, "WEIGHT_STRING": true, "WITHOUT": true, "WORK": true,
		"WRAPPER": true, "X509": true, "XA": true, "XID": true,
		"XML": true, "YEAR": true,

		// MariaDB specific keywords
		"ADMIN": true, "CUME_DIST": true, "DENSE_RANK": true, "EMPTY": true,
		"EXCEPT": true, "FIRST_VALUE": true, "GROUPING": true, "INTERSECT": true,
		"JSON_TABLE": true, "LAG": true, "LAST_VALUE": true, "LEAD": true,
		"NTH_VALUE": true, "NTILE": true, "OF": true, "OVER": true,
		"PERCENT_RANK": true, "PERSIST": true, "PERSIST_ONLY": true, "PERCENT": true,
		"RANK": true, "RECURSIVE": true, "ROW_NUMBER": true, "SYSTEM": true,
		"WINDOW": true,
	}

	// SQL Server specific keywords
	sqlServerKeywords = map[string]bool{
		// SQL Server specific reserved keywords (that aren't in the common list)
		"ABSOLUTE": true, "ACTION": true, "ADMIN": true, "AFTER": true,
		"AGGREGATE": true, "ALGORITHM": true, "ALLOW_SNAPSHOT_ISOLATION": true, "ANSI_NULLS": true,
		"ANSI_PADDING": true, "ANSI_WARNINGS": true, "APPLICATION_LOG": true, "APPLY": true,
		"ASSEMBLY": true, "ASYMMETRIC": true, "ATOMIC": true, "AUDIT": true,
		"AUTHORIZATION": true, "AUTO": true, "AUTO_CLEANUP": true, "AUTO_CLOSE": true,
		"AUTO_CREATE_STATISTICS": true, "AUTO_SHRINK": true, "AUTO_UPDATE_STATISTICS": true, "AVAILABILITY": true,
		"BACKUP": true, "BEFORE": true, "BEGIN": true, "BINARY": true,
		"BINDING": true, "BLOB_STORAGE": true, "BROKER": true, "BROKER_INSTANCE": true,
		"BULK": true, "CALLER": true, "CAP_CPU_PERCENT": true, "CASCADE": true,
		"CATALOG": true, "CATCH": true, "CHANGE_RETENTION": true, "CHANGE_TRACKING": true,
		"CHECKSUM": true, "CLASSIFIER_FUNCTION": true, "CLEANUP": true, "COLLECTION": true,
		"COLUMNSTORE": true, "COMMITTED": true, "COMPATIBILITY_LEVEL": true, "COMPRESS_ALL_ROW_GROUPS": true,
		"COMPRESSION": true, "CONCAT": true, "CONCAT_NULL_YIELDS_NULL": true, "CONTROL": true,
		"COOKIE": true, "CROSS": true, "CURSOR_CLOSE_ON_COMMIT": true, "CURSOR_DEFAULT": true,
		"DATA": true, "DATA_COMPRESSION": true, "DATA_PURITY": true, "DATABASE": true,
		"DATABASE_MIRRORING": true, "DATE_CORRELATION_OPTIMIZATION": true, "DATEADD": true, "DATEDIFF": true,
		"DATENAME": true, "DATEPART": true, "DAYS": true, "DB_CHAINING": true,
		"DBCC": true, "DEALLOCATE": true, "DECLARE": true, "DEFAULT_DATABASE": true,
		"DEFAULT_FULLTEXT_LANGUAGE": true, "DEFAULT_LANGUAGE": true, "DEFAULT_SCHEMA": true, "DELAYED_DURABILITY": true,
		"DENY": true, "DENSE_RANK": true, "DIRECTORY_NAME": true, "DISABLE": true,
		"DISABLED": true, "DISABLE_BROKER": true, "DISK": true, "DISTRIBUTED": true,
		"DUMP": true, "DURABILITY": true, "ELEMENTS": true, "EMERGENCY": true,
		"ENABLE": true, "ENABLE_BROKER": true, "ENCRYPTED": true, "ENCRYPTION": true,
		"ENDPOINT": true, "ERRLVL": true, "ERROR_BROKER_CONVERSATIONS": true, "ESCAPE": true,
		"EVENT": true, "EVENTDATA": true, "EXCLUSIVE": true, "EXECUTABLE": true,
		"EXECUTE": true, "EXIT": true, "EXPANDABLE": true, "FAST": true,
		"FAST_FORWARD": true, "FETCH": true, "FILE": true, "FILEGROUP": true,
		"FILEGROWTH": true, "FILENAME": true, "FILESTREAM": true, "FILLFACTOR": true,
		"FILTER": true, "FIRST": true, "FOLLOWING": true, "FOR": true,
		"FORCE": true, "FORCED": true, "FORCESEEK": true, "FOREIGN": true,
		"FORWARD_ONLY": true, "FREE": true, "FULLSCAN": true, "FULLTEXT": true,
		"GB": true, "GETDATE": true, "GETUTCDATE": true, "GO": true,
		"GOVERNOR": true, "GROUP_MAX_REQUESTS": true, "HADR": true, "HASH": true,
		"HASHED": true, "HEALTHCHECKTIMEOUT": true, "HONOR_BROKER_PRIORITY": true, "HOURS": true,
		"IDENTITY_INSERT": true, "IGNORE_CONSTRAINTS": true, "IGNORE_DUP_KEY": true, "IGNORE_NONCLUSTERED_COLUMNSTORE_INDEX": true,
		"IGNORE_TRIGGERS": true, "IMMEDIATE": true, "IMPERSONATE": true, "INCLUDE": true,
		"INCREMENT": true, "INCREMENTAL": true, "INFINITE": true, "INIT": true,
		"INSENSITIVE": true, "INSERTED": true, "ISOLATION": true, "KB": true,
		"KEEP": true, "KEEPDEFAULTS": true, "KEEPFIXED": true, "KEEPIDENTITY": true,
		"KEY": true, "KEYSET": true, "LANGUAGE": true, "LAST": true,
		"LEVEL": true, "LIBRARY": true, "LIFETIME": true, "LINKED": true,
		"LINUX": true, "LISTENER": true, "LISTENER_URL": true, "LOB_COMPACTION": true,
		"LOCAL": true, "LOCAL_SERVICE_NAME": true, "LOCK": true, "LOCK_ESCALATION": true,
		"LOG": true, "LOGIN": true, "LOOP": true, "MANUAL": true,
		"MARK": true, "MASTER": true, "MAX": true, "MAX_CPU_PERCENT": true,
		"MAX_DOP": true, "MAX_FILES": true, "MAX_MEMORY": true, "MAX_MEMORY_PERCENT": true,
		"MAX_PROCESSES": true, "MAX_QUEUE_READERS": true, "MAX_ROLLOVER_FILES": true, "MAXDOP": true,
		"MAXRECURSION": true, "MAXSIZE": true, "MB": true, "MEDIUM": true,
		"MEMORY_OPTIMIZED": true, "MERGE": true, "MESSAGE": true, "MIN": true,
		"MIN_ACTIVE_ROWVERSION": true, "MIN_CPU_PERCENT": true, "MIN_MEMORY_PERCENT": true, "MINUTES": true,
		"MIRROR": true, "MIRROR_ADDRESS": true, "MIXED_PAGE_ALLOCATION": true, "MODE": true,
		"MODIFY": true, "MOVE": true, "MULTI_USER": true, "NAME": true,
		"NATIVE_COMPILATION": true, "NESTED_TRIGGERS": true, "NEW_BROKER": true, "NEWNAME": true,
		"NEXT": true, "NO": true, "NO_CHECKSUM": true, "NO_COMPRESSION": true,
		"NO_EVENT_LOSS": true, "NO_TRUNCATE": true, "NO_WAIT": true, "NOCOUNT": true,
		"NODES": true, "NOEXPAND": true, "NON_TRANSACTED_ACCESS": true, "NORECOMPUTE": true,
		"NORECOVERY": true, "NOWAIT": true, "NTILE": true, "NUMANODE": true,
		"NUMERIC_ROUNDABORT": true, "OBJECT": true, "OFFLINE": true, "OFFSET": true,
		"OLD_PASSWORD": true, "ONLINE": true, "ONLY": true, "OPEN": true,
		"OPEN_EXISTING": true, "OPENQUERY": true, "OPENROWSET": true, "OPENXML": true,
		"OPTIMISTIC": true, "OPTIMIZE": true, "OPTIMIZE_FOR_SEQUENTIAL_KEY": true, "OPTION": true,
		"OUT": true, "OUTPUT": true, "OWNER": true, "OWNERSHIP": true,
		"PAGE": true, "PAGECOUNT": true, "PARTITION": true, "PARTITIONS": true,
		"PASSWORD": true, "PATH": true, "PERCENT": true, "PERCENT_RANK": true,
		"PERCENTILE_CONT": true, "PERCENTILE_DISC": true, "PERMISSION_SET": true, "PERSISTED": true,
		"PILOT": true, "PIVOT": true, "PLAN": true, "POLICY": true,
		"POOL": true, "POPULATION": true, "PRECEDING": true, "PRECISION": true,
		"PREDICATE": true, "PRIMARY_ROLE": true, "PRIOR": true, "PRIORITY": true,
		"PRIORITY_LEVEL": true, "PRIVATE": true, "PRIVILEGES": true, "PROCEDURE": true,
		"PROCESS": true, "PROFILE": true, "PROPERTY": true, "PROVIDER": true,
		"QUERY_STORE": true, "QUEUE": true, "QUOTED_IDENTIFIER": true, "RAISERROR": true,
		"RANGE": true, "RANK": true, "RC2": true, "RC4": true,
		"RC4_128": true, "READ_COMMITTED_SNAPSHOT": true, "READ_ONLY": true, "READ_WRITE": true,
		"READCOMMITTED": true, "READCOMMITTEDLOCK": true, "READONLY": true, "READPAST": true,
		"READUNCOMMITTED": true, "READWRITE": true, "REBUILD": true, "RECEIVE": true,
		"RECOMPILE": true, "RECOVERY": true, "RECURSIVE": true, "RECURSIVE_TRIGGERS": true,
		"REFERENCES": true, "REGENERATE": true, "RELATED_CONVERSATION": true, "RELATED_CONVERSATION_GROUP": true,
		"RELATIVE": true, "REMOTE": true, "REMOTE_PROC_TRANSACTIONS": true, "REMOTE_SERVICE_NAME": true,
		"REMOVE": true, "REORGANIZE": true, "REPEATABLE": true, "REPEATABLEREAD": true,
		"REPLACE": true, "REPLICA": true, "REPLICATE": true, "REQUEST_MAX_CPU_TIME_SEC": true,
		"REQUEST_MAX_MEMORY_GRANT_PERCENT": true, "REQUEST_MEMORY_GRANT_TIMEOUT_SEC": true, "REQUIRED_SYNCHRONIZED_SECONDARIES_TO_COMMIT": true, "RESAMPLE": true,
		"RESET": true, "RESTART": true, "RESTORE": true, "RESTRICT_GEOM_SPEC_TO_REGIONATOR": true,
		"RESTRICTED_USER": true, "RESUMABLE": true, "RETENTION": true, "RETURN": true,
		"RETURNS": true, "REVERT": true, "ROLE": true, "ROLLBACK": true,
		"ROUTE": true, "ROW": true, "ROW_NUMBER": true, "ROWCOUNT": true,
		"ROWGUID": true, "ROWGUIDCOL": true, "ROWS": true, "ROWS_PER_BATCH": true,
		"RSA_1024": true, "RSA_2048": true, "RSA_3072": true, "RSA_4096": true,
		"RSA_512": true, "SAFETY": true, "SAFE": true, "SAMPLE": true,
		"SAVE": true, "SCHEDULER": true, "SCHEMA": true, "SCHEMA_ID": true,
		"SCHEMA_NAME": true, "SCHEMABINDING": true, "SCOPE": true, "SCROLL": true,
		"SCROLL_LOCKS": true, "SEARCH": true, "SECONDS": true, "SECONDARY": true,
		"SECONDARY_ONLY": true, "SECONDARY_ROLE": true, "SECURITY": true, "SECURITY_LOG": true,
		"SECURITYAUDIT": true, "SELECTIVE": true, "SELF": true, "SEND": true,
		"SENT": true, "SEQUENCE": true, "SERIALIZABLE": true, "SERVER": true,
		"SERVICE": true, "SERVICE_BROKER": true, "SERVICE_NAME": true, "SESSION": true,
		"SESSION_TIMEOUT": true, "SETERROR": true, "SETS": true, "SETTINGS": true,
		"SHOWPLAN": true, "SHOWPLAN_ALL": true, "SHOWPLAN_TEXT": true, "SHOWPLAN_XML": true,
		"SHRINKLOG": true, "SHUTDOWN": true, "SID": true, "SIGNATURE": true,
		"SIMPLE": true, "SINGLE_USER": true, "SIZE": true, "SMALLINT": true,
		"SNAPSHOT": true, "SORT_IN_TEMPDB": true, "SOURCE": true, "SPARSE": true,
		"SPATIAL": true, "SPECIFICATION": true, "SPLIT": true, "SQL": true,
		"SQLDUMPERFLAGS": true, "SQLDUMPERUSERDUMPS": true, "SQLDUMPERSETUPFLAGS": true, "STANDBY": true,
		"START": true, "START_DATE": true, "STARTED": true, "STARTUP_STATE": true,
		"STATE": true, "STATIC": true, "STATISTICAL_SEMANTICS": true, "STATISTICS": true,
		"STATISTICS_INCREMENTAL": true, "STATISTICS_NORECOMPUTE": true, "STATS": true, "STATS_STREAM": true,
		"STATUS": true, "STATUSONLY": true, "STOP": true, "STOPLIST": true,
		"STOPPED": true, "STRING_AGG": true, "SUBJECT": true, "SUBSCRIPTION": true,
		"SUPPORTED": true, "SUSPEND": true, "SWITCH": true, "SYMMETRIC": true,
		"SYNCHRONOUS_COMMIT": true, "SYNONYM": true, "SYSTEM": true, "TAKE": true,
		"TARGET": true, "TARGET_RECOVERY_TIME": true, "TB": true, "TEXTSIZE": true,
		"THROW": true, "TIES": true, "TIME": true, "TIMEOUT": true,
		"TIMER": true, "TINYINT": true, "TOP": true, "TORN_PAGE_DETECTION": true,
		"TRACK_CAUSALITY": true, "TRANSFER": true, "TRANSFORM_NOISE_WORDS": true, "TRIGGER": true,
		"TRIPLE_DES": true, "TRIPLE_DES_3KEY": true, "TRUSTWORTHY": true, "TRY": true,
		"TSQL": true, "TWO_DIGIT_YEAR_CUTOFF": true, "TYPE": true, "TYPE_WARNING": true,
		"UNBOUNDED": true, "UNCOMMITTED": true, "UNCHECKED": true, "UNDEFINED": true,
		"UNLIMITED": true, "UNMASK": true, "UNPIVOT": true, "UNSAFE": true,
		"URL": true, "USAGE": true, "USE": true, "USED": true,
		"USER": true, "USER_OPTIONS": true, "USING": true, "VALIDATION": true,
		"VALUE": true, "VARYING": true, "VIEW": true, "VIEWS": true,
		"WAIT": true, "WAITFOR": true, "WELL_FORMED_XML": true, "WINDOWS": true,
		"WITHOUT": true, "WITNESS": true, "WORK": true, "WORKLOAD": true,
		"XML": true, "XMLNAMESPACES": true, "XSINIL": true, "ZONE": true,
	}

	// Oracle specific keywords
	oracleKeywords = map[string]bool{
		// Oracle specific reserved keywords (that aren't in the common list)
		"ACCESS": true, "ACCOUNT": true, "ACTIVATE": true, "ADMIN": true,
		"ADMINISTRATOR": true, "ADVISE": true, "AFTER": true, "ALGORITHM": true,
		"ALLOCATE": true, "ANALYZE": true, "ARCHIVE": true, "ARCHIVELOG": true,
		"ARRAY": true, "ASSOCIATE": true, "AT": true, "ATTRIBUTE": true,
		"AUDIT": true, "AUTHENTICATED": true, "AUTHORIZATION": true, "AUTOALLOCATE": true,
		"AUTOEXTEND": true, "AUTOMATIC": true, "AVAILABILITY": true, "BACKUP": true,
		"BECOME": true, "BEFORE": true, "BEGIN": true, "BEHALF": true,
		"BINARY": true, "BINDING": true, "BITMAP": true, "BLOCK": true,
		"BLOCKSIZE": true, "BODY": true, "CACHE": true, "CACHE_INSTANCES": true,
		"CANCEL": true, "CASCADE": true, "CATEGORY": true, "CERTIFICATE": true,
		"CHAINED": true, "CHANGE": true, "CHAR_CS": true, "CHARACTER": true,
		"CHECKPOINT": true, "CHILD": true, "CHUNK": true, "CLASS": true,
		"CLEAR": true, "CLONE": true, "CLOSE": true, "CLUSTER": true,
		"COMMENT": true, "COMMIT": true, "COMMITTED": true, "COMPATIBILITY": true,
		"COMPILE": true, "COMPLETE": true, "COMPOSITE_LIMIT": true, "COMPRESS": true,
		"COMPUTE": true, "CONNECT": true, "CONNECT_TIME": true, "CONSIDER": true,
		"CONSISTENT": true, "CONSTRAINT": true, "CONTAINER": true, "CONTAINED": true,
		"CONTINUE": true, "CONTROLFILE": true, "CORRUPTION": true, "COST": true,
		"CPU_PER_CALL": true, "CPU_PER_SESSION": true, "CURRENT": true, "CURRENT_USER": true,
		"CURSOR": true, "CYCLE": true, "DANGLING": true, "DATABASE": true,
		"DATAFILE": true, "DATAFILES": true, "DATAOBJNO": true, "DBA": true,
		"DBHIGH": true, "DBLOW": true, "DBMAC": true, "DDL": true,
		"DEALLOCATE": true, "DEBUG": true, "DEFINE": true, "DEFINITION": true,
		"DEGREE": true, "DEREF": true, "DIRECT_LOAD": true, "DIRECTORY": true,
		"DISABLE": true, "DISASSOCIATE": true, "DISCONNECT": true, "DISK": true,
		"DISKGROUP": true, "DISKS": true, "DISMOUNT": true, "DISPATCHERS": true,
		"DISTRIBUTED": true, "DML": true, "DOCUMENT": true, "DOMAIN": true,
		"DUMP": true, "EDIT_DISTANCE": true, "EDITION": true, "ELEMENT": true,
		"ELIMINATE_DUPLICATES": true, "ENABLE": true, "ENCRYPTION": true, "ENFORCE": true,
		"ENTRY": true, "ERROR_ON_OVERLAP_TIME": true, "ERRORS": true, "ESCAPE": true,
		"ESTIMATE": true, "EVENTS": true, "EXCEPT": true, "EXCEPTIONS": true,
		"EXCHANGE": true, "EXCLUDING": true, "EXCLUSIVE": true, "EXECUTE": true,
		"EXPIRE": true, "EXPLAIN": true, "EXTENT": true, "EXTENTS": true,
		"EXTERNALLY": true, "FAILED_LOGIN_ATTEMPTS": true, "FAST": true, "FEATURE": true,
		"FILE": true, "FILESYSTEM_LIKE_LOGGING": true, "FINAL": true, "FINISH": true,
		"FIRST": true, "FLUSH": true, "FOLLOWING": true, "FORCE": true,
		"FOREIGN": true, "FREELIST": true, "FREELISTS": true, "FREEPOOLS": true,
		"FRESH": true, "FULL": true, "FUNCTION": true, "FUNCTIONS": true,
		"GENERATED": true, "GLOBAL": true, "GLOBAL_NAME": true, "GLOBALLY": true,
		"GROUPS": true, "GUARANTEED": true, "GUARD": true, "HASH": true,
		"HASHKEYS": true, "HEADER": true, "HEAP": true, "HIERARCHY": true,
		"HOUR": true, "IDENTIFIED": true, "IDENTIFIER": true, "IDENTITY": true,
		"IDGENERATORS": true, "IDLE_TIME": true, "IMMEDIATE": true, "IMPLEMENTATION": true,
		"INCLUDING": true, "INCREMENT": true, "INCREMENTAL": true, "INDEXTYPES": true,
		"INDICATOR": true, "INITIAL": true, "INITIALIZED": true, "INITIALLY": true,
		"INITRANS": true, "INSTANCE": true, "INSTANCES": true, "INSTANTIABLE": true,
		"INSTEAD": true, "INTERMEDIATE": true, "ISOLATION": true, "JAVA": true,
		"JOB": true, "KEEP": true, "KEY": true, "KEYS": true,
		"KEYSIZE": true, "LABEL": true, "LANGUAGE": true, "LAST": true,
		"LAYER": true, "LDAP_REG_SYNC_INTERVAL": true, "LDAP_REGISTRATION": true, "LDAP_REGISTRATION_ENABLED": true,
		"LEADING": true, "LEVEL": true, "LIBRARY": true, "LIMIT": true,
		"LINK": true, "LIST": true, "LOB": true, "LOCAL": true,
		"LOCATION": true, "LOCATOR": true, "LOCK": true, "LOCKED": true,
		"LOG": true, "LOGFILE": true, "LOGGING": true, "LOGICAL": true,
		"LOGICAL_READS_PER_CALL": true, "LOGICAL_READS_PER_SESSION": true, "LOGON": true, "MANAGE": true,
		"MANAGED": true, "MANAGEMENT": true, "MANUAL": true, "MAP": true,
		"MAPPING": true, "MASTER": true, "MATCHED": true, "MATERIALIZED": true,
		"MAXARCHLOGS": true, "MAXDATAFILES": true, "MAXEXTENTS": true, "MAXIMIZE": true,
		"MAXINSTANCES": true, "MAXLOGFILES": true, "MAXLOGHISTORY": true, "MAXLOGMEMBERS": true,
		"MAXSIZE": true, "MAXTRANS": true, "MAXVALUE": true, "MEMBER": true,
		"MEMORY": true, "MERGE": true, "MINEXTENTS": true, "MINIMIZE": true,
		"MINIMUM": true, "MINVALUE": true, "MINUTE": true, "MLSLABEL": true,
		"MODE": true, "MODIFY": true, "MONITORING": true, "MONTH": true,
		"MOUNT": true, "MOVE": true, "MOVEMENT": true, "MULTISET": true,
		"NAME": true, "NAMED": true, "NAMESPACE": true, "NAN": true,
		"NATIONAL": true, "NCHAR": true, "NCHAR_CS": true, "NCLOB": true,
		"NEEDED": true, "NESTED": true, "NETWORK": true, "NEW": true,
		"NEXT": true, "NO": true, "NOARCHIVELOG": true, "NOAUDIT": true,
		"NOCACHE": true, "NOCOMPRESS": true, "NOCYCLE": true, "NODELAY": true,
		"NOFORCE": true, "NOLOGGING": true, "NOMAPPING": true, "NOMAXVALUE": true,
		"NOMINIMIZE": true, "NOMINVALUE": true, "NOMONITORING": true, "NONE": true,
		"NOPARALLEL": true, "NORELY": true, "NOREVERSE": true, "NORMAL": true,
		"NOROWDEPENDENCIES": true, "NOSORT": true, "NOTHING": true, "NOVALIDATE": true,
		"NULLS": true, "OBJECT": true, "OFF": true, "OFFLINE": true,
		"OID": true, "OIDINDEX": true, "OLD": true, "ONLINE": true,
		"ONLY": true, "OPCODE": true, "OPEN": true, "OPERATOR": true,
		"OPTIMAL": true, "OPTION": true, "ORGANIZATION": true, "OUT": true,
		"OUTER": true, "OUTLINE": true, "OVERFLOW": true, "OVERRIDING": true,
		"PACKAGE": true, "PACKAGES": true, "PARALLEL": true, "PARALLEL_ENABLE": true,
		"PARAMETERS": true, "PARENT": true, "PARTITION": true, "PARTITIONS": true,
		"PASSWORD": true, "PASSWORD_GRACE_TIME": true, "PASSWORD_LIFE_TIME": true, "PASSWORD_LOCK_TIME": true,
		"PASSWORD_REUSE_MAX": true, "PASSWORD_REUSE_TIME": true, "PASSWORD_VERIFY_FUNCTION": true, "PATH": true,
		"PCTFREE": true, "PCTINCREASE": true, "PCTTHRESHOLD": true, "PCTUSED": true,
		"PCTVERSION": true, "PERCENT_RANK": true, "PERFORMANCE": true, "PERMANENT": true,
		"PFILE": true, "PHYSICAL": true, "PIV_GB": true, "PIV_SSF": true,
		"PLAN": true, "PLSQL_CCFLAGS": true, "PLSQL_CODE_TYPE": true, "PLSQL_DEBUG": true,
		"PLSQL_OPTIMIZE_LEVEL": true, "PLSQL_WARNINGS": true, "POLICY": true, "POST_TRANSACTION": true,
		"POWER": true, "PRAGMA": true, "PREBUILT": true, "PRECEDING": true,
		"PRESERVE": true, "PRESERVE_OID": true, "PRIMARY": true, "PRIOR": true,
		"PRIVATE": true, "PRIVATE_SGA": true, "PRIVILEGE": true, "PRIVILEGES": true,
		"PROCEDURE": true, "PROFILE": true, "PROGRAM": true, "PROJECT": true,
		"PROTECTED": true, "PROTECTION": true, "PUBLIC": true, "PURGE": true,
		"QUERY": true, "QUIESCE": true, "QUOTA": true, "RANDOM": true,
		"RANGE": true, "RAW": true, "READ": true, "READS": true,
		"REBUILD": true, "RECORDS_PER_BLOCK": true, "RECOVER": true, "RECOVERY": true,
		"RECYCLEBIN": true, "REDUCED": true, "REDUNDANCY": true, "REF": true,
		"REFERENCE": true, "REFERENCED": true, "REFERENCES": true, "REFERENCING": true,
		"REFRESH": true, "REGISTER": true, "REGISTERED": true, "REJECT": true,
		"RELY": true, "REMOTE_DEPENDENCIES_MODE": true, "RENAME": true, "REPAIR": true,
		"REPLACE": true, "REQUIRED": true, "RESET": true, "RESETLOGS": true,
		"RESIZE": true, "RESOLVE": true, "RESOLVER": true, "RESOURCE": true,
		"RESTRICT": true, "RESTRICTED": true, "RESUMABLE": true, "RESUME": true,
		"RETENTION": true, "RETURN": true, "RETURNING": true, "REUSE": true,
		"REVERSE": true, "REVOKE": true, "REWRITE": true, "ROLE": true,
		"ROLES": true, "ROLLBACK": true, "ROLLUP": true, "ROW": true,
		"ROWDEPENDENCIES": true, "ROWID": true, "ROWNUM": true, "ROWS": true,
		"RULE": true, "SAMPLE": true, "SAVEPOINT": true, "SCAN": true,
		"SCAN_INSTANCES": true, "SCHEDULER": true, "SCHEMA": true, "SCN": true,
		"SCOPE": true, "SECOND": true, "SECURITY": true, "SEGMENT": true,
		"SELF": true, "SEQUENCE": true, "SERIALIZABLE": true, "SESSION": true,
		"SESSIONS_PER_USER": true, "SHARE": true, "SHARED": true, "SHARED_POOL": true,
		"SHRINK": true, "SHUTDOWN": true, "SIBLINGS": true, "SINGLE": true,
		"SIZE": true, "SKIP": true, "SORT": true, "SOURCE": true,
		"SPACE": true, "SPECIFICATION": true, "SPFILE": true, "SPLIT": true,
		"STANDBY": true, "START": true, "STATEMENT_ID": true, "STATIC": true,
		"STATISTICS": true, "STOP": true, "STORAGE": true, "STORE": true,
		"STRUCTURE": true, "SUBMULTISET": true, "SUBPARTITION": true, "SUBPARTITIONS": true,
		"SUBSTITUTABLE": true, "SUBTYPE": true, "SUCCESS": true, "SUSPEND": true,
		"SWITCH": true, "SWITCHOVER": true, "SYS_OP_BITVEC": true, "SYS_OP_ENFORCE_NOT_NULL$": true,
		"SYS_OP_NTCIMG$": true, "SYNONYM": true, "SYSDATE": true, "SYSDBA": true,
		"SYSOPER": true, "SYSTEM": true, "TABLES": true, "TABLESPACE": true,
		"TEMPFILE": true, "TEMPORARY": true, "TEMPORARY_TABLESPACE": true, "TERMINATE": true,
		"THREAD": true, "THROUGH": true, "TIES": true, "TIME": true,
		"TIMEOUT": true, "TIMEZONE_ABBR": true, "TIMEZONE_HOUR": true, "TIMEZONE_MINUTE": true,
		"TIMEZONE_REGION": true, "TRACE": true, "TRANSACTION": true, "TRANSITIONAL": true,
		"TRIGGERS": true, "TRUNCATE": true, "TRUSTED": true, "TUNING": true,
		"TX": true, "TYPE": true, "TYPES": true, "UID": true,
		"UNARCHIVED": true, "UNBOUNDED": true, "UNDEFINE": true, "UNDER": true,
		"UNDO": true, "UNDROP": true, "UNIFORM": true, "UNLIMITED": true,
		"UNLOCK": true, "UNPACKED": true, "UNPROTECTED": true, "UNQUIESCE": true,
		"UNRECOVERABLE": true, "UNTIL": true, "UNUSABLE": true, "UNUSED": true,
		"UPDATABLE": true, "UPGRADE": true, "USAGE": true, "USE": true,
		"USING": true, "VALIDATE": true, "VALIDATION": true, "VALUE": true,
		"VARRAY": true, "VARYING": true, "VERIFY": true, "VIRTUAL": true,
		"VISIBLE": true, "WAIT": true, "WALLET": true, "WELLFORMED": true,
		"WORK": true, "WRAPPED": true, "WRITE": true, "XMLATTRIBUTES": true,
		"XMLCOLATTVAL": true, "XMLELEMENT": true, "XMLFOREST": true, "XMLPARSE": true,
		"XMLROOT": true, "XMLSCHEMA": true, "XMLSERIALIZE": true, "XMLTABLE": true,
		"XMLTYPE": true, "YEAR": true, "YES": true, "ZONE": true,
	}

	// Suspicious patterns that might indicate SQL injection
	suspiciousRegexQueryPatterns = []string{
		`--`, `\/\*`, `\*\/`, `; `,
		`\bunion\b`, `\bdrop\b`,
		`\bexec\b`, `\bexecute\b`, `\btruncate\b`,
		`\bcreate\b`, `\balter\b`, `\bgrant\b`,
		`\brevoke\b`, `\bcommit\b`, `\brollback\b`,
		`\binto outfile\b`, `\binto dumpfile\b`,
		`\bload_file\b`, `\bsleep\b`, `\bbenchmark\b`,
		`\bwaitfor\b`, `\bdelay\b`, `\bsys_eval\b`,
		`\binformation_schema\b`, `\bsysobjects\b`,
		`\bxp_\w*\b`, `\bsp_\w*\b`, `\bdeclare\b`,
		`\b\d+\s*=\s*\d+\b`,
	}

	// Maximum identifier lengths per dialect
	maxIdentifierLengths = map[DXDatabaseType]int{
		PostgreSQL: 63,
		SQLServer:  128,
		Oracle:     128,
		MariaDB:    64,
	}

	// Valid operators for each dialect
	validOperators = map[DXDatabaseType]map[string]bool{
		PostgreSQL: {
			"=": true, "!=": true, ">": true, "<": true, ">=": true, "<=": true,
			"like": true, "ilike": true, "in": true, "not in": true,
			"is null": true, "is not null": true,
		},
		MariaDB: {
			"=": true, "!=": true, ">": true, "<": true, ">=": true, "<=": true,
			"like": true, "in": true, "not in": true,
			"is null": true, "is not null": true,
		},
		SQLServer: {
			"=": true, "!=": true, ">": true, "<": true, ">=": true, "<=": true,
			"like": true, "in": true, "not in": true,
			"is null": true, "is not null": true,
		},
		Oracle: {
			"=": true, "!=": true, ">": true, "<": true, ">=": true, "<=": true,
			"like": true, "in": true, "not in": true,
			"is null": true, "is not null": true,
		},
	}

	mariadbKeywordsCanBeUsedAsFieldNameDirectly = map[string]bool{
		// Non-reserved MariaDB/MySQL keywords - safe to use as identifiers
		"ACCOUNT":                       true,
		"ACTION":                        true,
		"ADMIN":                         true,
		"AFTER":                         true,
		"AGGREGATE":                     true,
		"ALGORITHM":                     true,
		"ANY":                           true,
		"AT":                            true,
		"AUTHORS":                       true,
		"AUTO_INCREMENT":                true,
		"AUTOEXTEND_SIZE":               true,
		"AVG":                           true,
		"AVG_ROW_LENGTH":                true,
		"BACKUP":                        true,
		"BEGIN":                         true,
		"BINLOG":                        true,
		"BIT":                           true,
		"BLOCK":                         true,
		"BOOL":                          true,
		"BOOLEAN":                       true,
		"BTREE":                         true,
		"CACHE":                         true,
		"CASCADED":                      true,
		"CHAIN":                         true,
		"CHANGE":                        true,
		"CHANGED":                       true,
		"CHANNEL":                       true,
		"CHARSET":                       true,
		"CHECKSUM":                      true,
		"CIPHER":                        true,
		"CLIENT":                        true,
		"COALESCE":                      true,
		"CODE":                          true,
		"COLLATION":                     true,
		"COLUMNS":                       true,
		"COMMENT":                       true,
		"COMMIT":                        true,
		"COMMITTED":                     true,
		"COMPACT":                       true,
		"COMPLETION":                    true,
		"COMPRESSED":                    true,
		"COMPRESSION":                   true,
		"CONCURRENT":                    true,
		"CONNECTION":                    true,
		"CONSISTENT":                    true,
		"CONTAINS":                      true,
		"CONTEXT":                       true,
		"CONTRIBUTORS":                  true,
		"COPY":                          true,
		"CPU":                           true,
		"DATA":                          true,
		"DATAFILE":                      true,
		"DEALLOCATE":                    true,
		"DEFINER":                       true,
		"DELAY_KEY_WRITE":               true,
		"DELAYED":                       true,
		"DELIMITER":                     true,
		"DES_KEY_FILE":                  true,
		"DIRECTORY":                     true,
		"DISABLE":                       true,
		"DISCARD":                       true,
		"DISK":                          true,
		"DO":                            true,
		"DUMPFILE":                      true,
		"DUPLICATE":                     true,
		"DYNAMIC":                       true,
		"ENABLE":                        true,
		"ENCLOSED":                      true,
		"ENCRYPTION":                    true,
		"ENGINE":                        true,
		"ENGINES":                       true,
		"ERROR":                         true,
		"ERRORS":                        true,
		"ESCAPE":                        true,
		"EVENT":                         true,
		"EVENTS":                        true,
		"EVERY":                         true,
		"EXCHANGE":                      true,
		"EXECUTE":                       true,
		"EXPANSION":                     true,
		"EXPIRE":                        true,
		"EXPORT":                        true,
		"EXTENDED":                      true,
		"EXTENT_SIZE":                   true,
		"FAST":                          true,
		"FAULTS":                        true,
		"FIELDS":                        true,
		"FILE":                          true,
		"FILE_BLOCK_SIZE":               true,
		"FILTER":                        true,
		"FIRST":                         true,
		"FIXED":                         true,
		"FLUSH":                         true,
		"FOLLOWS":                       true,
		"FORMAT":                        true,
		"FOUND":                         true,
		"GENERAL":                       true,
		"GEOMETRY":                      true,
		"GEOMETRYCOLLECTION":            true,
		"GET_FORMAT":                    true,
		"GLOBAL":                        true,
		"GRANTS":                        true,
		"GROUP_REPLICATION":             true,
		"HANDLER":                       true,
		"HASH":                          true,
		"HELP":                          true,
		"HOST":                          true,
		"HOSTS":                         true,
		"IDENTIFIED":                    true,
		"IGNORE_SERVER_IDS":             true,
		"IMPORT":                        true,
		"INDEXES":                       true,
		"INITIAL_SIZE":                  true,
		"INNOBASE":                      true,
		"INNODB":                        true,
		"IO":                            true,
		"IO_THREAD":                     true,
		"IPC":                           true,
		"ISOLATION":                     true,
		"ISSUER":                        true,
		"JSON":                          true,
		"KEY_BLOCK_SIZE":                true,
		"LANGUAGE":                      true,
		"LAST":                          true,
		"LEAVES":                        true,
		"LESS":                          true,
		"LINESTRING":                    true,
		"LIST":                          true,
		"LOCAL":                         true,
		"LOGFILE":                       true,
		"LOGS":                          true,
		"MASTER":                        true,
		"MASTER_AUTO_POSITION":          true,
		"MASTER_CONNECT_RETRY":          true,
		"MASTER_DELAY":                  true,
		"MASTER_HEARTBEAT_PERIOD":       true,
		"MASTER_HOST":                   true,
		"MASTER_LOG_FILE":               true,
		"MASTER_LOG_POS":                true,
		"MASTER_PASSWORD":               true,
		"MASTER_PORT":                   true,
		"MASTER_RETRY_COUNT":            true,
		"MASTER_SERVER_ID":              true,
		"MASTER_SSL":                    true,
		"MASTER_SSL_CA":                 true,
		"MASTER_SSL_CAPATH":             true,
		"MASTER_SSL_CERT":               true,
		"MASTER_SSL_CIPHER":             true,
		"MASTER_SSL_CRL":                true,
		"MASTER_SSL_CRLPATH":            true,
		"MASTER_SSL_KEY":                true,
		"MASTER_SSL_VERIFY_SERVER_CERT": true,
		"MASTER_TLS_VERSION":            true,
		"MASTER_USER":                   true,
		"MAX_CONNECTIONS_PER_HOUR":      true,
		"MAX_QUERIES_PER_HOUR":          true,
		"MAX_ROWS":                      true,
		"MAX_SIZE":                      true,
		"MAX_UPDATES_PER_HOUR":          true,
		"MAX_USER_CONNECTIONS":          true,
		"MEDIUM":                        true,
		"MEMORY":                        true,
		"MERGE":                         true,
		"MESSAGE_TEXT":                  true,
		"MICROSECOND":                   true,
		"MIGRATE":                       true,
		"MIN_ROWS":                      true,
		"MODE":                          true,
		"MODIFY":                        true,
		"MULTILINESTRING":               true,
		"MULTIPOINT":                    true,
		"MULTIPOLYGON":                  true,
		"MUTEX":                         true,
		"MYSQL_ERRNO":                   true,
		"NAME":                          true,
		"NAMES":                         true,
		"NATIONAL":                      true,
		"NCHAR":                         true,
		"NDB":                           true,
		"NDBCLUSTER":                    true,
		"NEVER":                         true,
		"NEXT":                          true,
		"NO":                            true,
		"NODEGROUP":                     true,
		"NONE":                          true,
		"NOWAIT":                        true,
		"NO_WAIT":                       true,
		"NVARCHAR":                      true,
		"OFFSET":                        true,
		"OJ":                            true,
		"OLD_PASSWORD":                  true,
		"ONE":                           true,
		"ONLY":                          true,
		"OPEN":                          true,
		"OPTIMIZER_COSTS":               true,
		"OPTIONS":                       true,
		"OWNER":                         true,
		"PACK_KEYS":                     true,
		"PAGE":                          true,
		"PARSER":                        true,
		"PARTIAL":                       true,
		"PARTITIONING":                  true,
		"PARTITIONS":                    true,
		"PASSWORD":                      true,
		"PHASE":                         true,
		"PLUGIN":                        true,
		"PLUGIN_DIR":                    true,
		"PLUGINS":                       true,
		"POINT":                         true,
		"POLYGON":                       true,
		"PORT":                          true,
		"PRECEDES":                      true,
		"PREPARE":                       true,
		"PRESERVE":                      true,
		"PREV":                          true,
		"PROCESSLIST":                   true,
		"PROFILE":                       true,
		"PROFILES":                      true,
		"PROXY":                         true,
		"QUERY":                         true,
		"QUICK":                         true,
		"REBUILD":                       true,
		"RECOVER":                       true,
		"REDO_BUFFER_SIZE":              true,
		"REDUNDANT":                     true,
		"RELAY":                         true,
		"RELAYLOG":                      true,
		"RELAY_LOG_FILE":                true,
		"RELAY_LOG_POS":                 true,
		"REMOVE":                        true,
		"REORGANIZE":                    true,
		"REPAIR":                        true,
		"REPEATABLE":                    true,
		"REPLICATE_DO_DB":               true,
		"REPLICATE_DO_TABLE":            true,
		"REPLICATE_IGNORE_DB":           true,
		"REPLICATE_IGNORE_TABLE":        true,
		"REPLICATE_REWRITE_DB":          true,
		"REPLICATE_WILD_DO_TABLE":       true,
		"REPLICATE_WILD_IGNORE_TABLE":   true,
		"REPLICATION":                   true,
		"RESET":                         true,
		"RESTART":                       true,
		"RESTORE":                       true,
		"RESUME":                        true,
		"RETURNS":                       true,
		"ROLLBACK":                      true,
		"ROLLUP":                        true,
		"ROUTINE":                       true,
		"ROW":                           true,
		"ROWS":                          true,
		"ROW_FORMAT":                    true,
		"RTREE":                         true,
		"SAVEPOINT":                     true,
		"SCHEDULE":                      true,
		"SCHEDULER":                     true,
		"SECURITY":                      true,
		"SERIAL":                        true,
		"SERVER":                        true,
		"SESSION":                       true,
		"SHARE":                         true,
		"SHUTDOWN":                      true,
		"SIGNED":                        true,
		"SIMPLE":                        true,
		"SLAVE":                         true,
		"SLOW":                          true,
		"SNAPSHOT":                      true,
		"SOCKET":                        true,
		"SOME":                          true,
		"SONAME":                        true,
		"SOUNDS":                        true,
		"SOURCE":                        true,
		"SPATIAL":                       true,
		"SQLEXCEPTION":                  true,
		"SQLSTATE":                      true,
		"SQLWARNING":                    true,
		"SQL_AFTER_GTIDS":               true,
		"SQL_AFTER_MTS_GAPS":            true,
		"SQL_BEFORE_GTIDS":              true,
		"SQL_BIG_RESULT":                true,
		"SQL_BUFFER_RESULT":             true,
		"SQL_CACHE":                     true,
		"SQL_CALC_FOUND_ROWS":           true,
		"SQL_NO_CACHE":                  true,
		"SQL_SMALL_RESULT":              true,
		"SQL_THREAD":                    true,
		"SSL":                           true,
		"STACKED":                       true,
		"STARTING":                      true,
		"STARTS":                        true,
		"STATS_AUTO_RECALC":             true,
		"STATS_PERSISTENT":              true,
		"STATS_SAMPLE_PAGES":            true,
		"STATUS":                        true,
		"STOP":                          true,
		"STORAGE":                       true,
		"STORED":                        true,
		"STRING":                        true,
		"SUBJECT":                       true,
		"SUBPARTITION":                  true,
		"SUBPARTITIONS":                 true,
		"SUPER":                         true,
		"SUSPEND":                       true,
		"SWAPS":                         true,
		"SWITCHES":                      true,
		"TABLESPACE":                    true,
		"TEMPORARY":                     true,
		"TEMPTABLE":                     true,
		"THAN":                          true,
		"TRANSACTION":                   true,
		"TRUNCATE":                      true,
		"TYPE":                          true,
		"TYPES":                         true,
		"UNCOMMITTED":                   true,
		"UNDEFINED":                     true,
		"UNDO":                          true,
		"UNDOFILE":                      true,
		"UNDO_BUFFER_SIZE":              true,
		"UNICODE":                       true,
		"UNINSTALL":                     true,
		"UNKNOWN":                       true,
		"UNTIL":                         true,
		"UPGRADE":                       true,
		"USAGE":                         true,
		"USE":                           true,
		"USER_RESOURCES":                true,
		"USE_FRM":                       true,
		"VALIDATION":                    true,
		"VALUE":                         true,
		"VARIABLES":                     true,
		"VCPU":                          true,
		"VIRTUAL":                       true,
		"VISIBLE":                       true,
		"WAIT":                          true,
		"WARNINGS":                      true,
		"WEEK":                          true,
		"WEIGHT_STRING":                 true,
		"WITHOUT":                       true,
		"WORK":                          true,
		"WRAPPER":                       true,
		"X509":                          true,
		"XA":                            true,
		"XID":                           true,
		"XML":                           true,
		"YEAR":                          true,
		// MariaDB specific non-reserved keywords
		"CUME_DIST":    true,
		"DENSE_RANK":   true,
		"EMPTY":        true,
		"FIRST_VALUE":  true,
		"GROUPING":     true,
		"JSON_TABLE":   true,
		"LAG":          true,
		"LAST_VALUE":   true,
		"LEAD":         true,
		"NTH_VALUE":    true,
		"NTILE":        true,
		"OF":           true,
		"OVER":         true,
		"PERCENT_RANK": true,
		"PERSIST":      true,
		"PERSIST_ONLY": true,
		"PERCENT":      true,
		"RANK":         true,
		"RECURSIVE":    true,
		"ROW_NUMBER":   true,
		"SYSTEM":       true,
	}

	// sqlServerKeywordsCanBeUsedAsFieldNameDirectly contains SQL Server keywords that are
	// non-reserved and can be used as column/table names without brackets.
	// Based on SQL Server T-SQL official documentation
	sqlServerKeywordsCanBeUsedAsFieldNameDirectly = map[string]bool{
		// Non-reserved SQL Server keywords - safe to use as identifiers
		"ABSOLUTE":                              true,
		"ACTION":                                true,
		"ADMIN":                                 true,
		"AFTER":                                 true,
		"AGGREGATE":                             true,
		"ALGORITHM":                             true,
		"ALLOW_SNAPSHOT_ISOLATION":              true,
		"ANSI_NULLS":                            true,
		"ANSI_PADDING":                          true,
		"ANSI_WARNINGS":                         true,
		"APPLICATION_LOG":                       true,
		"APPLY":                                 true,
		"ASSEMBLY":                              true,
		"ASYMMETRIC":                            true,
		"ATOMIC":                                true,
		"AUDIT":                                 true,
		"AUTO":                                  true,
		"AUTO_CLEANUP":                          true,
		"AUTO_CLOSE":                            true,
		"AUTO_CREATE_STATISTICS":                true,
		"AUTO_SHRINK":                           true,
		"AUTO_UPDATE_STATISTICS":                true,
		"AVAILABILITY":                          true,
		"BACKUP":                                true,
		"BEFORE":                                true,
		"BEGIN":                                 true,
		"BINDING":                               true,
		"BLOB_STORAGE":                          true,
		"BROKER":                                true,
		"BROKER_INSTANCE":                       true,
		"BULK":                                  true,
		"CALLER":                                true,
		"CAP_CPU_PERCENT":                       true,
		"CATALOG":                               true,
		"CATCH":                                 true,
		"CHANGE_RETENTION":                      true,
		"CHANGE_TRACKING":                       true,
		"CHECKSUM":                              true,
		"CLASSIFIER_FUNCTION":                   true,
		"CLEANUP":                               true,
		"COLLECTION":                            true,
		"COLUMNSTORE":                           true,
		"COMMITTED":                             true,
		"COMPATIBILITY_LEVEL":                   true,
		"COMPRESS_ALL_ROW_GROUPS":               true,
		"COMPRESSION":                           true,
		"CONCAT":                                true,
		"CONCAT_NULL_YIELDS_NULL":               true,
		"CONTROL":                               true,
		"COOKIE":                                true,
		"CURSOR_CLOSE_ON_COMMIT":                true,
		"CURSOR_DEFAULT":                        true,
		"DATA":                                  true,
		"DATA_COMPRESSION":                      true,
		"DATA_PURITY":                           true,
		"DATABASE_MIRRORING":                    true,
		"DATE_CORRELATION_OPTIMIZATION":         true,
		"DATEADD":                               true,
		"DATEDIFF":                              true,
		"DATENAME":                              true,
		"DATEPART":                              true,
		"DAYS":                                  true,
		"DB_CHAINING":                           true,
		"DBCC":                                  true,
		"DEALLOCATE":                            true,
		"DECLARE":                               true,
		"DEFAULT_DATABASE":                      true,
		"DEFAULT_FULLTEXT_LANGUAGE":             true,
		"DEFAULT_LANGUAGE":                      true,
		"DEFAULT_SCHEMA":                        true,
		"DELAYED_DURABILITY":                    true,
		"DENY":                                  true,
		"DIRECTORY_NAME":                        true,
		"DISABLE":                               true,
		"DISABLED":                              true,
		"DISABLE_BROKER":                        true,
		"DISK":                                  true,
		"DISTRIBUTED":                           true,
		"DUMP":                                  true,
		"DURABILITY":                            true,
		"ELEMENTS":                              true,
		"EMERGENCY":                             true,
		"ENABLE":                                true,
		"ENABLE_BROKER":                         true,
		"ENCRYPTED":                             true,
		"ENCRYPTION":                            true,
		"ENDPOINT":                              true,
		"ERRLVL":                                true,
		"ERROR_BROKER_CONVERSATIONS":            true,
		"ESCAPE":                                true,
		"EVENT":                                 true,
		"EVENTDATA":                             true,
		"EXCLUSIVE":                             true,
		"EXECUTABLE":                            true,
		"EXECUTE":                               true,
		"EXIT":                                  true,
		"EXPANDABLE":                            true,
		"FAST":                                  true,
		"FAST_FORWARD":                          true,
		"FETCH":                                 true,
		"FILE":                                  true,
		"FILEGROUP":                             true,
		"FILEGROWTH":                            true,
		"FILENAME":                              true,
		"FILESTREAM":                            true,
		"FILLFACTOR":                            true,
		"FILTER":                                true,
		"FIRST":                                 true,
		"FOLLOWING":                             true,
		"FORCE":                                 true,
		"FORCED":                                true,
		"FORCESEEK":                             true,
		"FORWARD_ONLY":                          true,
		"FREE":                                  true,
		"FULLSCAN":                              true,
		"FULLTEXT":                              true,
		"GB":                                    true,
		"GETDATE":                               true,
		"GETUTCDATE":                            true,
		"GO":                                    true,
		"GOVERNOR":                              true,
		"GROUP_MAX_REQUESTS":                    true,
		"HADR":                                  true,
		"HASH":                                  true,
		"HASHED":                                true,
		"HEALTHCHECKTIMEOUT":                    true,
		"HONOR_BROKER_PRIORITY":                 true,
		"HOURS":                                 true,
		"IDENTITY_INSERT":                       true,
		"IGNORE_CONSTRAINTS":                    true,
		"IGNORE_DUP_KEY":                        true,
		"IGNORE_NONCLUSTERED_COLUMNSTORE_INDEX": true,
		"IGNORE_TRIGGERS":                       true,
		"IMMEDIATE":                             true,
		"IMPERSONATE":                           true,
		"INCLUDE":                               true,
		"INCREMENT":                             true,
		"INCREMENTAL":                           true,
		"INFINITE":                              true,
		"INIT":                                  true,
		"INSENSITIVE":                           true,
		"INSERTED":                              true,
		"ISOLATION":                             true,
		"KB":                                    true,
		"KEEP":                                  true,
		"KEEPDEFAULTS":                          true,
		"KEEPFIXED":                             true,
		"KEEPIDENTITY":                          true,
		"KEY":                                   true,
		"KEYSET":                                true,
		"LANGUAGE":                              true,
		"LAST":                                  true,
		"LEVEL":                                 true,
		"LIBRARY":                               true,
		"LIFETIME":                              true,
		"LINKED":                                true,
		"LINUX":                                 true,
		"LISTENER":                              true,
		"LISTENER_URL":                          true,
		"LOB_COMPACTION":                        true,
		"LOCAL":                                 true,
		"LOCAL_SERVICE_NAME":                    true,
		"LOCK":                                  true,
		"LOCK_ESCALATION":                       true,
		"LOG":                                   true,
		"LOGIN":                                 true,
		"LOOP":                                  true,
		"MANUAL":                                true,
		"MARK":                                  true,
		"MASTER":                                true,
		"MAX":                                   true,
		"MAX_CPU_PERCENT":                       true,
		"MAX_DOP":                               true,
		"MAX_FILES":                             true,
		"MAX_MEMORY":                            true,
		"MAX_MEMORY_PERCENT":                    true,
		"MAX_PROCESSES":                         true,
		"MAX_QUEUE_READERS":                     true,
		"MAX_ROLLOVER_FILES":                    true,
		"MAXDOP":                                true,
		"MAXRECURSION":                          true,
		"MAXSIZE":                               true,
		"MB":                                    true,
		"MEDIUM":                                true,
		"MEMORY_OPTIMIZED":                      true,
		"MESSAGE":                               true,
		"MIN":                                   true,
		"MIN_ACTIVE_ROWVERSION":                 true,
		"MIN_CPU_PERCENT":                       true,
		"MIN_MEMORY_PERCENT":                    true,
		"MINUTES":                               true,
		"MIRROR":                                true,
		"MIRROR_ADDRESS":                        true,
		"MIXED_PAGE_ALLOCATION":                 true,
		"MODE":                                  true,
		"MODIFY":                                true,
		"MOVE":                                  true,
		"MULTI_USER":                            true,
		"NAME":                                  true,
		"NATIVE_COMPILATION":                    true,
		"NESTED_TRIGGERS":                       true,
		"NEW_BROKER":                            true,
		"NEWNAME":                               true,
		"NEXT":                                  true,
		"NO":                                    true,
		"NO_CHECKSUM":                           true,
		"NO_COMPRESSION":                        true,
		"NO_EVENT_LOSS":                         true,
		"NO_TRUNCATE":                           true,
		"NO_WAIT":                               true,
		"NOCOUNT":                               true,
		"NODES":                                 true,
		"NOEXPAND":                              true,
		"NON_TRANSACTED_ACCESS":                 true,
		"NORECOMPUTE":                           true,
		"NORECOVERY":                            true,
		"NOWAIT":                                true,
		"NTILE":                                 true,
		"NUMANODE":                              true,
		"NUMERIC_ROUNDABORT":                    true,
		"OBJECT":                                true,
		"OFFLINE":                               true,
		"OFFSET":                                true,
		"OLD_PASSWORD":                          true,
		"ONLINE":                                true,
		"ONLY":                                  true,
		"OPEN":                                  true,
		"OPEN_EXISTING":                         true,
		"OPENQUERY":                             true,
		"OPENROWSET":                            true,
		"OPENXML":                               true,
		"OPTIMISTIC":                            true,
		"OPTIMIZE":                              true,
		"OPTIMIZE_FOR_SEQUENTIAL_KEY":           true,
		"OPTION":                                true,
		"OUT":                                   true,
		"OUTPUT":                                true,
		"OWNER":                                 true,
		"OWNERSHIP":                             true,
		"PAGE":                                  true,
		"PAGECOUNT":                             true,
		"PARTITION":                             true,
		"PARTITIONS":                            true,
		"PASSWORD":                              true,
		"PATH":                                  true,
		"PERCENT":                               true,
		"PERCENTILE_CONT":                       true,
		"PERCENTILE_DISC":                       true,
		"PERMISSION_SET":                        true,
		"PERSISTED":                             true,
		"PILOT":                                 true,
		"PIVOT":                                 true,
		"PLAN":                                  true,
		"POLICY":                                true,
		"POOL":                                  true,
		"POPULATION":                            true,
		"PRECEDING":                             true,
		"PREDICATE":                             true,
		"PRIMARY_ROLE":                          true,
		"PRIOR":                                 true,
		"PRIORITY":                              true,
		"PRIORITY_LEVEL":                        true,
		"PRIVATE":                               true,
		"PRIVILEGES":                            true,
		"PROCESS":                               true,
		"PROFILE":                               true,
		"PROPERTY":                              true,
		"PROVIDER":                              true,
		"QUERY_STORE":                           true,
		"QUEUE":                                 true,
		"QUOTED_IDENTIFIER":                     true,
		"RAISERROR":                             true,
		"RANGE":                                 true,
		"RC2":                                   true,
		"RC4":                                   true,
		"RC4_128":                               true,
		"READ_COMMITTED_SNAPSHOT":               true,
		"READ_ONLY":                             true,
		"READ_WRITE":                            true,
		"READCOMMITTED":                         true,
		"READCOMMITTEDLOCK":                     true,
		"READONLY":                              true,
		"READPAST":                              true,
		"READUNCOMMITTED":                       true,
		"READWRITE":                             true,
		"REBUILD":                               true,
		"RECEIVE":                               true,
		"RECOMPILE":                             true,
		"RECOVERY":                              true,
		"RECURSIVE_TRIGGERS":                    true,
		"REGENERATE":                            true,
		"RELATED_CONVERSATION":                  true,
		"RELATED_CONVERSATION_GROUP":            true,
		"RELATIVE":                              true,
		"REMOTE":                                true,
		"REMOTE_PROC_TRANSACTIONS":              true,
		"REMOTE_SERVICE_NAME":                   true,
		"REMOVE":                                true,
		"REORGANIZE":                            true,
		"REPEATABLE":                            true,
		"REPEATABLEREAD":                        true,
		"REPLACE":                               true,
		"REPLICA":                               true,
		"REPLICATE":                             true,
		"REQUEST_MAX_CPU_TIME_SEC":              true,
		"REQUEST_MAX_MEMORY_GRANT_PERCENT":      true,
		"REQUEST_MEMORY_GRANT_TIMEOUT_SEC":      true,
		"REQUIRED_SYNCHRONIZED_SECONDARIES_TO_COMMIT": true,
		"RESAMPLE":                         true,
		"RESET":                            true,
		"RESTART":                          true,
		"RESTORE":                          true,
		"RESTRICT_GEOM_SPEC_TO_REGIONATOR": true,
		"RESTRICTED_USER":                  true,
		"RESUMABLE":                        true,
		"RETENTION":                        true,
		"RETURN":                           true,
		"RETURNS":                          true,
		"REVERT":                           true,
		"ROLLBACK":                         true,
		"ROUTE":                            true,
		"ROW":                              true,
		"ROWCOUNT":                         true,
		"ROWGUID":                          true,
		"ROWGUIDCOL":                       true,
		"ROWS":                             true,
		"ROWS_PER_BATCH":                   true,
		"RSA_1024":                         true,
		"RSA_2048":                         true,
		"RSA_3072":                         true,
		"RSA_4096":                         true,
		"RSA_512":                          true,
		"SAFETY":                           true,
		"SAFE":                             true,
		"SAMPLE":                           true,
		"SAVE":                             true,
		"SCHEDULER":                        true,
		"SCHEMA_ID":                        true,
		"SCHEMA_NAME":                      true,
		"SCHEMABINDING":                    true,
		"SCOPE":                            true,
		"SCROLL":                           true,
		"SCROLL_LOCKS":                     true,
		"SEARCH":                           true,
		"SECONDS":                          true,
		"SECONDARY":                        true,
		"SECONDARY_ONLY":                   true,
		"SECONDARY_ROLE":                   true,
		"SECURITY":                         true,
		"SECURITY_LOG":                     true,
		"SECURITYAUDIT":                    true,
		"SELECTIVE":                        true,
		"SELF":                             true,
		"SEND":                             true,
		"SENT":                             true,
		"SERIALIZABLE":                     true,
		"SERVER":                           true,
		"SERVICE":                          true,
		"SERVICE_BROKER":                   true,
		"SERVICE_NAME":                     true,
		"SESSION":                          true,
		"SESSION_TIMEOUT":                  true,
		"SETERROR":                         true,
		"SETS":                             true,
		"SETTINGS":                         true,
		"SHOWPLAN":                         true,
		"SHOWPLAN_ALL":                     true,
		"SHOWPLAN_TEXT":                    true,
		"SHOWPLAN_XML":                     true,
		"SHRINKLOG":                        true,
		"SHUTDOWN":                         true,
		"SID":                              true,
		"SIGNATURE":                        true,
		"SIMPLE":                           true,
		"SINGLE_USER":                      true,
		"SIZE":                             true,
		"SNAPSHOT":                         true,
		"SORT_IN_TEMPDB":                   true,
		"SOURCE":                           true,
		"SPARSE":                           true,
		"SPATIAL":                          true,
		"SPECIFICATION":                    true,
		"SPLIT":                            true,
		"SQL":                              true,
		"SQLDUMPERFLAGS":                   true,
		"SQLDUMPERUSERDUMPS":               true,
		"SQLDUMPERSETUPFLAGS":              true,
		"STANDBY":                          true,
		"START":                            true,
		"START_DATE":                       true,
		"STARTED":                          true,
		"STARTUP_STATE":                    true,
		"STATE":                            true,
		"STATIC":                           true,
		"STATISTICAL_SEMANTICS":            true,
		"STATISTICS":                       true,
		"STATISTICS_INCREMENTAL":           true,
		"STATISTICS_NORECOMPUTE":           true,
		"STATS":                            true,
		"STATS_STREAM":                     true,
		"STATUS":                           true,
		"STATUSONLY":                       true,
		"STOP":                             true,
		"STOPLIST":                         true,
		"STOPPED":                          true,
		"STRING_AGG":                       true,
		"SUBJECT":                          true,
		"SUBSCRIPTION":                     true,
		"SUPPORTED":                        true,
		"SUSPEND":                          true,
		"SWITCH":                           true,
		"SYMMETRIC":                        true,
		"SYNCHRONOUS_COMMIT":               true,
		"SYNONYM":                          true,
		"SYSTEM":                           true,
		"TAKE":                             true,
		"TARGET":                           true,
		"TARGET_RECOVERY_TIME":             true,
		"TB":                               true,
		"TEXTSIZE":                         true,
		"THROW":                            true,
		"TIES":                             true,
		"TIME":                             true,
		"TIMEOUT":                          true,
		"TIMER":                            true,
		"TOP":                              true,
		"TORN_PAGE_DETECTION":              true,
		"TRACK_CAUSALITY":                  true,
		"TRANSFER":                         true,
		"TRANSFORM_NOISE_WORDS":            true,
		"TRIPLE_DES":                       true,
		"TRIPLE_DES_3KEY":                  true,
		"TRUSTWORTHY":                      true,
		"TRY":                              true,
		"TSQL":                             true,
		"TWO_DIGIT_YEAR_CUTOFF":            true,
		"TYPE":                             true,
		"TYPE_WARNING":                     true,
		"UNBOUNDED":                        true,
		"UNCOMMITTED":                      true,
		"UNCHECKED":                        true,
		"UNDEFINED":                        true,
		"UNLIMITED":                        true,
		"UNMASK":                           true,
		"UNPIVOT":                          true,
		"UNSAFE":                           true,
		"URL":                              true,
		"USAGE":                            true,
		"USE":                              true,
		"USED":                             true,
		"USER_OPTIONS":                     true,
		"VALIDATION":                       true,
		"VALUE":                            true,
		"VIEWS":                            true,
		"WAIT":                             true,
		"WAITFOR":                          true,
		"WELL_FORMED_XML":                  true,
		"WINDOWS":                          true,
		"WITHOUT":                          true,
		"WITNESS":                          true,
		"WORK":                             true,
		"WORKLOAD":                         true,
		"XML":                              true,
		"XMLNAMESPACES":                    true,
		"XSINIL":                           true,
		"ZONE":                             true,
	}

	// oracleKeywordsCanBeUsedAsFieldNameDirectly contains Oracle keywords that are
	// non-reserved and can be used as column/table names without double quotes.
	// Based on Oracle Database official documentation
	oracleKeywordsCanBeUsedAsFieldNameDirectly = map[string]bool{
		// Non-reserved Oracle keywords - safe to use as identifiers
		"ACCOUNT":                   true,
		"ACTIVATE":                  true,
		"ADMIN":                     true,
		"ADMINISTRATOR":             true,
		"ADVISE":                    true,
		"AFTER":                     true,
		"ALGORITHM":                 true,
		"ANALYZE":                   true,
		"ARCHIVE":                   true,
		"ARCHIVELOG":                true,
		"ASSOCIATE":                 true,
		"AT":                        true,
		"ATTRIBUTE":                 true,
		"AUDIT":                     true,
		"AUTHENTICATED":             true,
		"AUTOALLOCATE":              true,
		"AUTOEXTEND":                true,
		"AUTOMATIC":                 true,
		"AVAILABILITY":              true,
		"BACKUP":                    true,
		"BECOME":                    true,
		"BEFORE":                    true,
		"BEGIN":                     true,
		"BEHALF":                    true,
		"BINDING":                   true,
		"BITMAP":                    true,
		"BLOCK":                     true,
		"BLOCKSIZE":                 true,
		"BODY":                      true,
		"CACHE":                     true,
		"CACHE_INSTANCES":           true,
		"CANCEL":                    true,
		"CATEGORY":                  true,
		"CERTIFICATE":               true,
		"CHAINED":                   true,
		"CHANGE":                    true,
		"CHAR_CS":                   true,
		"CHECKPOINT":                true,
		"CHILD":                     true,
		"CHUNK":                     true,
		"CLASS":                     true,
		"CLEAR":                     true,
		"CLONE":                     true,
		"CLOSE":                     true,
		"COMMENT":                   true,
		"COMMIT":                    true,
		"COMMITTED":                 true,
		"COMPATIBILITY":             true,
		"COMPILE":                   true,
		"COMPLETE":                  true,
		"COMPOSITE_LIMIT":           true,
		"COMPRESS":                  true,
		"COMPUTE":                   true,
		"CONNECT_TIME":              true,
		"CONSIDER":                  true,
		"CONSISTENT":                true,
		"CONTAINER":                 true,
		"CONTAINED":                 true,
		"CONTINUE":                  true,
		"CONTROLFILE":               true,
		"CORRUPTION":                true,
		"COST":                      true,
		"CPU_PER_CALL":              true,
		"CPU_PER_SESSION":           true,
		"CURSOR":                    true,
		"CYCLE":                     true,
		"DANGLING":                  true,
		"DATAFILE":                  true,
		"DATAFILES":                 true,
		"DATAOBJNO":                 true,
		"DBA":                       true,
		"DBHIGH":                    true,
		"DBLOW":                     true,
		"DBMAC":                     true,
		"DDL":                       true,
		"DEALLOCATE":                true,
		"DEBUG":                     true,
		"DEFINE":                    true,
		"DEFINITION":                true,
		"DEGREE":                    true,
		"DEREF":                     true,
		"DIRECT_LOAD":               true,
		"DIRECTORY":                 true,
		"DISABLE":                   true,
		"DISASSOCIATE":              true,
		"DISCONNECT":                true,
		"DISK":                      true,
		"DISKGROUP":                 true,
		"DISKS":                     true,
		"DISMOUNT":                  true,
		"DISPATCHERS":               true,
		"DISTRIBUTED":               true,
		"DML":                       true,
		"DOCUMENT":                  true,
		"DOMAIN":                    true,
		"DUMP":                      true,
		"EDIT_DISTANCE":             true,
		"EDITION":                   true,
		"ELEMENT":                   true,
		"ELIMINATE_DUPLICATES":      true,
		"ENABLE":                    true,
		"ENCRYPTION":                true,
		"ENFORCE":                   true,
		"ENTRY":                     true,
		"ERROR_ON_OVERLAP_TIME":     true,
		"ERRORS":                    true,
		"ESCAPE":                    true,
		"ESTIMATE":                  true,
		"EVENTS":                    true,
		"EXCEPTIONS":                true,
		"EXCHANGE":                  true,
		"EXCLUDING":                 true,
		"EXCLUSIVE":                 true,
		"EXECUTE":                   true,
		"EXPIRE":                    true,
		"EXPLAIN":                   true,
		"EXTENT":                    true,
		"EXTENTS":                   true,
		"EXTERNALLY":                true,
		"FAILED_LOGIN_ATTEMPTS":     true,
		"FAST":                      true,
		"FEATURE":                   true,
		"FILE":                      true,
		"FILESYSTEM_LIKE_LOGGING":   true,
		"FINAL":                     true,
		"FINISH":                    true,
		"FIRST":                     true,
		"FLUSH":                     true,
		"FOLLOWING":                 true,
		"FORCE":                     true,
		"FREELIST":                  true,
		"FREELISTS":                 true,
		"FREEPOOLS":                 true,
		"FRESH":                     true,
		"FUNCTIONS":                 true,
		"GENERATED":                 true,
		"GLOBAL":                    true,
		"GLOBAL_NAME":               true,
		"GLOBALLY":                  true,
		"GROUPS":                    true,
		"GUARANTEED":                true,
		"GUARD":                     true,
		"HASH":                      true,
		"HASHKEYS":                  true,
		"HEADER":                    true,
		"HEAP":                      true,
		"HIERARCHY":                 true,
		"HOUR":                      true,
		"IDENTIFIED":                true,
		"IDENTIFIER":                true,
		"IDGENERATORS":              true,
		"IDLE_TIME":                 true,
		"IMMEDIATE":                 true,
		"IMPLEMENTATION":            true,
		"INCLUDING":                 true,
		"INCREMENT":                 true,
		"INCREMENTAL":               true,
		"INDEXTYPES":                true,
		"INDICATOR":                 true,
		"INITIAL":                   true,
		"INITIALIZED":               true,
		"INITIALLY":                 true,
		"INITRANS":                  true,
		"INSTANCE":                  true,
		"INSTANCES":                 true,
		"INSTANTIABLE":              true,
		"INSTEAD":                   true,
		"INTERMEDIATE":              true,
		"ISOLATION":                 true,
		"JAVA":                      true,
		"JOB":                       true,
		"KEEP":                      true,
		"KEY":                       true,
		"KEYS":                      true,
		"KEYSIZE":                   true,
		"LABEL":                     true,
		"LANGUAGE":                  true,
		"LAST":                      true,
		"LAYER":                     true,
		"LDAP_REG_SYNC_INTERVAL":    true,
		"LDAP_REGISTRATION":         true,
		"LDAP_REGISTRATION_ENABLED": true,
		"LEADING":                   true,
		"LEVEL":                     true,
		"LIBRARY":                   true,
		"LINK":                      true,
		"LIST":                      true,
		"LOB":                       true,
		"LOCAL":                     true,
		"LOCATION":                  true,
		"LOCATOR":                   true,
		"LOCK":                      true,
		"LOCKED":                    true,
		"LOG":                       true,
		"LOGFILE":                   true,
		"LOGGING":                   true,
		"LOGICAL":                   true,
		"LOGICAL_READS_PER_CALL":    true,
		"LOGICAL_READS_PER_SESSION": true,
		"LOGON":                     true,
		"MANAGE":                    true,
		"MANAGED":                   true,
		"MANAGEMENT":                true,
		"MANUAL":                    true,
		"MAP":                       true,
		"MAPPING":                   true,
		"MASTER":                    true,
		"MATCHED":                   true,
		"MATERIALIZED":              true,
		"MAXARCHLOGS":               true,
		"MAXDATAFILES":              true,
		"MAXEXTENTS":                true,
		"MAXIMIZE":                  true,
		"MAXINSTANCES":              true,
		"MAXLOGFILES":               true,
		"MAXLOGHISTORY":             true,
		"MAXLOGMEMBERS":             true,
		"MAXSIZE":                   true,
		"MAXTRANS":                  true,
		"MAXVALUE":                  true,
		"MEMBER":                    true,
		"MEMORY":                    true,
		"MINEXTENTS":                true,
		"MINIMIZE":                  true,
		"MINIMUM":                   true,
		"MINVALUE":                  true,
		"MINUTE":                    true,
		"MLSLABEL":                  true,
		"MODE":                      true,
		"MODIFY":                    true,
		"MONITORING":                true,
		"MONTH":                     true,
		"MOUNT":                     true,
		"MOVE":                      true,
		"MOVEMENT":                  true,
		"MULTISET":                  true,
		"NAME":                      true,
		"NAMED":                     true,
		"NAMESPACE":                 true,
		"NAN":                       true,
		"NATIONAL":                  true,
		"NCHAR":                     true,
		"NCHAR_CS":                  true,
		"NCLOB":                     true,
		"NEEDED":                    true,
		"NESTED":                    true,
		"NETWORK":                   true,
		"NEW":                       true,
		"NEXT":                      true,
		"NO":                        true,
		"NOARCHIVELOG":              true,
		"NOAUDIT":                   true,
		"NOCACHE":                   true,
		"NOCOMPRESS":                true,
		"NOCYCLE":                   true,
		"NODELAY":                   true,
		"NOFORCE":                   true,
		"NOLOGGING":                 true,
		"NOMAPPING":                 true,
		"NOMAXVALUE":                true,
		"NOMINIMIZE":                true,
		"NOMINVALUE":                true,
		"NOMONITORING":              true,
		"NONE":                      true,
		"NOPARALLEL":                true,
		"NORELY":                    true,
		"NOREVERSE":                 true,
		"NORMAL":                    true,
		"NOROWDEPENDENCIES":         true,
		"NOSORT":                    true,
		"NOTHING":                   true,
		"NOVALIDATE":                true,
		"NULLS":                     true,
		"OBJECT":                    true,
		"OFF":                       true,
		"OFFLINE":                   true,
		"OID":                       true,
		"OIDINDEX":                  true,
		"OLD":                       true,
		"ONLINE":                    true,
		"ONLY":                      true,
		"OPCODE":                    true,
		"OPEN":                      true,
		"OPERATOR":                  true,
		"OPTIMAL":                   true,
		"OPTION":                    true,
		"ORGANIZATION":              true,
		"OUT":                       true,
		"OUTLINE":                   true,
		"OVERFLOW":                  true,
		"OVERRIDING":                true,
		"PACKAGE":                   true,
		"PACKAGES":                  true,
		"PARALLEL":                  true,
		"PARALLEL_ENABLE":           true,
		"PARAMETERS":                true,
		"PARENT":                    true,
		"PARTITION":                 true,
		"PARTITIONS":                true,
		"PASSWORD":                  true,
		"PASSWORD_GRACE_TIME":       true,
		"PASSWORD_LIFE_TIME":        true,
		"PASSWORD_LOCK_TIME":        true,
		"PASSWORD_REUSE_MAX":        true,
		"PASSWORD_REUSE_TIME":       true,
		"PASSWORD_VERIFY_FUNCTION":  true,
		"PATH":                      true,
		"PCTFREE":                   true,
		"PCTINCREASE":               true,
		"PCTTHRESHOLD":              true,
		"PCTUSED":                   true,
		"PCTVERSION":                true,
		"PERFORMANCE":               true,
		"PERMANENT":                 true,
		"PFILE":                     true,
		"PHYSICAL":                  true,
		"PIV_GB":                    true,
		"PIV_SSF":                   true,
		"PLAN":                      true,
		"PLSQL_CCFLAGS":             true,
		"PLSQL_CODE_TYPE":           true,
		"PLSQL_DEBUG":               true,
		"PLSQL_OPTIMIZE_LEVEL":      true,
		"PLSQL_WARNINGS":            true,
		"POLICY":                    true,
		"POST_TRANSACTION":          true,
		"POWER":                     true,
		"PRAGMA":                    true,
		"PREBUILT":                  true,
		"PRECEDING":                 true,
		"PRESERVE":                  true,
		"PRESERVE_OID":              true,
		"PRIOR":                     true,
		"PRIVATE":                   true,
		"PRIVATE_SGA":               true,
		"PRIVILEGE":                 true,
		"PRIVILEGES":                true,
		"PROFILE":                   true,
		"PROGRAM":                   true,
		"PROJECT":                   true,
		"PROTECTED":                 true,
		"PROTECTION":                true,
		"PUBLIC":                    true,
		"PURGE":                     true,
		"QUERY":                     true,
		"QUIESCE":                   true,
		"QUOTA":                     true,
		"RANDOM":                    true,
		"RANGE":                     true,
		"RAW":                       true,
		"READ":                      true,
		"READS":                     true,
		"REBUILD":                   true,
		"RECORDS_PER_BLOCK":         true,
		"RECOVER":                   true,
		"RECOVERY":                  true,
		"RECYCLEBIN":                true,
		"REDUCED":                   true,
		"REDUNDANCY":                true,
		"REF":                       true,
		"REFERENCE":                 true,
		"REFERENCED":                true,
		"REFERENCING":               true,
		"REFRESH":                   true,
		"REGISTER":                  true,
		"REGISTERED":                true,
		"REJECT":                    true,
		"RELY":                      true,
		"REMOTE_DEPENDENCIES_MODE":  true,
		"RENAME":                    true,
		"REPAIR":                    true,
		"REPLACE":                   true,
		"REQUIRED":                  true,
		"RESET":                     true,
		"RESETLOGS":                 true,
		"RESIZE":                    true,
		"RESOLVE":                   true,
		"RESOLVER":                  true,
		"RESTRICT":                  true,
		"RESTRICTED":                true,
		"RESUMABLE":                 true,
		"RESUME":                    true,
		"RETENTION":                 true,
		"RETURN":                    true,
		"RETURNING":                 true,
		"REUSE":                     true,
		"REVERSE":                   true,
		"REWRITE":                   true,
		"ROLES":                     true,
		"ROLLBACK":                  true,
		"ROLLUP":                    true,
		"ROW":                       true,
		"ROWDEPENDENCIES":           true,
		"ROWID":                     true,
		"ROWNUM":                    true,
		"ROWS":                      true,
		"RULE":                      true,
		"SAMPLE":                    true,
		"SAVEPOINT":                 true,
		"SCAN":                      true,
		"SCAN_INSTANCES":            true,
		"SCHEDULER":                 true,
		"SCN":                       true,
		"SCOPE":                     true,
		"SECOND":                    true,
		"SECURITY":                  true,
		"SEGMENT":                   true,
		"SELF":                      true,
		"SERIALIZABLE":              true,
		"SESSION":                   true,
		"SESSIONS_PER_USER":         true,
		"SHARE":                     true,
		"SHARED":                    true,
		"SHARED_POOL":               true,
		"SHRINK":                    true,
		"SHUTDOWN":                  true,
		"SIBLINGS":                  true,
		"SINGLE":                    true,
		"SIZE":                      true,
		"SKIP":                      true,
		"SORT":                      true,
		"SOURCE":                    true,
		"SPACE":                     true,
		"SPECIFICATION":             true,
		"SPFILE":                    true,
		"SPLIT":                     true,
		"STANDBY":                   true,
		"START":                     true,
		"STATEMENT_ID":              true,
		"STATIC":                    true,
		"STATISTICS":                true,
		"STOP":                      true,
		"STORAGE":                   true,
		"STORE":                     true,
		"STRUCTURE":                 true,
		"SUBMULTISET":               true,
		"SUBPARTITION":              true,
		"SUBPARTITIONS":             true,
		"SUBSTITUTABLE":             true,
		"SUBTYPE":                   true,
		"SUCCESS":                   true,
		"SUSPEND":                   true,
		"SWITCH":                    true,
		"SWITCHOVER":                true,
		"SYS_OP_BITVEC":             true,
		"SYS_OP_ENFORCE_NOT_NULL$":  true,
		"SYS_OP_NTCIMG$":            true,
		"SYNONYM":                   true,
		"SYSDATE":                   true,
		"SYSDBA":                    true,
		"SYSOPER":                   true,
		"SYSTEM":                    true,
		"TABLES":                    true,
		"TABLESPACE":                true,
		"TEMPFILE":                  true,
		"TEMPORARY":                 true,
		"TEMPORARY_TABLESPACE":      true,
		"TERMINATE":                 true,
		"THREAD":                    true,
		"THROUGH":                   true,
		"TIES":                      true,
		"TIME":                      true,
		"TIMEOUT":                   true,
		"TIMEZONE_ABBR":             true,
		"TIMEZONE_HOUR":             true,
		"TIMEZONE_MINUTE":           true,
		"TIMEZONE_REGION":           true,
		"TRACE":                     true,
		"TRANSACTION":               true,
		"TRANSITIONAL":              true,
		"TRIGGERS":                  true,
		"TRUNCATE":                  true,
		"TRUSTED":                   true,
		"TUNING":                    true,
		"TX":                        true,
		"TYPE":                      true,
		"TYPES":                     true,
		"UID":                       true,
		"UNARCHIVED":                true,
		"UNBOUNDED":                 true,
		"UNDEFINE":                  true,
		"UNDER":                     true,
		"UNDO":                      true,
		"UNDROP":                    true,
		"UNIFORM":                   true,
		"UNLIMITED":                 true,
		"UNLOCK":                    true,
		"UNPACKED":                  true,
		"UNPROTECTED":               true,
		"UNQUIESCE":                 true,
		"UNRECOVERABLE":             true,
		"UNTIL":                     true,
		"UNUSABLE":                  true,
		"UNUSED":                    true,
		"UPDATABLE":                 true,
		"UPGRADE":                   true,
		"USAGE":                     true,
		"USE":                       true,
		"VALIDATE":                  true,
		"VALIDATION":                true,
		"VALUE":                     true,
		"VARRAY":                    true,
		"VERIFY":                    true,
		"VIRTUAL":                   true,
		"VISIBLE":                   true,
		"WAIT":                      true,
		"WALLET":                    true,
		"WELLFORMED":                true,
		"WORK":                      true,
		"WRAPPED":                   true,
		"WRITE":                     true,
		"XMLATTRIBUTES":             true,
		"XMLCOLATTVAL":              true,
		"XMLELEMENT":                true,
		"XMLFOREST":                 true,
		"XMLPARSE":                  true,
		"XMLROOT":                   true,
		"XMLSCHEMA":                 true,
		"XMLSERIALIZE":              true,
		"XMLTABLE":                  true,
		"XMLTYPE":                   true,
		"YEAR":                      true,
		"YES":                       true,
		"ZONE":                      true,
	}
)

// isReservedKeyword checks if an identifier is a reserved keyword in the specific dialect
func isReservedKeyword(dialect DXDatabaseType, word string) bool {
	// Convert to uppercase for case-insensitive comparison
	upperWord := strings.ToUpper(word)

	var c map[string]bool
	// Add dialect-specific keywords
	switch dialect {
	case PostgreSQL:
		c = postgresKeywords
	case MariaDB:
		c = mysqlKeywords
	case SQLServer:
		c = sqlServerKeywords
	case Oracle:
		c = oracleKeywords
	case UnknownDatabaseType:
		panic("unknown database type")
	default:
		panic("unhandled default case")
	}

	if v, ok := c[upperWord]; ok {
		return v
	}

	return false
}

func isKeywordCanBeUseAsFieldNameDirectly(dialect DXDatabaseType, word string) bool {
	// Convert to uppercase for case-insensitive comparison
	upperWord := strings.ToUpper(word)

	// Add dialect-specific keywords
	var c map[string]bool
	switch dialect {
	case PostgreSQL:
		c = postgresKeywordsCanBeUsedAsFieldNameDirectly
	case MariaDB:
		c = mariadbKeywordsCanBeUsedAsFieldNameDirectly
	case SQLServer:
		c = sqlServerKeywordsCanBeUsedAsFieldNameDirectly
	case Oracle:
		c = oracleKeywordsCanBeUsedAsFieldNameDirectly
	case UnknownDatabaseType:
		panic("unknown database type")
	default:
		panic("unhandled default case")
	}

	if v, ok := c[upperWord]; ok {
		return v
	}

	return false
}

// CheckIdentifier validates table and column names according to database-specific rules
func CheckIdentifier(dialect DXDatabaseType, identifier string) error {
	if identifier == "" {
		return errors.Errorf("identifier cannot be empty")
	}

	// Check for quoted identifiers
	isQuoted := false
	quoteType := -1

	if len(identifier) >= 2 {
		quoteChars := QuoteCharacters[dialect]
		for i, startChar := range quoteChars.Start {
			endChar := quoteChars.End[i]
			if rune(identifier[0]) == startChar && rune(identifier[len(identifier)-1]) == endChar {
				isQuoted = true
				quoteType = i
				break
			}
		}
	}

	if isQuoted {
		// Extract content without quotes
		content := identifier[1 : len(identifier)-1]

		// For quoted identifiers, mainly check length and basic sanity
		if content == "" {
			return errors.Errorf("empty quoted identifier")
		}

		// Check length
		if maxLen := maxIdentifierLengths[dialect]; len(content) > maxLen {
			return errors.Errorf("quoted identifier %q exceeds maximum length of %d for dialect %s",
				identifier, maxLen, dialect)
		}

		// Check for quote doubling (escaping) in the content
		quoteChar := QuoteCharacters[dialect].Start[quoteType]
		if strings.Count(content, string(quoteChar)) > 0 {
			// In SQL, quotes within quoted identifiers must be doubled
			// e.g., "column""name" is valid and represents: column"name
			// Verify this pattern
			if !strings.Contains(content, string(quoteChar)+string(quoteChar)) {
				return errors.Errorf("invalid quote character in identifier without proper escaping")
			}
		}

		// Still check for suspicious patterns, as even quoted identifiers shouldn't contain SQL injection
		if err := checkSuspiciousQueryPatterns(content, false); err != nil {
			return errors.Errorf("potentially dangerous quoted identifier: %+v", err)
		}

		return nil
	}

	// Handle qualified names (e.g., schema.table.column) for unquoted identifiers
	parts := strings.Split(identifier, ".")
	for _, part := range parts {
		if part == "" {
			return errors.Errorf("empty part in identifier %q", identifier)
		}

		// Get the appropriate pattern for this dialect
		pattern, exists := identifierPatterns[dialect]
		if !exists {
			return errors.Errorf("unknown database dialect: %s", dialect)
		}

		// Check pattern for unquoted identifiers
		if !pattern.MatchString(part) {
			return errors.Errorf("invalid identifier format for %s: %s", dialect, part)
		}

		// Check if dentifier is a reserved keyword
		if isReservedKeyword(dialect, part) {
			if !isKeywordCanBeUseAsFieldNameDirectly(dialect, part) {
				return errors.Errorf("identifier %q is a reserved keyword in %s", part, dialect)
			}
		}

		// Check length
		if maxLen := maxIdentifierLengths[dialect]; len(part) > maxLen {
			return errors.Errorf("identifier %q exceeds maximum length of %d for dialect %s",
				part, maxLen, dialect)
		}
	}

	return nil
}

// CheckOperator validates SQL operators
func CheckOperator(dialect DXDatabaseType, operator string) error {
	op := strings.ToLower(strings.TrimSpace(operator))
	if ops, ok := validOperators[dialect]; ok {
		if !ops[op] {
			return errors.Errorf("operator %q not supported for dialect %s", operator, dialect)
		}
	}
	return nil
}

// CheckValue validates a value for SQL injection
func CheckValue(dialect DXDatabaseType, value any) error {
	if value == nil {
		return nil
	}

	switch v := value.(type) {
	case *string:
		vv := *v
		return checkStringValue(vv)
	case string:
		return checkStringValue(v)
	case []any:
		for _, item := range v {
			if err := CheckValue(dialect, item); err != nil {
				return err
			}
		}
	case []string:
		for _, item := range v {
			if err := CheckValue(dialect, item); err != nil {
				return err
			}
		}
	case []uint8, []uint64, []int64, []int32, []int16, []int8, []int, []float64, []float32, []bool:
		return nil
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64, bool:
		// Numeric and boolean values are safe
		return nil
	case map[string]interface{}:
		// Handle JSONB data type
		for key, val := range v {
			if err := CheckIdentifier(dialect, key); err != nil {
				return err
			}
			if err := CheckValue(dialect, val); err != nil {
				return err
			}
		}
	case time.Time:
		return nil
	case decimal.Decimal:
		return nil
	default:
		return nil
		//return errors.Errorf("unsupported value type: %T", value)
	}

	return nil
}

// CheckLikePattern validates LIKE patterns
func CheckLikePattern(query string) error {
	// Convert to lowercase for case-insensitive matching
	loweredQuery := strings.ToLower(query)

	// Find all LIKE or ILIKE clauses
	likePositions := []int{}
	likeKeywords := []string{"like", "ilike"}

	for _, keyword := range likeKeywords {
		currentPos := 0
		for {
			// Find next occurrence starting from currentPos
			foundPos := strings.Index(loweredQuery[currentPos:], keyword)
			if foundPos == -1 {
				break
			}
			// Add the absolute position
			absolutePos := currentPos + foundPos
			likePositions = append(likePositions, absolutePos)
			// Move past this occurrence
			currentPos = absolutePos + len(keyword)
		}
	}

	// For each LIKE/ILIKE found, extract and check its pattern
	for _, pos := range likePositions {
		// Find the next value after LIKE/ILIKE (usually enclosed in quotes)
		remainingQuery := query[pos:]
		quotePos := strings.Index(remainingQuery, "'")
		if quotePos == -1 {
			continue // No pattern found, skip
		}

		// Find the closing quote
		endQuotePos := strings.Index(remainingQuery[quotePos+1:], "'")
		if endQuotePos == -1 {
			continue // Unclosed quote, skip
		}

		// Extract the pattern between quotes
		pattern := remainingQuery[quotePos+1 : quotePos+1+endQuotePos]

		// Check the actual pattern
		if err := checkStringValue(pattern); err != nil {
			return err
		}

		// Check wildcard count
		if strings.Count(pattern, "%") > 5 {
			return errors.Errorf("too many wildcards in LIKE pattern")
		}
	}

	return nil
}

// CheckOrderBy validates ORDER BY expressions
func CheckOrderBy(dialect DXDatabaseType, expr string) error {
	if expr == "" {
		return errors.Errorf("empty order by expression")
	}

	for _, part := range strings.Split(expr, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		// Split into field and direction
		tokens := strings.Fields(part)
		if len(tokens) == 0 {
			return errors.Errorf("empty order by part")
		}

		// Check field name
		if err := CheckIdentifier(dialect, tokens[0]); err != nil {
			return errors.Wrap(err, fmt.Sprintf("invalid field in order by: ", err.Error()))
		}

		// Check direction if specified
		if len(tokens) > 1 {
			dir := strings.ToUpper(tokens[1])
			if dir != "ASC" && dir != "DESC" {
				return errors.Errorf("invalid sort direction: %s", tokens[1])
			}
		}

		// Check for NULLS FIRST/LAST if present
		if len(tokens) > 2 {
			if tokens[2] != "NULLS" || len(tokens) < 4 || (tokens[3] != "FIRST" && tokens[3] != "LAST") {
				return errors.Errorf("invalid NULLS FIRST/LAST syntax")
			}
		}
	}

	return nil
}

func CheckOrderByDirection(dialect DXDatabaseType, direction string) error {
	if direction == "" {
		return errors.New("empty order by expression")
	}

	// Normalize: trim and uppercase, collapse whitespace
	normalizedDirection := strings.Join(strings.Fields(strings.ToUpper(direction)), " ")

	// Check basic directions (all databases support)
	switch normalizedDirection {
	case "ASC", "DESC":
		return nil
	}

	// Check NULLS syntax
	switch normalizedDirection {
	case "ASC NULLS FIRST", "ASC NULLS LAST", "DESC NULLS FIRST", "DESC NULLS LAST":
		// Only PostgreSQL and Oracle support NULLS syntax
		switch dialect {
		case PostgreSQL, Oracle:
			return nil
		case MariaDB, DeprecatedMysql:
			return errors.Errorf("MariaDB/MySQL does not support '%s' syntax", normalizedDirection)
		case SQLServer:
			return errors.Errorf("SQL Server does not support '%s' syntax", normalizedDirection)
		case UnknownDatabaseType:
			return errors.Errorf("unknown database type for '%s'", normalizedDirection)
		default:
			return errors.Errorf("unsupported database type for '%s'", normalizedDirection)
		}
	}

	return errors.Errorf("invalid sort direction: %s", direction)
}

// CheckBaseQuery validates the base query for suspicious patterns
func CheckBaseQuery(dialect DXDatabaseType, query string) error {
	if query == "" {
		return errors.Errorf("empty query")
	}

	loweredQuery := strings.ToLower(query)

	// Check for multiple statements
	if strings.Count(query, ";") > 0 {
		return errors.Errorf("multiple statements not allowed")
	}

	// Check for suspicious patterns
	if err := checkSuspiciousQueryPatterns(loweredQuery, false); err != nil {
		return errors.Errorf("query validation failed: %+v", err)
	}

	return nil
}

// Internal helper functions

func checkStringValue(value string) error {
	/*lowered := strings.ToLower(value)

	  // Check for suspicious patterns
	  for _, pattern := range suspiciousValuePatterns {
	  	if strings.Contains(lowered, pattern) {
	  		return errors.Errorf("suspicious pattern (%s) detected in value: %s", pattern, value)
	  	}
	  }*/
	return nil
}

func checkSuspiciousQueryPatterns(value string, ignoreInComments bool) error {
	lowered := strings.ToLower(value)

	// First, check if the value is within a comment
	if ignoreInComments && (strings.Contains(lowered, "/*") || strings.Contains(lowered, "*/") || strings.Contains(lowered, "--")) {
		return nil
	}

	for _, pattern := range suspiciousRegexQueryPatterns {
		// Use a more specific logic to avoid false positives

		if regexp.MustCompile(pattern).MatchString(lowered) {
			return errors.Errorf("suspicious pattern detected: %s", pattern)
		}
	}
	return nil
}

func CheckAll(dialect DXDatabaseType, query string, arg any) (err error) {
	if AllowRisk {
		return nil
	}
	err = CheckBaseQuery(dialect, query)
	if err != nil {
		return errors.Errorf("SQL_INJECTION_DETECTED:QUERY_VALIDATION_FAILED: %+v=%s +%v", err, query, arg)
	}

	err = CheckValue(dialect, arg)
	if err != nil {
		return errors.Errorf("SQL_INJECTION_DETECTED:VALUE_VALIDATION_FAILED: %+v", err)
	}

	// Check LIKE patterns
	if strings.Contains(query, "LIKE") {
		err = CheckLikePattern(query)
		if err != nil {
			return errors.Errorf("SQL_INJECTION_DETECTED:LIKE_PATTERN_VALIDATION_FAILED: %+v", err)
		}
	}

	// Check ORDER BY expressions
	if strings.Contains(query, "ORDER BY") {
		err = CheckOrderBy(dialect, query)
		if err != nil {
			return errors.Errorf("SQL_INJECTION_DETECTED:ORDER_BY_VALIDATION_FAILED: %+v", err)
		}
	}

	return nil
}

// ValidateAndSanitizeOrderBy validates and sanitizes the order by clause
func ValidateAndSanitizeOrderBy(orderBy string) (string, error) {
	if strings.TrimSpace(orderBy) == "" {
		return "id ASC", nil // Default order
	}

	// Allowed field names - add your fields here
	allowedFields := map[string]bool{
		"id":         true,
		"code":       true,
		"name":       true,
		"created_at": true,
		"updated_at": true,
		// Add other allowed fields here
	}

	// Split by comma and validate each part
	parts := strings.Split(orderBy, ",")
	var sanitizedParts []string

	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		// Split into field and direction
		components := strings.Fields(part)
		if len(components) == 0 || len(components) > 2 {
			return "", errors.Errorf("invalid order by format: %s", part)
		}

		// Validate field name (only allow alphanumeric and underscore)
		field := strings.ToLower(components[0])
		if !allowedFields[field] {
			return "", errors.Errorf("invalid field name: %s", field)
		}

		// Validate direction if provided
		direction := "ASC" // default direction
		if len(components) == 2 {
			dir := strings.ToUpper(components[1])
			if dir != "ASC" && dir != "DESC" {
				return "", errors.Errorf("invalid sort direction: %s", components[1])
			}
			direction = dir
		}

		sanitizedParts = append(sanitizedParts, fmt.Sprintf("%s %s", field, direction))
	}

	if len(sanitizedParts) == 0 {
		return "id ASC", nil
	}

	return strings.Join(sanitizedParts, ", "), nil
}

// Example usage in handler
func ValidateAndSanitizeOrderByExampleUsage() {
	// Valid examples
	examples := []string{
		"id ASC",
		"name DESC, created_at ASC",
		"code asc, id desc",
		"updated_at", // Will use default ASC
		"",           // Will use default "id ASC"
	}

	for _, example := range examples {
		result, err := ValidateAndSanitizeOrderBy(example)
		if err != nil {
			fmt.Printf("Error: %v\n", err)
			continue
		}
		fmt.Printf("Input: %s -> Sanitized: %s\n", example, result)
	}

	// Invalid examples that will be rejected
	invalidExamples := []string{
		"id ASC; DROP TABLE users",
		"name' OR '1'='1",
		"id) UNION SELECT",
		"unknown_field ASC",
		"id ASCENDING", // Invalid direction
		"id ASC DESC",  // Too many directions
		"id, , name",   // Empty part
	}

	for _, example := range invalidExamples {
		_, err := ValidateAndSanitizeOrderBy(example)
		fmt.Printf("Invalid input '%s': %v\n", example, err)
	}
}
