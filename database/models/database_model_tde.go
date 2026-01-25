package models

import (
	"github.com/donnyhardyanto/dxlib/base"
)

// ModelDBTDEConfig holds Transparent Data Encryption configuration for different database types
// Each database has different TDE approaches:
// - PostgreSQL: Uses table access method (e.g., "tde_heap" with pg_tde extension)
// - Oracle: Uses encrypted tablespace (tables created in pre-configured encrypted tablespace)
// - SQL Server: Database-level TDE (no per-table syntax, just a marker that TDE is expected)
// - MariaDB/MySQL: Uses InnoDB table encryption option (ENCRYPTION='Y')
type ModelDBTDEConfig struct {
	// PostgreSQL: Table access method name for TDE (e.g., "tde_heap")
	ModelDBPostgreSQLAccessMethod string

	// Oracle: Encrypted tablespace name where the table will be created
	OracleTablespace string

	// SQLServer: Flag indicating that database-level TDE should be enabled
	// No per-table syntax is needed; this is for documentation/validation purposes
	SQLServerTDEEnabled bool

	// MariaDB: Encryption option for InnoDB tables ('Y' for enabled, 'N' or empty for disabled)
	MariaDBEncryption string
}

// IsEnabled checks if TDE is configured for the specified database type key
func (t ModelDBTDEConfig) IsEnabled(dbType base.DXDatabaseType) bool {
	switch dbType {
	case base.DXDatabaseTypePostgreSQL:
		return t.ModelDBPostgreSQLAccessMethod != ""
	case base.DXDatabaseTypeOracle:
		return t.OracleTablespace != ""
	case base.DXDatabaseTypeSQLServer:
		return t.SQLServerTDEEnabled
	case base.DXDatabaseTypeMariaDB:
		return t.MariaDBEncryption == "Y"
	default:
		return false
	}
}
