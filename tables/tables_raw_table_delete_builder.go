package tables

import (
	"database/sql"

	"github.com/donnyhardyanto/dxlib/databases"
	"github.com/donnyhardyanto/dxlib/databases/db/query"
	"github.com/donnyhardyanto/dxlib/log"
	tableQueryBuilder "github.com/donnyhardyanto/dxlib/tables/query_builder"
	"github.com/donnyhardyanto/dxlib/utils"
)

// DeleteWithBuilder executes a DELETE using TableDeleteQueryBuilder for safe SQL construction.
// WHERE and RETURNING are read from tqb.
func (t *DXRawTable) DeleteWithBuilder(l *log.DXLog, tqb *tableQueryBuilder.TableDeleteQueryBuilder) (sql.Result, []utils.JSON, error) {
	if err := t.EnsureDatabase(); err != nil {
		return nil, nil, err
	}
	if tqb.Error != nil {
		return nil, nil, tqb.Error
	}

	tqb.SourceName = t.GetFullTableName()

	if len(t.EncryptionKeyDefs) > 0 || len(t.EncryptionColumnDefs) > 0 {
		dtx, err := t.Database.TransactionBegin(databases.LevelReadCommitted)
		if err != nil {
			return nil, nil, err
		}
		defer dtx.Finish(l, err)
		if err := t.TxSetAllEncryptionSessionKeys(dtx); err != nil {
			return nil, nil, err
		}
		return query.TxDeleteWithDeleteQueryBuilder2(dtx, tqb.DeleteQueryBuilder)
	}

	return query.DeleteWithDeleteQueryBuilder2(t.Database.Connection, tqb.DeleteQueryBuilder)
}

// TxDeleteWithBuilder executes a DELETE within a transaction using TableDeleteQueryBuilder.
func (t *DXRawTable) TxDeleteWithBuilder(dtx *databases.DXDatabaseTx, tqb *tableQueryBuilder.TableDeleteQueryBuilder) (sql.Result, []utils.JSON, error) {
	if tqb.Error != nil {
		return nil, nil, tqb.Error
	}

	tqb.SourceName = t.GetFullTableName()
	return query.TxDeleteWithDeleteQueryBuilder2(dtx, tqb.DeleteQueryBuilder)
}

// DeleteByIdWithBuilder executes a DELETE by ID using TableDeleteQueryBuilder.
func (t *DXRawTable) DeleteByIdWithBuilder(l *log.DXLog, id int64, tqb *tableQueryBuilder.TableDeleteQueryBuilder) (sql.Result, []utils.JSON, error) {
	tqb.Conditions = append(tqb.Conditions, t.FieldNameForRowId+" = :__delete_id__")
	tqb.Args["__delete_id__"] = id
	return t.DeleteWithBuilder(l, tqb)
}

// TxDeleteByIdWithBuilder executes a DELETE by ID within a transaction using TableDeleteQueryBuilder.
func (t *DXRawTable) TxDeleteByIdWithBuilder(dtx *databases.DXDatabaseTx, id int64, tqb *tableQueryBuilder.TableDeleteQueryBuilder) (sql.Result, []utils.JSON, error) {
	tqb.Conditions = append(tqb.Conditions, t.FieldNameForRowId+" = :__delete_id__")
	tqb.Args["__delete_id__"] = id
	return t.TxDeleteWithBuilder(dtx, tqb)
}
