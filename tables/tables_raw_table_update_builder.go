package tables

import (
	"database/sql"

	"github.com/donnyhardyanto/dxlib/databases"
	"github.com/donnyhardyanto/dxlib/databases/db/query"
	"github.com/donnyhardyanto/dxlib/log"
	tableQueryBuilder "github.com/donnyhardyanto/dxlib/tables/query_builder"
	"github.com/donnyhardyanto/dxlib/utils"
)

// UpdateWithBuilder executes an UPDATE using TableUpdateQueryBuilder for safe SQL construction.
// SetFields, WHERE, and RETURNING are all read from tqb.
func (t *DXRawTable) UpdateWithBuilder(l *log.DXLog, tqb *tableQueryBuilder.TableUpdateQueryBuilder) (sql.Result, []utils.JSON, error) {
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
		return query.TxUpdateWithUpdateQueryBuilder2(dtx, tqb.UpdateQueryBuilder)
	}

	return query.UpdateWithUpdateQueryBuilder2(t.Database.Connection, tqb.UpdateQueryBuilder)
}

// TxUpdateWithBuilder executes an UPDATE within a transaction using TableUpdateQueryBuilder.
func (t *DXRawTable) TxUpdateWithBuilder(dtx *databases.DXDatabaseTx, tqb *tableQueryBuilder.TableUpdateQueryBuilder) (sql.Result, []utils.JSON, error) {
	if tqb.Error != nil {
		return nil, nil, tqb.Error
	}

	tqb.SourceName = t.GetFullTableName()
	return query.TxUpdateWithUpdateQueryBuilder2(dtx, tqb.UpdateQueryBuilder)
}

// UpdateByIdWithBuilder executes an UPDATE by ID using TableUpdateQueryBuilder.
func (t *DXRawTable) UpdateByIdWithBuilder(l *log.DXLog, id int64, tqb *tableQueryBuilder.TableUpdateQueryBuilder) (sql.Result, []utils.JSON, error) {
	tqb.Conditions = append(tqb.Conditions, t.FieldNameForRowId+" = :__update_id__")
	tqb.Args["__update_id__"] = id
	return t.UpdateWithBuilder(l, tqb)
}

// TxUpdateByIdWithBuilder executes an UPDATE by ID within a transaction using TableUpdateQueryBuilder.
func (t *DXRawTable) TxUpdateByIdWithBuilder(dtx *databases.DXDatabaseTx, id int64, tqb *tableQueryBuilder.TableUpdateQueryBuilder) (sql.Result, []utils.JSON, error) {
	tqb.Conditions = append(tqb.Conditions, t.FieldNameForRowId+" = :__update_id__")
	tqb.Args["__update_id__"] = id
	return t.TxUpdateWithBuilder(dtx, tqb)
}
