package tables

import (
	"database/sql"

	"github.com/donnyhardyanto/dxlib/databases"
	"github.com/donnyhardyanto/dxlib/databases/db/query"
	"github.com/donnyhardyanto/dxlib/log"
	tableQueryBuilder "github.com/donnyhardyanto/dxlib/tables/query_builder"
	"github.com/donnyhardyanto/dxlib/utils"
)

// InsertWithBuilder executes an INSERT using TableInsertQueryBuilder for safe SQL construction.
// SetFields and RETURNING are read from tqb.
func (t *DXRawTable) InsertWithBuilder(l *log.DXLog, tqb *tableQueryBuilder.TableInsertQueryBuilder) (sql.Result, utils.JSON, error) {
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
		return query.TxInsertWithInsertQueryBuilder2(dtx, tqb.InsertQueryBuilder)
	}

	return query.InsertWithInsertQueryBuilder2(t.Database.Connection, tqb.InsertQueryBuilder)
}

// TxInsertWithBuilder executes an INSERT within a transaction using TableInsertQueryBuilder.
func (t *DXRawTable) TxInsertWithBuilder(dtx *databases.DXDatabaseTx, tqb *tableQueryBuilder.TableInsertQueryBuilder) (sql.Result, utils.JSON, error) {
	if tqb.Error != nil {
		return nil, nil, tqb.Error
	}

	tqb.SourceName = t.GetFullTableName()
	return query.TxInsertWithInsertQueryBuilder2(dtx, tqb.InsertQueryBuilder)
}
