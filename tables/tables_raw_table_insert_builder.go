package tables

import (
	"context"
	"database/sql"

	"github.com/donnyhardyanto/dxlib/databases"
	"github.com/donnyhardyanto/dxlib/databases/db/query"
	"github.com/donnyhardyanto/dxlib/log"
	tableQueryBuilder "github.com/donnyhardyanto/dxlib/tables/query_builder"
	"github.com/donnyhardyanto/dxlib/utils"
)

// InsertWithBuilder executes an INSERT using TableInsertQueryBuilder for safe SQL construction.
// SetFields and RETURNING are read from tqb.
func (t *DXRawTable) InsertWithBuilder(ctx context.Context, l *log.DXLog, tqb *tableQueryBuilder.TableInsertQueryBuilder) (result sql.Result, returning utils.JSON, err error) {
	if err = t.EnsureDatabase(); err != nil {
		return nil, nil, err
	}
	if tqb.Error != nil {
		return nil, nil, tqb.Error
	}

	tqb.SourceName = t.GetFullTableName()

	if len(t.EncryptionKeyDefs) > 0 || len(t.EncryptionColumnDefs) > 0 {
		txErr := t.Database.Tx(ctx, l, databases.LevelReadCommitted, func(dtx *databases.DXDatabaseTx) error {
			if err = t.TxSetAllEncryptionSessionKeys(dtx); err != nil {
				return err
			}
			result, returning, err = query.TxInsertWithInsertQueryBuilder2(ctx, dtx, tqb.InsertQueryBuilder)
			return err
		})
		if txErr != nil {
			return nil, nil, txErr
		}
		return result, returning, nil
	}

	return query.InsertWithInsertQueryBuilder2(ctx, t.Database.Connection, tqb.InsertQueryBuilder)
}

// TxInsertWithBuilder executes an INSERT within a transaction using TableInsertQueryBuilder.
func (t *DXRawTable) TxInsertWithBuilder(dtx *databases.DXDatabaseTx, tqb *tableQueryBuilder.TableInsertQueryBuilder) (sql.Result, utils.JSON, error) {
	if tqb.Error != nil {
		return nil, nil, tqb.Error
	}

	tqb.SourceName = t.GetFullTableName()
	return query.TxInsertWithInsertQueryBuilder2(dtx.Ctx, dtx, tqb.InsertQueryBuilder)
}
