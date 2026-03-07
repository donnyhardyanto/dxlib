package tables

import (
	"database/sql"

	"github.com/donnyhardyanto/dxlib/databases"
	"github.com/donnyhardyanto/dxlib/databases/db/query"
	tableQueryBuilder "github.com/donnyhardyanto/dxlib/tables/query_builder"
	"github.com/donnyhardyanto/dxlib/utils"
)

// TxInsertWithBuilderAuto sets encryption session keys then delegates to query.TxInsertWithInsertQueryBuilder2.
func (t *DXRawTable) TxInsertWithBuilderAuto(dtx *databases.DXDatabaseTx, tqb *tableQueryBuilder.TableInsertQueryBuilder) (sql.Result, utils.JSON, error) {
	if tqb.Error != nil {
		return nil, nil, tqb.Error
	}

	tqb.SourceName = t.GetFullTableName()

	if t.HasEncryptionConfig() {
		if err := t.TxSetAllEncryptionSessionKeys(dtx); err != nil {
			return nil, nil, err
		}
	}

	return query.TxInsertWithInsertQueryBuilder2(dtx.Ctx, dtx, tqb.InsertQueryBuilder)
}
