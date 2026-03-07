package tables

import (
	"database/sql"

	"github.com/donnyhardyanto/dxlib/databases"
	"github.com/donnyhardyanto/dxlib/databases/db/query"
	tableQueryBuilder "github.com/donnyhardyanto/dxlib/tables/query_builder"
	"github.com/donnyhardyanto/dxlib/utils"
)

// TxDeleteWithBuilderAuto sets encryption session keys then delegates to query.TxDeleteWithDeleteQueryBuilder2.
func (t *DXRawTable) TxDeleteWithBuilderAuto(dtx *databases.DXDatabaseTx, tqb *tableQueryBuilder.TableDeleteQueryBuilder) (sql.Result, []utils.JSON, error) {
	if tqb.Error != nil {
		return nil, nil, tqb.Error
	}

	tqb.SourceName = t.GetFullTableName()

	if t.HasEncryptionConfig() {
		if err := t.TxSetAllEncryptionSessionKeys(dtx); err != nil {
			return nil, nil, err
		}
	}

	return query.TxDeleteWithDeleteQueryBuilder2(dtx.Ctx, dtx, tqb.DeleteQueryBuilder)
}

// TxDeleteByIdWithBuilderAuto sets encryption session keys then deletes by ID.
func (t *DXRawTable) TxDeleteByIdWithBuilderAuto(dtx *databases.DXDatabaseTx, id int64, tqb *tableQueryBuilder.TableDeleteQueryBuilder) (sql.Result, []utils.JSON, error) {
	tqb.Conditions = append(tqb.Conditions, t.FieldNameForRowId+" = :__delete_id__")
	tqb.Args["__delete_id__"] = id
	return t.TxDeleteWithBuilderAuto(dtx, tqb)
}
