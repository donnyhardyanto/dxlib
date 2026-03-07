package tables

import (
	"database/sql"

	"github.com/donnyhardyanto/dxlib/databases"
	"github.com/donnyhardyanto/dxlib/databases/db/query"
	tableQueryBuilder "github.com/donnyhardyanto/dxlib/tables/query_builder"
	"github.com/donnyhardyanto/dxlib/utils"
)

// TxUpdateWithBuilderAuto sets encryption session keys then delegates to query.TxUpdateWithUpdateQueryBuilder2.
func (t *DXRawTable) TxUpdateWithBuilderAuto(dtx *databases.DXDatabaseTx, tqb *tableQueryBuilder.TableUpdateQueryBuilder) (sql.Result, []utils.JSON, error) {
	if tqb.Error != nil {
		return nil, nil, tqb.Error
	}

	tqb.SourceName = t.GetFullTableName()

	if t.HasEncryptionConfig() {
		if err := t.TxSetAllEncryptionSessionKeys(dtx); err != nil {
			return nil, nil, err
		}
	}

	return query.TxUpdateWithUpdateQueryBuilder2(dtx.Ctx, dtx, tqb.UpdateQueryBuilder)
}

// TxUpdateByIdWithBuilderAuto sets encryption session keys then updates by ID.
func (t *DXRawTable) TxUpdateByIdWithBuilderAuto(dtx *databases.DXDatabaseTx, id int64, tqb *tableQueryBuilder.TableUpdateQueryBuilder) (sql.Result, []utils.JSON, error) {
	tqb.Conditions = append(tqb.Conditions, t.FieldNameForRowId+" = :__update_id__")
	tqb.Args["__update_id__"] = id
	return t.TxUpdateWithBuilderAuto(dtx, tqb)
}
