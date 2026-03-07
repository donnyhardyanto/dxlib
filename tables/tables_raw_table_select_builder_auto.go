package tables

import (
	"github.com/donnyhardyanto/dxlib/databases"
	"github.com/donnyhardyanto/dxlib/databases/db"
	"github.com/donnyhardyanto/dxlib/databases/db/query"
	"github.com/donnyhardyanto/dxlib/errors"
	tableQueryBuilder "github.com/donnyhardyanto/dxlib/tables/query_builder"
	"github.com/donnyhardyanto/dxlib/utils"
)

// TxSelectWithBuilderAuto sets encryption session keys and handles EncryptionColumnDefs OutFields,
// then delegates to query.TxSelectWithSelectQueryBuilder2.
func (t *DXRawTable) TxSelectWithBuilderAuto(dtx *databases.DXDatabaseTx, tqb *tableQueryBuilder.TableSelectQueryBuilder) (*db.DXDatabaseTableRowsInfo, []utils.JSON, error) {
	if tqb.Error != nil {
		return nil, nil, tqb.Error
	}

	t.prepareBuilderForSelect(tqb)

	if t.HasEncryptionConfig() {
		if err := t.TxSetAllEncryptionSessionKeys(dtx); err != nil {
			return nil, nil, err
		}
	}

	return query.TxSelectWithSelectQueryBuilder2(dtx.Ctx, dtx, tqb.SelectQueryBuilder, t.FieldTypeMapping)
}

// TxSelectOneWithBuilderAuto returns a single row using the builder with encryption support.
func (t *DXRawTable) TxSelectOneWithBuilderAuto(dtx *databases.DXDatabaseTx, tqb *tableQueryBuilder.TableSelectQueryBuilder) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	origLimit := tqb.LimitValue
	tqb.LimitValue = 1
	rowsInfo, rows, err := t.TxSelectWithBuilderAuto(dtx, tqb)
	tqb.LimitValue = origLimit

	if err != nil {
		return nil, nil, err
	}
	if len(rows) == 0 {
		return rowsInfo, nil, nil
	}
	return rowsInfo, rows[0], nil
}

// TxShouldSelectOneWithBuilderAuto returns a single row or RECORD_NOT_FOUND error, with encryption support.
func (t *DXRawTable) TxShouldSelectOneWithBuilderAuto(dtx *databases.DXDatabaseTx, tqb *tableQueryBuilder.TableSelectQueryBuilder) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	rowsInfo, row, err := t.TxSelectOneWithBuilderAuto(dtx, tqb)
	if err != nil {
		return nil, nil, err
	}
	if row == nil {
		return nil, nil, errors.New("RECORD_NOT_FOUND")
	}
	return rowsInfo, row, nil
}

// TxCountWithBuilderAuto returns total row count using the builder with encryption support.
func (t *DXRawTable) TxCountWithBuilderAuto(dtx *databases.DXDatabaseTx, tqb *tableQueryBuilder.TableSelectQueryBuilder) (int64, error) {
	if tqb.Error != nil {
		return 0, tqb.Error
	}

	tqb.SourceName = t.GetListViewName()

	if t.HasEncryptionConfig() {
		if err := t.TxSetAllEncryptionSessionKeys(dtx); err != nil {
			return 0, err
		}
	}

	return query.TxCountWithSelectQueryBuilder2(dtx.Ctx, dtx, tqb.SelectQueryBuilder)
}
