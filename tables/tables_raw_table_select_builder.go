package tables

import (
	"fmt"

	"github.com/donnyhardyanto/dxlib/base"
	"github.com/donnyhardyanto/dxlib/databases"
	"github.com/donnyhardyanto/dxlib/databases/db"
	"github.com/donnyhardyanto/dxlib/databases/db/query"
	"github.com/donnyhardyanto/dxlib/errors"
	"github.com/donnyhardyanto/dxlib/log"
	tableQueryBuilder "github.com/donnyhardyanto/dxlib/tables/query_builder"
	"github.com/donnyhardyanto/dxlib/utils"
)

// prepareBuilderForSelect sets SourceName and handles encryption OutFields on the builder.
// Returns true if encryption session keys need to be set in a transaction.
func (t *DXRawTable) prepareBuilderForSelect(tqb *tableQueryBuilder.TableSelectQueryBuilder) (needsEncryptionTx bool) {
	tqb.SourceName = t.GetListViewName()

	if len(t.EncryptionColumnDefs) > 0 {
		dbType := base.StringToDXDatabaseType(t.Database.Connection.DriverName())
		encryptionColumns := t.convertEncryptionColumnDefsForSelect()
		var outFields []string
		if len(tqb.OutFields) > 0 {
			outFields = append(outFields, tqb.OutFields...)
		} else {
			outFields = append(outFields, "*")
		}
		for _, col := range encryptionColumns {
			if col.ViewHasDecrypt {
				outFields = append(outFields, col.AliasName)
			} else {
				expr := db.DecryptExpression(dbType, col.FieldName, col.EncryptionKeyDef.SessionKey)
				outFields = append(outFields, fmt.Sprintf("%s AS %s", expr, col.AliasName))
			}
		}
		tqb.OutFields = outFields
		return true
	}

	if len(t.EncryptionKeyDefs) > 0 {
		return true
	}

	return false
}

// SelectWithBuilder returns multiple rows using TableSelectQueryBuilder for safe SQL construction.
// fieldNames, orderBy, limit, forUpdatePart are all read from tqb.
func (t *DXRawTable) SelectWithBuilder(l *log.DXLog, tqb *tableQueryBuilder.TableSelectQueryBuilder) (*db.DXDatabaseTableRowsInfo, []utils.JSON, error) {
	if err := t.EnsureDatabase(); err != nil {
		return nil, nil, err
	}
	if tqb.Error != nil {
		return nil, nil, tqb.Error
	}

	needsEncryptionTx := t.prepareBuilderForSelect(tqb)

	if needsEncryptionTx {
		dtx, err := t.Database.TransactionBegin(databases.LevelReadCommitted)
		if err != nil {
			return nil, nil, err
		}
		defer dtx.Finish(l, err)
		if err := t.TxSetAllEncryptionSessionKeys(dtx); err != nil {
			return nil, nil, err
		}
		return query.TxSelectWithSelectQueryBuilder2(dtx, tqb.SelectQueryBuilder)
	}

	return query.SelectWithSelectQueryBuilder2(t.Database.Connection, tqb.SelectQueryBuilder)
}

// SelectOneWithBuilder returns a single row using TableSelectQueryBuilder.
func (t *DXRawTable) SelectOneWithBuilder(l *log.DXLog, tqb *tableQueryBuilder.TableSelectQueryBuilder) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	// Save and restore LimitValue so we don't mutate the caller's tqb
	origLimit := tqb.LimitValue
	tqb.LimitValue = 1
	rowsInfo, rows, err := t.SelectWithBuilder(l, tqb)
	tqb.LimitValue = origLimit

	if err != nil {
		return nil, nil, err
	}
	if len(rows) == 0 {
		return rowsInfo, nil, nil
	}
	return rowsInfo, rows[0], nil
}

// ShouldSelectOneWithBuilder returns a single row or error if not found.
func (t *DXRawTable) ShouldSelectOneWithBuilder(l *log.DXLog, tqb *tableQueryBuilder.TableSelectQueryBuilder) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	rowsInfo, row, err := t.SelectOneWithBuilder(l, tqb)
	if err != nil {
		return nil, nil, err
	}
	if row == nil {
		return nil, nil, errors.New("RECORD_NOT_FOUND")
	}
	return rowsInfo, row, nil
}

// TxSelectWithBuilder returns multiple rows within a transaction using TableSelectQueryBuilder.
func (t *DXRawTable) TxSelectWithBuilder(dtx *databases.DXDatabaseTx, tqb *tableQueryBuilder.TableSelectQueryBuilder) (*db.DXDatabaseTableRowsInfo, []utils.JSON, error) {
	if tqb.Error != nil {
		return nil, nil, tqb.Error
	}

	tqb.SourceName = t.GetListViewName()
	return query.TxSelectWithSelectQueryBuilder2(dtx, tqb.SelectQueryBuilder)
}

// TxSelectOneWithBuilder returns a single row within a transaction using TableSelectQueryBuilder.
func (t *DXRawTable) TxSelectOneWithBuilder(dtx *databases.DXDatabaseTx, tqb *tableQueryBuilder.TableSelectQueryBuilder) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	// Save and restore LimitValue so we don't mutate the caller's tqb
	origLimit := tqb.LimitValue
	tqb.LimitValue = 1
	rowsInfo, rows, err := t.TxSelectWithBuilder(dtx, tqb)
	tqb.LimitValue = origLimit

	if err != nil {
		return nil, nil, err
	}
	if len(rows) == 0 {
		return rowsInfo, nil, nil
	}
	return rowsInfo, rows[0], nil
}

// TxShouldSelectOneWithBuilder returns a single row or error if not found within a transaction.
func (t *DXRawTable) TxShouldSelectOneWithBuilder(dtx *databases.DXDatabaseTx, tqb *tableQueryBuilder.TableSelectQueryBuilder) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	rowsInfo, row, err := t.TxSelectOneWithBuilder(dtx, tqb)
	if err != nil {
		return nil, nil, err
	}
	if row == nil {
		return nil, nil, errors.New("RECORD_NOT_FOUND")
	}
	return rowsInfo, row, nil
}

// CountWithBuilder returns total row count using TableSelectQueryBuilder.
func (t *DXRawTable) CountWithBuilder(l *log.DXLog, tqb *tableQueryBuilder.TableSelectQueryBuilder) (int64, error) {
	if err := t.EnsureDatabase(); err != nil {
		return 0, err
	}
	if tqb.Error != nil {
		return 0, tqb.Error
	}

	tqb.SourceName = t.GetListViewName()

	if len(t.EncryptionKeyDefs) > 0 || len(t.EncryptionColumnDefs) > 0 {
		dtx, err := t.Database.TransactionBegin(databases.LevelReadCommitted)
		if err != nil {
			return 0, err
		}
		defer dtx.Finish(l, err)
		if err := t.TxSetAllEncryptionSessionKeys(dtx); err != nil {
			return 0, err
		}
		return query.TxCountWithSelectQueryBuilder2(dtx, tqb.SelectQueryBuilder)
	}

	return query.CountWithSelectQueryBuilder2(t.Database.Connection, tqb.SelectQueryBuilder)
}
