package table2

import (
	_ "time/tzdata"

	"github.com/donnyhardyanto/dxlib/database2/database_type"
	"github.com/donnyhardyanto/dxlib/log"
	"github.com/donnyhardyanto/dxlib/utils"
)

type OnResultList func(listRow utils.JSON) (utils.JSON, error)

/*
	type DXRawTable2 struct {
		DXBaseTable2
	}
*/
func (t *DXRawTable2) SelectAll(log *log.DXLog) (rowsInfo *database_type.RowsInfo, r []utils.JSON, err error) {
	return t.Select(log, nil, nil, nil, nil, nil, nil)
}

/*func (t *DXRawTable2) Count(log *log.DXLog, summaryCalcFieldsPart string, whereAndFieldNameValues utils.JSON, joinSQLPart any) (totalRows int64, summaryCalcRow utils.JSON, err error) {
	totalRows, summaryCalcRow, err = t.Database.ShouldCount(t.ListViewNameId, summaryCalcFieldsPart, whereAndFieldNameValues, joinSQLPart)
	return totalRows, summaryCalcRow, err
}
*/
/*
func (t *DXRawTable) TxSelectCount(tx *database.DXDatabaseTx, summaryCalcFieldsPart string, whereAndFieldNameValues utils.JSON) (totalRows int64, summaryCalcRow utils.JSON, err error) {

		totalRows, summaryCalcRow, err = tx.ShouldCount(t.ListViewNameId, summaryCalcFieldsPart, whereAndFieldNameValues)
		return totalRows, summaryCalcRow, err
	}
*/
