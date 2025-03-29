package table

import (
	"database/sql"
	"fmt"
	"github.com/donnyhardyanto/dxlib/api"
	"github.com/donnyhardyanto/dxlib/database"
	"github.com/donnyhardyanto/dxlib/database/protected/db"
	"github.com/donnyhardyanto/dxlib/database/protected/export"
	"github.com/donnyhardyanto/dxlib/log"
	"github.com/donnyhardyanto/dxlib/utils"
	"github.com/pkg/errors"
	"net/http"
	"strings"
	"time"
	_ "time/tzdata"
)

type OnResultList func(listRow utils.JSON) (utils.JSON, error)

type DXRawTable struct {
	DXBaseTable
}

func (t *DXRawTable) SelectAll(log *log.DXLog) (rowsInfo *db.RowsInfo, r []utils.JSON, err error) {
	return t.Select(log, nil, nil, nil, nil, nil, nil)
}

/*func (t *DXRawTable) Count(log *log.DXLog, summaryCalcFieldsPart string, whereAndFieldNameValues utils.JSON, joinSQLPart any) (totalRows int64, summaryCalcRow utils.JSON, err error) {
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
