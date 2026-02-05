package tables

import (
	"github.com/donnyhardyanto/dxlib/databases"
	"github.com/donnyhardyanto/dxlib/databases/db"
	"github.com/donnyhardyanto/dxlib/errors"
	tableQueryBuilder "github.com/donnyhardyanto/dxlib/tables/query_builder"
	"github.com/donnyhardyanto/dxlib/utils"
)

// === Paging Support ===

// === Standalone Paging Functions ===

// NamedQueryPaging executes a paging query using databases.DXDatabase
func NamedQueryPaging(
	dxDb3 *databases.DXDatabase,
	fieldTypeMapping db.DXDatabaseTableFieldTypeMapping,
	tableName string,
	rowPerPage, pageIndex int64,
	whereClause, orderBy string,
	args utils.JSON,
) (*PagingResult, error) {
	if dxDb3 == nil {
		return nil, errors.New("database3 connection is nil")
	}

	if err := dxDb3.EnsureConnection(); err != nil {
		return nil, err
	}

	rowsInfo, list, totalRows, totalPages, _, err := db.NamedQueryPaging(
		dxDb3.Connection,
		fieldTypeMapping,
		"",
		rowPerPage,
		pageIndex,
		"*",
		tableName,
		whereClause,
		"",
		orderBy,
		args,
	)
	if err != nil {
		return nil, err
	}

	return &PagingResult{
		RowsInfo:   rowsInfo,
		Rows:       list,
		TotalRows:  totalRows,
		TotalPages: totalPages,
	}, nil
}

// NamedQueryPagingWithBuilder executes a paging query using TableSelectQueryBuilder
func NamedQueryPagingWithBuilder(
	dxDb3 *databases.DXDatabase,
	fieldTypeMapping db.DXDatabaseTableFieldTypeMapping,
	tableName string,
	rowPerPage, pageIndex int64,
	tqb *tableQueryBuilder.TableSelectQueryBuilder,
	orderBy string,
) (*PagingResult, error) {
	whereClause, args, err := tqb.Build()
	if err != nil {
		return nil, err
	}
	return NamedQueryPaging(dxDb3, fieldTypeMapping, tableName, rowPerPage, pageIndex, whereClause, orderBy, args)
}
