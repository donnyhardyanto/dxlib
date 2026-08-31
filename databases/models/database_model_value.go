package models

import (
	"strings"
	"time"

	"github.com/donnyhardyanto/dxlib/base"
	"github.com/donnyhardyanto/dxlib/types"
)

// NormalizeFieldValueForDBType converts a value into the form the column it is
// bound to actually stores, for the cases where that differs by engine.
//
// Today there is exactly one such case, and it exists because MariaDB has no
// column type that can hold a UTC offset. DataTypeISO8601 is therefore ISO-8601
// text there and a real instant type everywhere else -- see the comment on
// DataTypeISO8601 for why -- and text needs the value written in the layout the
// column's ordering depends on.
//
// Without this the driver decides, and what it decides is a connection setting.
// go-sql-driver/mysql encodes a time.Time as a MySQL datetime literal, which
// MariaDB then coerces into the VARCHAR -- not ISO-8601, no offset, and rendered
// in whatever zone the DSN's `loc` names. Measured against MariaDB 11.8 with one
// instant, 2026-08-31T19:00:00+07:00:
//
//	loc unset (UTC)     stored "2026-08-31 12:00:00"
//	loc=Asia/Jakarta    stored "2026-08-31 19:00:00"
//	through this func   stored "2026-08-31T12:00:00.000000000Z" either way
//
// One instant, two different strings, and nothing in the column says which rule
// produced it. That is the exact failure DataTypeISO8601 exists to prevent, so
// the DDL change and this conversion are one change; neither is correct alone.
// Text that is not ISO-8601 is also strictly worse than the DATETIME it replaced,
// which at least had date semantics.
//
// A string that already parses as a timestamp is re-rendered rather than passed
// through, so a caller handing over "2026-08-31T12:00:00+07:00" gets the same
// instant, stored in the one layout that sorts. A string that does not parse is
// left exactly as it was: this function normalises, it does not validate, and
// swallowing an unparseable value here would hide it from the error the database
// is about to raise.
func NormalizeFieldValueForDBType(field *ModelDBField, dbType base.DXDatabaseType, val any) any {
	if val == nil || field == nil {
		return val
	}
	if field.Type.GoType != types.GoTypeTime {
		return val
	}
	if !storesTimeAsText(field, dbType) {
		return val
	}

	switch v := val.(type) {
	case time.Time:
		return v.UTC().Format(types.ISO8601TextLayout)
	case *time.Time:
		if v == nil {
			return val
		}
		return v.UTC().Format(types.ISO8601TextLayout)
	case string:
		// Accept what the API layer produces for this type, which is RFC3339 of
		// whatever precision the caller sent.
		for _, layout := range []string{time.RFC3339Nano, time.RFC3339, types.ISO8601TextLayout} {
			if ts, err := time.Parse(layout, v); err == nil {
				return ts.UTC().Format(types.ISO8601TextLayout)
			}
		}
		return val
	}
	return val
}

// storesTimeAsText reports whether this field's column is a character type on
// this engine. Asked of the resolved SQL type rather than of the engine, so a
// model that overrides TypeByDatabaseType for its own reasons is answered
// correctly instead of by an assumption about which engine is which.
func storesTimeAsText(field *ModelDBField, dbType base.DXDatabaseType) bool {
	if field.Type.TypeByDatabaseType == nil {
		return false
	}
	sqlType := strings.ToUpper(field.Type.TypeByDatabaseType[dbType])
	return strings.Contains(sqlType, "CHAR") || strings.Contains(sqlType, "TEXT")
}
