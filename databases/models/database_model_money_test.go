package models

import (
	"testing"

	"github.com/donnyhardyanto/dxlib/types"
	"github.com/shopspring/decimal"
)

// A money field takes the decimal string a client sends and the decimal.Decimal
// a money request parameter resolves to; a binary float is still refused, since
// it may already have lost digits.
func TestValidateFieldValueMoneyTakesStringAndDecimal(t *testing.T) {
	tbl := &ModelDBTable{}
	field := &ModelDBField{Type: types.DataTypeMoney}
	d, _ := decimal.NewFromString("1250000.375")

	for _, ok := range []any{"1250000.375", d, nil} {
		if err := tbl.validateFieldValue("amount", field, ok); err != nil {
			t.Errorf("%T %v refused: %v", ok, ok, err)
		}
	}
	for _, bad := range []any{1250000.375, int64(5), true} {
		if err := tbl.validateFieldValue("amount", field, bad); err == nil {
			t.Errorf("%T %v accepted, want an error", bad, bad)
		}
	}
}
