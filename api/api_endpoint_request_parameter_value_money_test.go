package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	dxlibTypes "github.com/donnyhardyanto/dxlib/types"
	utilsHttp "github.com/donnyhardyanto/dxlib/utils/http"
	"github.com/shopspring/decimal"
)

// Money arrives as a JSON string and is held as decimal.Decimal. A JSON number
// is refused: it has been through a binary float and may have lost digits.
func TestResolveMoneyTakesDecimalStringsOnly(t *testing.T) {
	for _, c := range []struct {
		name  string
		raw   any
		want  string
		fails bool
	}{
		{"decimal string", "1250000.375", "1250000.375", false},
		{"integer string", "7", "7", false},
		{"negative", "-12.5", "-12.5", false},
		{"explicit plus", "+3.25", "3.25", false},
		{"zero", "0", "0", false},
		{"leading dot", ".5", "0.5", false},
		{"trailing dot", "5.", "5", false},
		{"many decimals kept", "0.123456789", "0.123456789", false},
		{"big amount beyond float precision", "12345678901234567.8901", "12345678901234567.8901", false},
		{"json number", 1250000.375, "", true},
		{"json integer number", float64(7), "", true},
		{"empty", "", "", true},
		{"spaces", " 5", "", true},
		{"exponent", "1e5", "", true},
		{"two dots", "1.2.3", "", true},
		{"currency sign", "$5", "", true},
		{"thousands separator", "1,250", "", true},
		{"just a sign", "-", "", true},
		{"just a dot", ".", "", true},
		{"words", "abc", "", true},
		{"bool", true, "", true},
	} {
		t.Run(c.name, func(t *testing.T) {
			v := &DXAPIEndPointRequestParameterValue{
				Owner:    &DXAPIEndPointRequest{},
				Metadata: DXAPIEndPointParameter{NameId: "amount", Type: dxlibTypes.APIParameterTypeMoney},
				RawValue: c.raw,
			}
			err := v.Validate()
			if c.fails {
				if err == nil {
					t.Fatalf("expected %v to be rejected, got %#v", c.raw, v.Value)
				}
				return
			}
			if err != nil {
				t.Fatalf("rejected %v: %v", c.raw, err)
			}
			got, ok := v.Value.(decimal.Decimal)
			if !ok {
				t.Fatalf("got %T, want decimal.Decimal", v.Value)
			}
			want, _ := decimal.NewFromString(c.want)
			if !got.Equal(want) {
				t.Fatalf("got %s, want %s", got, want)
			}
		})
	}
}

// The real request path: a money string in a JSON body or a query string
// reaches the handler as decimal.Decimal, and a JSON number answers 422.
func TestPreProcessRequestAcceptsMoneyAsString(t *testing.T) {
	params := []DXAPIEndPointParameter{
		{NameId: "amount", Type: dxlibTypes.APIParameterTypeMoney, IsMustExist: true},
	}
	want, _ := decimal.NewFromString("1250000.375")

	t.Run("json string", func(t *testing.T) {
		r := httptest.NewRequest(http.MethodPost, "/probe", strings.NewReader(`{"amount": "1250000.375"}`))
		r.Header.Set("Content-Type", "application/json")
		aepr, rec := newPreProcessContext(r, &DXAPIEndPoint{
			Method:             http.MethodPost,
			EndPointType:       EndPointTypeHTTPJSON,
			RequestContentType: utilsHttp.RequestContentTypeApplicationJSON,
			Parameters:         params,
		})
		if err := aepr.PreProcessRequest(); err != nil {
			t.Fatalf("request rejected (status %d): %v", rec.Code, err)
		}
		isExist, got, err := aepr.GetParameterValueAsDecimal("amount")
		if err != nil || !isExist {
			t.Fatalf("amount: isExist=%v err=%v", isExist, err)
		}
		if !got.Equal(want) {
			t.Fatalf("amount = %s, want %s", got, want)
		}
	})

	t.Run("query string", func(t *testing.T) {
		r := httptest.NewRequest(http.MethodGet, "/probe?amount=1250000.375", nil)
		aepr, rec := newPreProcessContext(r, &DXAPIEndPoint{
			Method:       http.MethodGet,
			EndPointType: EndPointTypeHTTPJSON,
			Parameters:   params,
		})
		if err := aepr.PreProcessRequest(); err != nil {
			t.Fatalf("request rejected (status %d): %v", rec.Code, err)
		}
		_, got, err := aepr.GetParameterValueAsDecimal("amount")
		if err != nil || !got.Equal(want) {
			t.Fatalf("amount = %s err=%v, want %s", got, err, want)
		}
	})

	t.Run("json number is 422", func(t *testing.T) {
		r := httptest.NewRequest(http.MethodPost, "/probe", strings.NewReader(`{"amount": 1250000.375}`))
		r.Header.Set("Content-Type", "application/json")
		aepr, rec := newPreProcessContext(r, &DXAPIEndPoint{
			Method:             http.MethodPost,
			EndPointType:       EndPointTypeHTTPJSON,
			RequestContentType: utilsHttp.RequestContentTypeApplicationJSON,
			Parameters:         params,
		})
		if err := aepr.PreProcessRequest(); err == nil {
			t.Fatal("money as a JSON number was accepted")
		}
		if rec.Code != http.StatusUnprocessableEntity {
			t.Fatalf("status = %d, want 422", rec.Code)
		}
	})
}
