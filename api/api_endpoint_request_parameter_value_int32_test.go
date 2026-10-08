package api

import (
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	dxlibTypes "github.com/donnyhardyanto/dxlib/types"
	utilsHttp "github.com/donnyhardyanto/dxlib/utils/http"
)

// An int32 parameter takes the same wire shapes as an int64 one (a JSON number
// arrives as float64, a query-string value as a string) and refuses anything
// outside the int32 range on either path.
func TestResolveInt32AcceptsNumbersAndStringsWithinRange(t *testing.T) {
	for _, c := range []struct {
		name  string
		kind  dxlibTypes.APIParameterType
		raw   any
		want  int32
		fails bool
	}{
		{"json number", dxlibTypes.APIParameterTypeInt32, float64(12345), 12345, false},
		{"decimal string", dxlibTypes.APIParameterTypeInt32, "12345", 12345, false},
		{"negative number", dxlibTypes.APIParameterTypeInt32, float64(-7), -7, false},
		{"negative string", dxlibTypes.APIParameterTypeInt32, "-7", -7, false},
		{"max as number", dxlibTypes.APIParameterTypeInt32, float64(math.MaxInt32), math.MaxInt32, false},
		{"min as number", dxlibTypes.APIParameterTypeInt32, float64(math.MinInt32), math.MinInt32, false},
		{"max as string", dxlibTypes.APIParameterTypeInt32, "2147483647", math.MaxInt32, false},
		{"min as string", dxlibTypes.APIParameterTypeInt32, "-2147483648", math.MinInt32, false},
		{"max+1 as number", dxlibTypes.APIParameterTypeInt32, float64(math.MaxInt32) + 1, 0, true},
		{"min-1 as number", dxlibTypes.APIParameterTypeInt32, float64(math.MinInt32) - 1, 0, true},
		{"max+1 as string", dxlibTypes.APIParameterTypeInt32, "2147483648", 0, true},
		{"min-1 as string", dxlibTypes.APIParameterTypeInt32, "-2147483649", 0, true},
		{"huge number", dxlibTypes.APIParameterTypeInt32, 1e30, 0, true},
		{"huge negative number", dxlibTypes.APIParameterTypeInt32, -1e30, 0, true},
		{"fractional number", dxlibTypes.APIParameterTypeInt32, 5.5, 0, true},
		{"negative fractional number", dxlibTypes.APIParameterTypeInt32, -5.5, 0, true},
		{"fractional string", dxlibTypes.APIParameterTypeInt32, "5.5", 0, true},
		{"empty string", dxlibTypes.APIParameterTypeInt32, "", 0, true},
		{"not a number", dxlibTypes.APIParameterTypeInt32, "abc", 0, true},
		{"padded string", dxlibTypes.APIParameterTypeInt32, " 5", 0, true},
		{"bool", dxlibTypes.APIParameterTypeInt32, true, 0, true},
		{"go int64 in range", dxlibTypes.APIParameterTypeInt32, int64(9), 9, false},
		{"go int64 out of range", dxlibTypes.APIParameterTypeInt32, int64(math.MaxInt32) + 1, 0, true},

		{"zero-positive takes zero", dxlibTypes.APIParameterTypeInt32ZP, "0", 0, false},
		{"zero-positive takes zero number", dxlibTypes.APIParameterTypeInt32ZP, float64(0), 0, false},
		{"zero-positive rejects negative", dxlibTypes.APIParameterTypeInt32ZP, "-1", 0, true},
		{"positive rejects zero", dxlibTypes.APIParameterTypeInt32P, "0", 0, true},
		{"positive rejects negative number", dxlibTypes.APIParameterTypeInt32P, float64(-3), 0, true},
		{"positive takes one", dxlibTypes.APIParameterTypeInt32P, "1", 1, false},
		{"nullable takes a value", dxlibTypes.APIParameterTypeNullableInt32, float64(42), 42, false},
		{"nullable rejects out of range", dxlibTypes.APIParameterTypeNullableInt32, float64(math.MaxInt32) + 1, 0, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			v := &DXAPIEndPointRequestParameterValue{
				Owner:    &DXAPIEndPointRequest{},
				Metadata: DXAPIEndPointParameter{NameId: "n", Type: c.kind},
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
			if got, ok := v.Value.(int32); !ok || got != c.want {
				t.Fatalf("got %#v, want int32 %d", v.Value, c.want)
			}
		})
	}
}

// A nullable-int32 means the parameter may be left out; absent stays nil and is
// never read as zero, while the Go type of a present value is a plain int32.
func TestResolveNullableInt32KeepsNil(t *testing.T) {
	v := &DXAPIEndPointRequestParameterValue{
		Owner:    &DXAPIEndPointRequest{},
		Metadata: DXAPIEndPointParameter{NameId: "n", Type: dxlibTypes.APIParameterTypeNullableInt32},
		RawValue: nil,
	}
	if err := v.Validate(); err != nil {
		t.Fatal(err)
	}
	if v.Value != nil {
		t.Fatalf("got %#v, want nil", v.Value)
	}
}

func TestNewParameterMarksNullableInt32Nullable(t *testing.T) {
	ep := &DXAPIEndPoint{}
	p := ep.NewParameter(nil, "n", dxlibTypes.APIParameterTypeNullableInt32, "", false)
	if !p.IsNullable {
		t.Fatal("nullable-int32 parameter is not marked nullable")
	}
	q := ep.NewParameter(nil, "m", dxlibTypes.APIParameterTypeInt32, "", true)
	if q.IsNullable {
		t.Fatal("int32 parameter is marked nullable")
	}
}

// The real request path: a JSON body and a GET query string both reach the
// handler as int32 values, and the typed getter reads them.
func TestPreProcessRequestAcceptsInt32Parameters(t *testing.T) {
	params := []DXAPIEndPointParameter{
		{NameId: "count", Type: dxlibTypes.APIParameterTypeInt32, IsMustExist: true},
		{NameId: "page", Type: dxlibTypes.APIParameterTypeInt32P, IsMustExist: true},
		{NameId: "offset", Type: dxlibTypes.APIParameterTypeInt32ZP, IsMustExist: true},
		{NameId: "parent", Type: dxlibTypes.APIParameterTypeNullableInt32, IsNullable: true},
	}

	t.Run("json body", func(t *testing.T) {
		r := httptest.NewRequest(http.MethodPost, "/probe", strings.NewReader(`{"count": -5, "page": 2, "offset": 0}`))
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
		assertInt32Param(t, aepr, "count", -5)
		assertInt32Param(t, aepr, "page", 2)
		assertInt32Param(t, aepr, "offset", 0)
		isExist, _, err := aepr.GetParameterValueAsInt32("parent")
		if err != nil || isExist {
			t.Fatalf("absent nullable-int32: isExist=%v err=%v", isExist, err)
		}
	})

	t.Run("query string", func(t *testing.T) {
		r := httptest.NewRequest(http.MethodGet, "/probe?count=7&page=1&offset=3&parent=11", nil)
		aepr, rec := newPreProcessContext(r, &DXAPIEndPoint{
			Method:       http.MethodGet,
			EndPointType: EndPointTypeHTTPJSON,
			Parameters:   params,
		})
		if err := aepr.PreProcessRequest(); err != nil {
			t.Fatalf("request rejected (status %d): %v", rec.Code, err)
		}
		assertInt32Param(t, aepr, "count", 7)
		assertInt32Param(t, aepr, "page", 1)
		assertInt32Param(t, aepr, "offset", 3)
		assertInt32Param(t, aepr, "parent", 11)
	})

	t.Run("json body out of range is 422", func(t *testing.T) {
		r := httptest.NewRequest(http.MethodPost, "/probe", strings.NewReader(`{"count": 2147483648, "page": 2, "offset": 0}`))
		r.Header.Set("Content-Type", "application/json")
		aepr, rec := newPreProcessContext(r, &DXAPIEndPoint{
			Method:             http.MethodPost,
			EndPointType:       EndPointTypeHTTPJSON,
			RequestContentType: utilsHttp.RequestContentTypeApplicationJSON,
			Parameters:         params,
		})
		if err := aepr.PreProcessRequest(); err == nil {
			t.Fatal("out-of-range int32 was accepted")
		}
		if rec.Code != http.StatusUnprocessableEntity {
			t.Fatalf("status = %d, want 422", rec.Code)
		}
	})
}

func assertInt32Param(t *testing.T, aepr *DXAPIEndPointRequest, name string, want int32) {
	t.Helper()
	isExist, got, err := aepr.GetParameterValueAsInt32(name)
	if err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	if !isExist {
		t.Fatalf("%s: not present", name)
	}
	if got != want {
		t.Fatalf("%s = %d, want %d", name, got, want)
	}
}
