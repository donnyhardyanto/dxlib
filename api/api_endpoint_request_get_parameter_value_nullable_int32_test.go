package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	dxlibTypes "github.com/donnyhardyanto/dxlib/types"
	utilsHttp "github.com/donnyhardyanto/dxlib/utils/http"
)

func newNullableInt32Request(t *testing.T, body string) *DXAPIEndPointRequest {
	t.Helper()
	r := httptest.NewRequest(http.MethodPost, "/probe", strings.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	aepr, rec := newPreProcessContext(r, &DXAPIEndPoint{
		Method:             http.MethodPost,
		EndPointType:       EndPointTypeHTTPJSON,
		RequestContentType: utilsHttp.RequestContentTypeApplicationJSON,
		Parameters: []DXAPIEndPointParameter{
			{NameId: "parent", Type: dxlibTypes.APIParameterTypeNullableInt32, IsNullable: true},
		},
	})
	if err := aepr.PreProcessRequest(); err != nil {
		t.Fatalf("request rejected (status %d): %v", rec.Code, err)
	}
	return aepr
}

// A nullable-int32 sent as JSON null and one left out both mean not given:
// the getter answers isExist false and nil for both, like its int64 and
// string siblings.
func TestGetParameterValueAsNullableInt32(t *testing.T) {
	t.Run("value", func(t *testing.T) {
		aepr := newNullableInt32Request(t, `{"parent": 11}`)
		isExist, v, err := aepr.GetParameterValueAsNullableInt32("parent")
		if err != nil || !isExist || v == nil || *v != 11 {
			t.Fatalf("got isExist=%v v=%v err=%v, want true 11", isExist, v, err)
		}
	})
	for name, body := range map[string]string{"absent": `{}`, "json null": `{"parent": null}`} {
		t.Run(name, func(t *testing.T) {
			aepr := newNullableInt32Request(t, body)
			isExist, v, err := aepr.GetParameterValueAsNullableInt32("parent")
			if err != nil || isExist || v != nil {
				t.Fatalf("got isExist=%v v=%v err=%v, want false nil", isExist, v, err)
			}
			isExist, v, err = aepr.GetParameterValueAsNullableInt32("parent", int32(5))
			if err != nil || isExist || v == nil || *v != 5 {
				t.Fatalf("with default: got isExist=%v v=%v err=%v, want false 5", isExist, v, err)
			}
			isExist, v, err = aepr.GetParameterValueAsNullableInt32("parent", nil)
			if err != nil || isExist || v != nil {
				t.Fatalf("with nil default: got isExist=%v v=%v err=%v, want false nil", isExist, v, err)
			}
		})
	}
	t.Run("mandatory parameter sent as json null", func(t *testing.T) {
		ep := &DXAPIEndPoint{
			Method:             http.MethodPost,
			EndPointType:       EndPointTypeHTTPJSON,
			RequestContentType: utilsHttp.RequestContentTypeApplicationJSON,
		}
		ep.NewParameter(nil, "parent", dxlibTypes.APIParameterTypeNullableInt32, "", true)
		r := httptest.NewRequest(http.MethodPost, "/probe", strings.NewReader(`{"parent": null}`))
		r.Header.Set("Content-Type", "application/json")
		aepr, rec := newPreProcessContext(r, ep)
		if err := aepr.PreProcessRequest(); err != nil {
			t.Fatalf("request rejected (status %d): %v", rec.Code, err)
		}
		isExist, v, err := aepr.GetParameterValueAsNullableInt32("parent")
		if err != nil || isExist || v != nil {
			t.Fatalf("got isExist=%v v=%v err=%v, want false nil", isExist, v, err)
		}
	})
	t.Run("default of another type is refused", func(t *testing.T) {
		aepr := newNullableInt32Request(t, `{}`)
		if _, _, err := aepr.GetParameterValueAsNullableInt32("parent", 5); err == nil {
			t.Fatal("an int default was accepted")
		}
	})
	t.Run("parameter of another type is refused", func(t *testing.T) {
		aepr := newNullableInt32Request(t, `{"parent": 3}`)
		aepr.ParameterValues["parent"].Value = int64(3)
		if _, _, err := aepr.GetParameterValueAsNullableInt32("parent"); err == nil {
			t.Fatal("an int64 value was read as int32")
		}
	})
}
