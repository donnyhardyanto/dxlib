package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/donnyhardyanto/dxlib/utils"
	utilsHttp "github.com/donnyhardyanto/dxlib/utils/http"
	"github.com/donnyhardyanto/dxlib/utils/lv"
)

// The prekey hook decodes client-supplied data; a payload with fewer than two
// elements must be answered as corrupt, not indexed past its end.
func TestE2EEV2ShortPayloadIsRejected(t *testing.T) {
	saved := OnE2EEPrekeyUnPack
	defer func() { OnE2EEPrekeyUnPack = saved }()
	OnE2EEPrekeyUnPack = func(aepr *DXAPIEndPointRequest, prekeyIndex string, dataAsHexString string) ([]*lv.LV, []byte, []byte, utils.JSON, error) {
		return []*lv.LV{{}}, nil, nil, nil, nil
	}

	r := httptest.NewRequest(http.MethodPost, "/probe", strings.NewReader(`{"i":"1","d":"00"}`))
	r.Header.Set("Content-Type", "application/json")
	aepr, rec := newPreProcessContext(r, &DXAPIEndPoint{
		Method:             http.MethodPost,
		EndPointType:       EndPointTypeHTTPEndToEndEncryptionV2,
		RequestContentType: utilsHttp.RequestContentTypeApplicationJSON,
	})

	if err := aepr.PreProcessRequest(); err == nil {
		t.Fatal("short payload accepted")
	}
	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422", rec.Code)
	}
}
