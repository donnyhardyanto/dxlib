package api

import (
	"github.com/donnyhardyanto/dxlib/utils/lv"
)

var OnE2EEPrekeyUnPack func(method DXAPIEndPointType, prekeyIndex string, dataAsHexString string) (lvPayloadElements []*lv.LV, sharedKey2AsBytes []byte, edB0PrivateKeyAsBytes []byte, err error)
var OnE2EEPrekeyPack func(method DXAPIEndPointType, preKeyIndex string, edB0PrivateKeyAsBytes []byte, sharedKey2AsBytes []byte, payloads ...*lv.LV) (dataBlockEnvelopeAsHexString string, err error)
