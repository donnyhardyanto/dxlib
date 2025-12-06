package api

import (
	"github.com/donnyhardyanto/dxlib/utils"
	"github.com/donnyhardyanto/dxlib/utils/lv"
)

var OnE2EEPrekeyUnPack func(aepr *DXAPIEndPointRequest, prekeyIndex string, dataAsHexString string) (lvPayloadElements []*lv.LV, sharedKey2AsBytes []byte, edB0PrivateKeyAsBytes []byte, preKeyData utils.JSON, err error)
var OnE2EEPrekeyPack func(aepr *DXAPIEndPointRequest, preKeyIndex string, edB0PrivateKeyAsBytes []byte, sharedKey2AsBytes []byte, payloads ...*lv.LV) (dataBlockEnvelopeAsHexString string, err error)
