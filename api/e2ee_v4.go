package api

import (
	"github.com/donnyhardyanto/dxlib/utils"
	"github.com/donnyhardyanto/dxlib/utils/lv"
)

// V4 (persistent-session inner envelope, little-endian LVLE) hooks.
//
// V4 is identical to V3 in protocol semantics. The only difference is
// the LV framing: V3 uses 4-byte BIG-ENDIAN length prefixes (lv.LV),
// V4 uses 4-byte LITTLE-ENDIAN length prefixes (lv.LVLE).
//
// Use EndPointTypeHTTPEndToEndEncryptionV4 for endpoints whose host
// implementation works natively with lv.LVLE, eliminating the LV↔LVLE
// conversion that V3 requires.

// OnE2EEV4Unpack decrypts an incoming V4 request body and returns the
// plaintext payload as LVLE elements plus opaque state for the matching
// pack call.
var OnE2EEV4Unpack func(
	aepr *DXAPIEndPointRequest,
	bodyAsJSON utils.JSON,
) (
	lvPayloadElements []*lv.LVLE,
	state utils.JSON,
	err error,
)

// OnE2EEV4Pack encrypts the response and returns the bytes to write to the
// HTTP response body. Payloads are LVLE (little-endian) containers.
var OnE2EEV4Pack func(
	aepr *DXAPIEndPointRequest,
	state utils.JSON,
	payloads ...*lv.LVLE,
) (responseBody []byte, err error)
