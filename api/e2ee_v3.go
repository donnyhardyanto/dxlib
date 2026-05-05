package api

import (
	"github.com/donnyhardyanto/dxlib/utils"
	"github.com/donnyhardyanto/dxlib/utils/lv"
)

// V3 (persistent-session inner envelope) hooks.
//
// V3 supports three on-the-wire shapes, dispatched inside the host-supplied
// implementation by inspecting the body bytes:
//
//   1. New format (bulk request, post-bootstrap):
//        { "connection_id": "<hex>", "data": "<base64>" }
//        data = iv(16) || aes-256-cbc-pkcs7-ct || hmac-sha256(32)
//
//   2. Bootstrap (one-shot ECDH at /v1/startup_1):
//        { "data": "<base64 of L32V-packed client public keys>" }
//        Used to establish a new session and obtain a connection_id.
//
//   3. Legacy (already-shipped mobile builds, request-only encryption):
//        { "data": "<base64 of OpenSSL Salted__ envelope>" }
//        Refused after the host's deadline.
//
// dxlib does not interpret these shapes. It only routes the request body to
// OnE2EEV3Unpack and the response payload to OnE2EEV3Pack. Whatever opaque
// state the host needs to carry between unpack and pack (connection_id,
// mode flag, derived sub-keys, etc.) is stuffed into utils.JSON `state` —
// the same pattern V2 uses with EncryptionParameters.

// OnE2EEV3Unpack decrypts an incoming V3 request body and returns the
// plaintext payload as LV elements plus opaque state for the matching
// pack call.
//
// Implementations should:
//   - Detect whether the body is bootstrap, bulk, or legacy.
//   - For bulk: load the AES + HMAC keys from Redis under connection_id,
//     verify HMAC, AES-CBC decrypt, return the plaintext as LVs.
//   - For bootstrap: parse the L32V client public keys, run ECDH +
//     optionally ML-KEM, persist the session in Redis, set state["mode"]
//     so Pack knows to return the L32V bootstrap response, and place
//     the L32V bootstrap response into the LV slice OR into state for
//     Pack to retrieve.
//   - For legacy: run OpenSSL EVP_BytesToKey + AES-CBC-PKCS7 decrypt.
//     Set state["legacy"] = true so Pack returns plaintext (legacy
//     mobile does not decrypt responses).
//   - Set state with whatever Pack needs.
//   - On any rejection, return an error with a stable code so the
//     surrounding api endpoint preprocessing can map it to the right
//     HTTP response.
var OnE2EEV3Unpack func(
	aepr *DXAPIEndPointRequest,
	bodyAsJSON utils.JSON,
) (
	lvPayloadElements []*lv.LV,
	state utils.JSON,
	err error,
)

// OnE2EEV3Pack encrypts the response and returns the bytes to write to the
// HTTP response body.
//
// Implementations should:
//   - Read state to decide which mode to use.
//   - For bulk: encrypt payloads with the session AES key and HMAC,
//     return { "connection_id": ..., "data": "<base64>" } as JSON bytes.
//   - For bootstrap: return { "data": "<base64 of L32V bootstrap response>" }
//     as JSON bytes (the bootstrap response is signed but otherwise
//     plaintext — the session key is what /v1/startup_1 establishes).
//   - For legacy: return plaintext JSON. The published mobile app does
//     not decrypt responses.
var OnE2EEV3Pack func(
	aepr *DXAPIEndPointRequest,
	state utils.JSON,
	payloads ...*lv.LV,
) (responseBody []byte, err error)
