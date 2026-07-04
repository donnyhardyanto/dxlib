package api

// OnBeforePreProcessRequest is an OPTIONAL transport-authentication hook invoked by routeHandler
// IMMEDIATELY BEFORE PreProcessRequest() — i.e. before ANY request body handling, and before the
// E2EE inner-decrypt that PreProcessRequest triggers (OnE2EEV3Unpack / OnE2EEV4Unpack).
//
// Purpose (F-BE-02 / BUG-SEC-121): let a host authenticate the transport (e.g. validate the
// api-proxy-v4 X-Proxy-Token) BEFORE the framework decrypts / bootstraps a session / checks replay,
// so an unauthenticated caller can never reach those primitives.
//
// Contract:
//   - nil (default) => no-op, fully backward compatible (existing services behave exactly as before).
//   - non-nil and returns nil  => request proceeds to PreProcessRequest as normal.
//   - non-nil and returns error => routeHandler REJECTS the request with 401 and does NOT call
//     PreProcessRequest (no decrypt, no session create, no replay check). The hook may write its own
//     response; if it hasn't, routeHandler writes a generic 401.
//
// It is a package-level var (mirroring OnE2EEV4Unpack) so a host library (bms-common) wires it once.
// Only services that wire it are affected; it applies to every routeHandler request in that process,
// which for the mobile brokers is exactly the E2EE endpoints (health/OAM runs on a separate listener
// and never passes through routeHandler).
var OnBeforePreProcessRequest func(aepr *DXAPIEndPointRequest) error
