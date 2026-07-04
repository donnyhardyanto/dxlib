package api

import "os"

// logDecryptedBody gates whether DecryptedRequestDumpAsString emits the DECRYPTED request
// body/headers to logs (BUG-SEC-122). Default OFF: a backend must not write decrypted business
// data (KYC PII, balances, etc.) to the log store by default — and because that dump rides on
// Log.Errorf (Error level), lowering CONSOLE_LOG_LEVEL does NOT suppress it. A developer opts in
// for a live debugging session via DXLIB_LOG_DECRYPTED_BODY=true. The raw (still-encrypted)
// requestDump at the call sites is unaffected — only the plaintext dump is gated.
var logDecryptedBody = os.Getenv("DXLIB_LOG_DECRYPTED_BODY") == "true"

// SetLogDecryptedBody overrides the gate at runtime (used by tests).
func SetLogDecryptedBody(b bool) { logDecryptedBody = b }
