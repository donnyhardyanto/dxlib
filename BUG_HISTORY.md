# Fixed bugs

Entries move here from [BUG_OUTSTANDING.md](BUG_OUTSTANDING.md) when fixed, newest first.

## BUG-002: 15 open code-scanning alerts (CodeQL, Scorecard, SonarCloud)

- **Reported**: 2026-10-28, by the owner, from the repository's code-scanning page: four CodeQL
  `go/incorrect-integer-conversion` alerts (`utils/xlsx/reader.go`, `internal/xlsxtest/xlsxtest.go`,
  `api/openapi_read.go` twice), one CodeQL `go/weak-crypto-key` alert in the test helper
  `utils/tls/tlstest`, two SonarCloud alerts (`read-all` permissions in the Scorecard workflow; the Dart
  example without a `pubspec.lock`) and eight OpenSSF Scorecard findings (token permissions, security
  policy, vulnerabilities, SAST, code review, dependency update tool, branch protection, CII badge).
- **Cause**: the `_xHHHH_` decoders parsed four hex digits with a 32-bit `ParseUint` and cast the value to
  a rune, and `openAPICount` checked an `int64` fits an `int` with a round trip, which CodeQL does not read
  as a bound. The release workflow granted `contents: write` to every job; the Scorecard workflow used
  `read-all`; `SECURITY.md` named the reporting page without a link; Scorecard runs osv-scanner without
  Go call analysis, so the accepted GO-2026-5932 (x/crypto/openpgp, not called, no fixed version) counted
  against the repository; the Dart example ignored its lock file.
- **Fixed**: 2026-10-28. The decoders parse the four digits as a 16-bit value, `openAPICount` compares
  against `math.MaxInt`, each with tests at the edges; the release workflow is read-only except the
  GoReleaser job; the Scorecard workflow names its read permissions; `SECURITY.md` links the private
  reporting page; `osv-scanner.toml` records the accepted advisory at the root and beside the SBOM records;
  the Dart example commits `pubspec.lock`. The weak test key is dismissed as test-only: the 1024-bit RSA
  keys exist for the `KEY_TOO_WEAK` negative tests. The Scorecard items that need a repository setting or
  a policy (branch protection, code review, a dependency bot, a CodeQL workflow in place of the default
  setup, the CII badge) were put to the owner and settled per the answers.
