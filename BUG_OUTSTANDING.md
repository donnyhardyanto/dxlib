# Open bugs

One `BUG-NNN` entry per bug; a fixed entry moves to [BUG_HISTORY.md](BUG_HISTORY.md) with the fixing
commit, which says `Fixes BUG-NNN`. The repository's GitHub issues are disabled.

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
- **Fix committed**: 2026-10-28, commits 32dc73c (the conversions, with tests at the edges) and 0819fd6
  (the posture files). The decoders parse the four digits as a 16-bit value, `openAPICount` compares
  against `math.MaxInt`, each with tests at the edges; the release workflow is read-only except the
  GoReleaser job; the Scorecard workflow names its read permissions; `SECURITY.md` links the private
  reporting page; `osv-scanner.toml` records the accepted advisory at the root and beside the SBOM records;
  the Dart example commits `pubspec.lock`. The weak test key is dismissed as test-only: the 1024-bit RSA
  keys exist for the `KEY_TOO_WEAK` negative tests. The Scorecard items that need a repository setting or
  a policy were put to the owner: on the answers, branch protection, code review, a dependency bot and a
  CodeQL workflow in place of the default setup are dismissed as won't fix, each with its reason on the
  alert (a one-owner repository whose review flow pushes to `main`; dependencies bumped on purpose with a
  scan per bump; CodeQL running through the default setup). The badge alert stays open until the owner
  registers the project at bestpractices.dev with the answers in `docs/openssf-best-practices.md`.
- **Awaiting confirmation**: the entry moves to `BUG_HISTORY.md` once the scans after the push close the
  nine alerts the commits fix: CodeQL #100, #90, #88, #87; SonarCloud #99, #74; Scorecard #93, #92, #95.
