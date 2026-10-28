# OpenSSF Best Practices badge: passing-level answers

The answers to paste at <https://www.bestpractices.dev/> for dxlib, one per criterion, in the order the
form shows them. The form takes a status (Met, Unmet, N/A) and a justification; a `{Met URL}` criterion
needs a URL in the justification. Facts here are as of 2026-10-28; keep them true when the project
changes, and update the badge entry at least yearly.

Registration: sign in with the GitHub account that owns the repository. Project name `dxlib`, project
URL and repository URL both `https://github.com/donnyhardyanto/dxlib`, programming language Go,
description: "Go library for building backend services: HTTP API server with typed parameters and
OpenAPI, multi-database access, logging, configuration, Redis, TLS policy, xlsx reading and writing,
OpenTelemetry."

Three criteria cannot be Met as things stand and are answered honestly below: `discussion` (issues and
discussions are off; bug reports go by email), `crypto_weaknesses` (one finding, tracked privately as
`SECURITY.md` requires until its fix is released) and `test_most` (35% statement coverage). `discussion`
is a MUST, so the badge stays below passing until a public, URL-addressable discussion channel exists;
everything else at passing level is Met or N/A.

## Basics

| Criterion | Status | Justification |
| --- | --- | --- |
| description_good | Met | README.md opens with what dxlib is and provides: https://github.com/donnyhardyanto/dxlib#dxlib |
| interact | Met | README.md "Reporting bugs and contributing" says how to obtain the module (go.mod pin by tag), where to report bugs and enhancements (email, named there) and how changes are made: https://github.com/donnyhardyanto/dxlib#reporting-bugs-and-contributing |
| contribution | Met | The same section explains the process: outside pull requests are not accepted; reports go by email and the maintainer makes the change, with the bar every change meets. https://github.com/donnyhardyanto/dxlib#reporting-bugs-and-contributing |
| contribution_requirements | Met | The same section names the requirements: gofmt, go vet, go test -race clean, a test per change, and the dependency scan and licence check for dependency changes. https://github.com/donnyhardyanto/dxlib#reporting-bugs-and-contributing |
| floss_license | Met | MIT License. |
| floss_license_osi | Met | MIT is OSI-approved. |
| license_location | Met | https://github.com/donnyhardyanto/dxlib/blob/main/LICENSE |
| documentation_basics | Met | README.md (what it is, how to take it, dependency and security practice) and LIBRARY.md (every package and exported identifier with its purpose). |
| documentation_interface | Met | LIBRARY.md documents every exported type, constant, variable and function, and pkg.go.dev renders the Go reference documentation: https://pkg.go.dev/github.com/donnyhardyanto/dxlib |
| sites_https | Met | The project site, repository and downloads are GitHub over HTTPS; the Go module proxy serves the module over HTTPS. |
| discussion | Unmet | GitHub issues and discussions are off by policy (public issues were being used as a lead-in to malicious pull requests). Bug reports and enhancement requests go by email, which is not searchable or addressable by URL. |
| english | Met | Documentation, code comments, commit messages and release notes are in English; reports in English are accepted. |
| maintained | Met | Active: releases v1.131.0, v1.132.0 and v1.132.1 in October 2026, commits most days. https://github.com/donnyhardyanto/dxlib/commits/main |

## Change Control

| Criterion | Status | Justification |
| --- | --- | --- |
| repo_public | Met | https://github.com/donnyhardyanto/dxlib |
| repo_track | Met | Git history with author, date and message for every change. |
| repo_interim | Met | Every commit lands on `main`; releases are tags on that history, so interim versions are public. |
| repo_distributed | Met | git. |
| version_unique | Met | Each release is a tag `vX.Y.Z` and a GitHub release of the same name. |
| version_semver | Met | Semantic Versioning: https://github.com/donnyhardyanto/dxlib/tags |
| version_tags | Met | Every release is a git tag; consumers pin the tag in go.mod. |
| release_notes | Met | Each GitHub release carries a hand-written summary of consumer-facing changes, with the grouped change list beneath it: https://github.com/donnyhardyanto/dxlib/releases |
| release_notes_vulns | N/A | No publicly known vulnerability in dxlib has had a CVE or advisory so far (0 advisories). When one is fixed, the release notes name it, as SECURITY.md says. |

## Reporting

| Criterion | Status | Justification |
| --- | --- | --- |
| report_process | Met | README.md "Reporting bugs and contributing": reports by email, acknowledged, tracked as BUG-NNN entries. https://github.com/donnyhardyanto/dxlib#reporting-bugs-and-contributing |
| report_tracker | Met | Individual bugs are tracked as `BUG-NNN` entries in BUG_OUTSTANDING.md and moved to BUG_HISTORY.md when fixed, each fixing commit saying `Fixes BUG-NNN`. https://github.com/donnyhardyanto/dxlib/blob/main/BUG_OUTSTANDING.md |
| report_responses | Met | Every report received is acknowledged. No outside bug report was received in the last 12 months; the entries in BUG_HISTORY.md are from the maintainer's own audits and were all resolved. |
| enhancement_responses | Met | No outside enhancement request was received in the last 12 months; every one received is answered. |
| report_archive | Met | BUG_OUTSTANDING.md, BUG_HISTORY.md and the git history (`Fixes BUG-NNN`) are the public, searchable archive: https://github.com/donnyhardyanto/dxlib/blob/main/BUG_HISTORY.md |
| vulnerability_report_process | Met | https://github.com/donnyhardyanto/dxlib/blob/main/SECURITY.md |
| vulnerability_report_private | Met | GitHub private vulnerability reporting is enabled; SECURITY.md links the form: https://github.com/donnyhardyanto/dxlib/security/advisories/new |
| vulnerability_report_response | N/A | No vulnerability report was received in the last 6 months. |

## Quality

| Criterion | Status | Justification |
| --- | --- | --- |
| build | Met | `go build ./...` from a clean checkout; the CI workflow builds with GOWORK=off and a read-only go.sum so a consumer's build is what is tested. https://github.com/donnyhardyanto/dxlib/blob/main/.github/workflows/ci.yml |
| build_common_tools | Met | The Go toolchain alone (go.mod names the version). |
| build_floss_tools | Met | Go is BSD-licensed; no other tool is needed to build. |
| test | Met | `go test -race ./...` over the whole module; the CI workflow runs it on every push and pull request, and README.md names the command. https://github.com/donnyhardyanto/dxlib/blob/main/.github/workflows/ci.yml |
| test_invocation | Met | `go test ./...`, the standard Go invocation. |
| test_most | Unmet | Statement coverage of the whole module is 35% (`go test -coverprofile`, 2026-10-28); the packages that handle untrusted input (the xlsx reader, the OpenAPI reader, the TLS policy, API parameter validation) are tested at their edges and fuzzed, the database and infrastructure clients less so. |
| test_continuous_integration | Met | GitHub Actions on every push to main and every pull request: build, vet, race tests, gofmt. https://github.com/donnyhardyanto/dxlib/actions |
| test_policy | Met | Every change comes with a test for the behaviour it adds or the bug it fixes; README.md "Reporting bugs and contributing" states it. |
| tests_are_added | Met | Recent changes each carry their tests, for example 32dc73c (bounded integer conversions with edge tests), 6a8cbe6 (raw cell values with golden files) and 26da2ff (reader limits with their tests). https://github.com/donnyhardyanto/dxlib/commits/main |
| tests_documented_added | Met | README.md "Reporting bugs and contributing", the "Changes" point. |
| warnings | Met | `go vet ./...` and `gofmt -l` run in CI on every push and fail it; CodeQL and SonarCloud analyse every push. |
| warnings_fixed | Met | The CI workflow (vet, gofmt, race tests) passed on the latest push to main when checked (2026-10-28); every CodeQL alert is fixed or dismissed with a written reason. |
| warnings_strict | Met | vet and gofmt are blocking, not advisory; CodeQL runs its security-and-quality suite. |

## Security

| Criterion | Status | Justification |
| --- | --- | --- |
| know_secure_design | Met | The maintainer designs the library's security mechanisms (the TLS policy in utils/tls with its design document, the API's end-to-end encryption, the hardened xlsx reader with explicit limits and fuzzing) and reviews every change against the usual design principles (least privilege, fail closed, explicit input bounds). |
| know_common_errors | Met | The maintainer knows the common error classes for Go services (injection through SQL and shell, unbounded input and integer conversion, TLS misconfiguration, weak key sizes and hashes, secret leakage) and the library counters them: query parameters through database/sql and sqlx, bounded readers and checked conversions, a TLS floor of 2048-bit RSA and TLS 1.2, argon2id and bcrypt for passwords, secret scanning and push protection on the repository. |
| crypto_published | Met | Only published, reviewed algorithms: TLS 1.2 and 1.3 from crypto/tls, X25519, AES, SHA-2, argon2id and bcrypt. |
| crypto_call | Met | All cryptography comes from the Go standard library and golang.org/x/crypto; nothing is re-implemented. |
| crypto_floss | Met | The Go standard library and golang.org/x/crypto are BSD-licensed. |
| crypto_keylength | Met | utils/tls refuses RSA keys under 2048 bits and ECDSA under P-256 by default (MinRSABitsFloor, MinECDSABits in utils/tls/policy.go) and TLS below 1.2; smaller sizes cannot be enabled below the floor. AES keys are 128 or 256 bits; X25519 for key agreement. |
| crypto_working | Met | No MD4, MD5, DES or RC4 anywhere in the module (grep over the source, 2026-10-28). TLS 1.2 is limited to ECDHE suites with AES-GCM or ChaCha20-Poly1305. |
| crypto_weaknesses | Unmet | One finding is tracked privately, as SECURITY.md requires, in a draft security advisory on the repository; it is fixed in a tagged release and the advisory published with it, after which this answer moves to Met. TLS uses AES-GCM and ChaCha20-Poly1305 only, and there is no SHA-1. |
| crypto_pfs | Met | TLS 1.3 always; the TLS 1.2 suite list holds only ECDHE suites, so every key agreement has forward secrecy. |
| crypto_password_storage | Met | utils/security stores passwords with bcrypt (default cost) or argon2id with a per-user salt (utils/security/security.go). |
| crypto_random | Met | Every key, nonce, IV and salt comes from crypto/rand; math/rand is not used outside tests. |
| delivery_mitm | Met | The module is fetched by the Go toolchain over HTTPS through the module proxy, and every download is checked against go.sum and the Go checksum database; releases are on GitHub over HTTPS. |
| delivery_unsigned | Met | No hash is fetched over http; the Go checksum database signs the checksums the toolchain verifies. |
| vulnerabilities_fixed_60_days | Met | None publicly known in dxlib. Dependencies are scanned (syft, grype, osv-scanner, govulncheck, trivy) before every dependency change and at every release, with the records committed in sbom/<tag>/. The one open advisory, GO-2026-5932, concerns an unmaintained package (x/crypto/openpgp) that dxlib does not call; it is recorded as accepted with its review date. |
| vulnerabilities_critical_fixed | Met | None reported; the process in SECURITY.md fixes a confirmed weakness in a new tagged release. |
| no_leaked_credentials | Met | GitHub secret scanning and push protection are on; no credential is in the repository (key files are gitignored). |

## Analysis

| Criterion | Status | Justification |
| --- | --- | --- |
| static_analysis | Met | CodeQL (GitHub default setup: Go, JavaScript, Actions) on every push and weekly; SonarCloud on every push; go vet in CI; govulncheck at every dependency change and release. |
| static_analysis_common_vulnerabilities | Met | CodeQL's security queries (for example go/incorrect-integer-conversion, go/weak-crypto-key) and govulncheck. |
| static_analysis_fixed | Met | Every CodeQL alert is fixed or dismissed with a written reason (the last four fixed 2026-10-28 with tests at the edges). |
| static_analysis_often | Met | CodeQL and SonarCloud run on every push to main. |
| dynamic_analysis | Met | `go test -race ./...` on every push; fuzz tests on the xlsx reader (FuzzOpen, FuzzSheetXML, FuzzSharedStrings, FuzzNumFmt in utils/xlsx) run before releases that touch it. |
| dynamic_analysis_unsafe | N/A | The module is Go only; it includes no C or C++ (no cgo). |
| dynamic_analysis_enable_assertions | Met | The race detector and the tests' own checks run on every push; the fuzz targets check invariants on every input. |
| dynamic_analysis_fixed | Met | None outstanding; a finding from the race detector or a fuzzer is fixed before the next release. |
