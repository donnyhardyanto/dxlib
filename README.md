# dxlib

dxlib is a Go library for building backend services. It provides an HTTP API server with typed endpoint
parameters, OpenAPI reading and writing and end-to-end encryption; connections to PostgreSQL, MySQL and
MariaDB, Oracle and SQL Server behind one table abstraction; structured logging; configuration files with
sensitive-value masking; Redis with rate limiting, sessions and single sign-on; a TLS policy for servers and
clients; a streaming writer and a hardened reader of `.xlsx` files; OpenTelemetry metrics; object storage;
and the small utilities those share. [LIBRARY.md](LIBRARY.md) lists every package and exported identifier,
and <https://pkg.go.dev/github.com/donnyhardyanto/dxlib> renders the reference documentation. Consumers
import it by module path and pin a release tag in their `go.mod`.

Released under the MIT License; see [LICENSE](LICENSE). Third-party credits are in [NOTICE](NOTICE).

[]: # with the following snippet from README.md:
[]: # # dxlib

## Reporting bugs and contributing

GitHub issues and discussions are off on this repository, and pull requests from outside contributors are
not accepted: public issues are used as a lead-in to malicious pull requests that claim to fix them, so bugs
are tracked in files instead.

- **Bug reports and enhancement requests**: email <hardyanto.donny@gmail.com> with `dxlib` in the subject,
  the version tag, what you ran and what happened. Every report is acknowledged; a confirmed bug gets a
  `BUG-NNN` entry in [BUG_OUTSTANDING.md](BUG_OUTSTANDING.md), moves to [BUG_HISTORY.md](BUG_HISTORY.md)
  when fixed, and the fixing commit says `Fixes BUG-NNN`. Those two files and the git history are the public
  archive of reports and their outcome.
- **Security weaknesses**: never by email or in public; follow [SECURITY.md](SECURITY.md).
- **Changes**: the maintainer makes them from the reports. Every change meets the same bar before it is
  committed: `gofmt`, `go vet` and `go test -race ./...` clean (the CI workflow runs the same on every
  push), a test for each new behaviour and for each bug fixed, and the dependency scan and licence check
  below for any dependency change.

## Dependencies

Every new or upgraded dependency is scanned and its licence checked before the change is committed.
From the repo root, after `go get` and `go mod tidy`:

    syft dir:. -q -o cyclonedx-json=/tmp/dxlib-sbom.cdx.json
    grype sbom:/tmp/dxlib-sbom.cdx.json
    osv-scanner scan source -r .
    govulncheck ./...
    trivy fs --scanners vuln .

The databases disagree at times, so all five run and the union counts. Every dependency must be open
source under an OSI-approved licence; syft lists the licences from the module cache after
`go mod download`:

    SYFT_GOLANG_SEARCH_LOCAL_MOD_CACHE_LICENSES=true syft dir:. -q -o json \
      | jq -r '.artifacts[] | [.type, .name, .version, ([.licenses[]?.value] | join(" | "))] | @tsv'

An empty licence column means syft could not name it: read the package's licence file. The commit
message records the scan result and the licences.

### Accepted findings: `.dependency-allowlist.json`

A finding that no version change can clear is recorded in `.dependency-allowlist.json` at the repo
root. The pre-commit dependency scan reads it, so an entry can arrive in the same commit as the
dependency it covers:

    {"licences":           {"<name>[@<version glob>]": "<SPDX licence, checked by hand>"},
     "licence_exceptions": {"<name>[@<version glob>]": "<why the owner accepted it>"},
     "vulnerabilities":    {"<GO-, GHSA- or CVE- id>": "<why the owner accepted it>"}}

A vulnerability is accepted only on the owner's word, and only with proof from govulncheck that no
dxlib code calls the vulnerable package: its report must say the code is affected by 0
vulnerabilities, with the finding listed under modules that are required but not called. The entry
says why, when it was accepted and when to look at it again. It is removed once a fixed release
exists.

Current entries:

- `GO-2026-5932` in `golang.org/x/crypto`, accepted 2026-10-05. The advisory declares
  `golang.org/x/crypto/openpgp` unmaintained and unsafe by design, and it covers every release of the
  module (introduced at v0, no fixed version), so no upgrade clears it. dxlib imports only `argon2`
  and `bcrypt` from the module; both lack a standard-library equivalent. Nothing imports openpgp, and
  govulncheck reports the package is not called. Review by 2027-04-05, or sooner if a fix appears.

A project that depends on dxlib inherits this finding through dxlib's `go.mod`. It can accept it the
same way, citing this entry, provided its own govulncheck run shows the same: openpgp not called.

Trivy has no reachability filter and does not read the allowlist, so an accepted finding is also
listed in `.trivyignore` with its review date as the expiry (`GO-2026-5932 exp:2027-04-05`). When the
entry expires Trivy reports the finding again, which is the prompt to review it.

## Compliance and Security

Every release's software bill of materials is committed in `sbom/<tag>/` at the root of this repository, in two
formats from the same syft run: CycloneDX JSON (`<name>-<version>.cdx.json`) and SPDX JSON
(`<name>-<version>.spdx.json`). Beside them are the report of each scanner that ran on the release's tree (grype
on the SBOM, osv-scanner on the source, govulncheck for each Go module, and the repository's own scanners when
it names any), `tools.json` with the scanners' versions and exit statuses, `allowlist-applied.json` with the
accepted findings that applied, and a README.md naming the tag, the commit scanned, the date and the result.
Both SBOMs are also attached to the GitHub release, uploaded while it is still a draft, and the release's notes
carry the scan summary; a release is published only once both files are on it.

The records go back to v1.128.0. Four releases carry no asset: v1.129.0, v1.130.0, v1.131.0 and v1.132.0 were
published by the release workflow within minutes of their tag push, GitHub makes a published release immutable,
and the upload that followed was refused, although their notes claimed the file (BUG-001 in
[BUG_HISTORY.md](BUG_HISTORY.md)). For those four, `sbom/<tag>/` is the SBOM; the records were made on
2026-10-28 from each tag's tree, and the scan found what the tree had then (`golang.org/x/net` 0.61.0 and
`go-jose/v4` 4.1.5, fixed in v1.132.1), so each README says `Result: findings`. The two files attached to
v1.132.1 were scanned on 2026-10-28 03:06 UTC and name one component the scan picked up outside the tree; the
committed `sbom/v1.132.1/` is a later scan of the same commit without it, and is the record to use.

To verify a release against its record: the commit in `sbom/<tag>/README.md` must be the tag's
(`git rev-parse <tag>^{commit}`), and where the release has the assets, `shasum -a 256` of the downloaded file
and of the committed one must agree. To run the scan again for a tag: `sbom-record <tag>` from the repository's
root scans the tag's tree, writes `sbom/<tag>/` and commits it. The scanners it runs are named in
`.agentq-release.json`; since the commit that added that file, trivy runs beside the four above, and its report
is `reports/scanner-0.txt`.

Accepted findings and licences, each with its reason, are in `.dependency-allowlist.json` (`vulnerabilities`,
`licences`, `licence_exceptions`); a scanner's own ignore file (`osv-scanner.toml`, `.grype.yaml`,
`.trivyignore`) holds its entries where one exists. Each `sbom/<tag>/` holds its own `osv-scanner.toml`,
because osv-scanner reads the committed SBOMs as inputs when it scans the whole repository (OpenSSF Scorecard
does so): the copy repeats the root's accepted finding and, for a record whose release had findings, names
those findings as that release's, so a scan of the current tree does not report a past release's dependencies
as the current ones. The record's own reports are untouched by it.
