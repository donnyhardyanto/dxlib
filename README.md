# dxlib

Newer version of dxlib.

Released under the MIT License; see [LICENSE](LICENSE). Third-party credits are in [NOTICE](NOTICE).

[]: # with the following snippet from README.md:
[]: # # dxlib

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
