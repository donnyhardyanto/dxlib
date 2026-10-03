# CLAUDE.md

Go library, module `github.com/donnyhardyanto/dxlib`. `LIBRARY.md` documents the packages. Consumers pin it
by tag through their own `go.mod` (dx-erp carries a nested copy), so a change here reaches them only after a
tag is cut and their pin moves.

## Checks

    go build ./... && go vet ./... && go test -race ./...

Bugs are tracked in GitHub issues only, one per bug, with `Fixes #N` in the fixing commit. There are no
`BUG_OUTSTANDING.md` or `BUG_HISTORY.md` files (in any spelling); do not create them. Ask before pushing;
never force-push.

Docker is not used. The library has no images or compose files; if containers are ever needed, they use
Podman (`podman`, `podman compose`).

## Dependencies: SBOM scan

Every dependency upgrade or new dependency gets an SBOM scan before it is committed. That covers `go.mod`
(the library itself), `examples/dxlibv3-dart/pubspec.yaml`, and the vendored JavaScript under `js/`.

Tools (all via Homebrew except govulncheck):

    brew install syft grype osv-scanner
    go install golang.org/x/vuln/cmd/govulncheck@latest

Run from the repo root after changing the dependency (`go get ...` and `go mod tidy`, or `fvm flutter pub get`
in the Dart example):

    syft dir:. -q -o cyclonedx-json=/tmp/dxlib-sbom.cdx.json
    grype sbom:/tmp/dxlib-sbom.cdx.json
    osv-scanner scan source -r .
    govulncheck ./...

The databases disagree at times (grype has flagged an `x/crypto` release that osv-scanner passed), so run all
of them and act on the union.

Rules:

- A new library is adopted only when the scan is clean: no known vulnerability in it or in anything it pulls
  in, at the version chosen. If it is not clean, choose a fixed version or a different library, or ask the
  owner.
- An upgrade must not add a finding. A finding that was already there and has no fix yet may stay, but name it.
- Put the result in the commit message: "SBOM scan clean (syft, grype, osv-scanner, govulncheck)", or what was
  found and how it was resolved.

## Dependencies: licence check

Every dependency must be open source. Check the licence of a new or upgraded dependency in the same pass as
the SBOM scan, for everything the scan covers (Go modules, the Dart example's packages, the vendored
JavaScript) and anything else vendored into the repo.

List licences with syft, which reads them from the Go module cache (run `go mod download` first):

    SYFT_GOLANG_SEARCH_LOCAL_MOD_CACHE_LICENSES=true syft dir:. -q -o json \
      | jq -r '.artifacts[] | [.type, .name, .version, ([.licenses[]?.value] | join(" | "))] | @tsv'

An empty licence column, or one holding only a `sha256:` hash, means syft could not name it: read the
package's `LICENSE` file. The Dart example has no `pubspec.lock`, so syft does not see its packages; check
them on pub.dev (`curl -s https://pub.dev/api/packages/<name>/score | jq '.tags'` shows `license:` tags).
The vendored JavaScript carries its licence in the file header or a `LICENSE` file next to it.

Rules:

- Only OSI-approved licences: MIT, BSD, Apache-2.0, ISC, MPL-2.0 and the like. MPL-2.0 is allowed: its
  copyleft is weak and per file, so it reaches only changes to the MPL files themselves.
- GPL, AGPL and LGPL: ask the owner before adding or moving to one, because of how this library is
  distributed.
- `golang/freetype` (used by `captcha`) is offered under the FreeType License or GPL-2.0+. It is used under
  the FreeType License, a BSD-style licence with a credit clause; the credit is in `NOTICE`. Keep it there
  while the module is a dependency.
- Never a source-available, "community", commercial or key-gated licence.
- Check again on every major version: a library can leave open source in one (PrimeVue 5 moved to a
  proprietary licence, so dx-erp stays on PrimeVue 4.x).
- Put the licence result next to the SBOM result in the commit message, for example "licences: all
  MIT/BSD/Apache-2.0".
