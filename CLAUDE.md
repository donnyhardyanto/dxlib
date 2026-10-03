# CLAUDE.md

Go library, module `github.com/donnyhardyanto/dxlib`. `LIBRARY.md` documents the packages. Consumers pin it
by tag through their own `go.mod` (dx-erp carries a nested copy), so a change here reaches them only after a
tag is cut and their pin moves.

## Checks

    go build ./... && go vet ./... && go test -race ./...

Bugs are tracked in GitHub issues only, one per bug, with `Fixes #N` in the fixing commit. There are no
`BUG_OUTSTANDING.md` or `BUG_HISTORY.md` files (in any spelling); do not create them. Ask before pushing;
never force-push.

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
