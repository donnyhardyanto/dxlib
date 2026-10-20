# CLAUDE.md

Go library, module `github.com/donnyhardyanto/dxlib`; `LIBRARY.md` documents the packages. The repository is
public. Consumers pin it by tag in their own `go.mod` (dx-erp carries a nested copy), so a change reaches them
only after a tag is cut and their pin moves.

The umbrella `MyOwnApps/CLAUDE.md` and `~/.claude/CLAUDE.md` apply on top of this file: pushing (ask first,
never force-push), Podman over Docker, the public-repo no-AI-traces rule, licence policy and the accepted
GO-2026-5932 advisory. Only what is specific to dxlib is written here.

## Checks

    go build ./... && go vet ./... && go test -race ./...

## Bugs

- Tracked in `BUG_OUTSTANDING.md` and `BUG_HISTORY.md` at the repo root (public repo, GitHub issues disabled),
  one `BUG-NNN` entry per bug, moved to the history file when fixed, `Fixes BUG-NNN` in the fixing commit.

## Containers

- None: no images or compose files. If ever needed, Podman (`podman`, `podman compose`), never Docker.

## Dependencies

Scope: `go.mod`, `examples/dxlibv3-dart/pubspec.yaml`, the vendored JavaScript under `js/`, and anything else
vendored. Every new or upgraded dependency gets the SBOM scan and the licence check in one pass, before commit.

Tools (Homebrew, except govulncheck):

    brew install syft grype osv-scanner trivy
    go install golang.org/x/vuln/cmd/govulncheck@latest

### SBOM scan

Run from the repo root after the change (`go get ...` and `go mod tidy`, or `fvm flutter pub get` in the Dart
example):

    syft dir:. -q -o cyclonedx-json=/tmp/dxlib-sbom.cdx.json
    grype sbom:/tmp/dxlib-sbom.cdx.json
    osv-scanner scan source -r .
    govulncheck ./...
    trivy fs --scanners vuln .

- Run all of them and act on the union: the databases disagree (grype has flagged an `x/crypto` release that
  osv-scanner passed).
- New library: adopted only when clean, it and everything it pulls in, at the chosen version. Otherwise take a
  fixed version or another library, or ask the owner.
- Upgrade: must not add a finding. An existing finding with no fix yet may stay; name it.
- Accepted finding: entry in `.dependency-allowlist.json` plus a line in `.trivyignore` (id and `exp:` review
  date), since Trivy has no reachability filter. Never `--ignore-unfixed`: it hides every unfixed finding.
- Commit message: "SBOM scan clean (syft, grype, osv-scanner, govulncheck, trivy)", or what was found and how it
  was resolved.

### Licence check

List licences (run `go mod download` first; syft reads them from the module cache):

    SYFT_GOLANG_SEARCH_LOCAL_MOD_CACHE_LICENSES=true syft dir:. -q -o json \
      | jq -r '.artifacts[] | [.type, .name, .version, ([.licenses[]?.value] | join(" | "))] | @tsv'

- Empty column, or only a `sha256:` hash: read the package's `LICENSE` file.
- Dart example: no `pubspec.lock`, so syft misses it; check pub.dev
  (`curl -s https://pub.dev/api/packages/<name>/score | jq '.tags'` shows `license:` tags).
- Vendored JavaScript: licence in the file header or a `LICENSE` file beside it.
- Allowed: OSI-approved only (MIT, BSD, Apache-2.0, ISC, MPL-2.0 and the like; MPL-2.0's copyleft is per file).
- GPL, AGPL, LGPL: ask the owner first, because of how this library is distributed.
- Never source-available, "community", commercial or key-gated. Check again on every major version.
- `golang/freetype` (used by `captcha`) is FreeType License or GPL-2.0+; dxlib uses it under the FreeType
  License, whose credit is in `NOTICE`. Keep that credit while the module is a dependency.
- Commit message: the licence result next to the SBOM result, e.g. "licences: all MIT/BSD/Apache-2.0".
