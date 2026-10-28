# SBOM of dxlib v1.132.1

- Tag: `v1.132.1`
- Commit scanned: `905eafb272b479c2bc464c2e951d6033f75a3e30`
- Date: 2026-10-28 10:41 UTC
- Result: clean

SBOM: dxlib-1.132.1.cdx.json (CycloneDX, 128 components). Scan (syft 1.55.0, grype 0.121.0, osv-scanner 2.7.0, govulncheck v1.8.0): clean. Accepted: GO-2026-5932 in golang.org/x/crypto 0.58.0 (./go.mod) (.dependency-allowlist.json).

## Files
- `dxlib-1.132.1.cdx.json`: CycloneDX JSON, from syft
- `dxlib-1.132.1.spdx.json`: SPDX JSON, from the same syft run
- `reports/grype.json`: grype sbom:dxlib-1.132.1.cdx.json -o json
- `reports/osv-scanner.json`: osv-scanner scan source -r --allow-no-lockfiles --format json .
- `reports/osv-scanner.stderr.txt`: what osv-scanner wrote to stderr
- `reports/govulncheck-0.json`: govulncheck -format json ./... in .
- `tools.json`: the tools and their versions, each step's command and exit status
- `allowlist-applied.json`: the .dependency-allowlist.json entries that applied, with their reasons
- `osv-scanner.toml`: osv-scanner's ignore entries for the SBOMs in this folder when the whole repository is scanned (the root's accepted finding); the reports above were made without it

## Left out of the scan

- `.build`
- `.worktrees`
- `references`
- `sbom` (the records of the earlier scans, #346)

## Run it again

`sbom-record v1.132.1` from the repository's root scans the tag's tree again and replaces this folder.
