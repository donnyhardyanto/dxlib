# SBOM of dxlib v1.133.0

- Tag: `v1.133.0`
- Commit scanned: `5b67eaead8a35a31e8f863aaca5f8a143e362484`
- Date: 2026-10-28 11:48 UTC
- Result: clean

SBOM: dxlib-1.133.0.cdx.json (CycloneDX, 182 components). Scan (syft 1.55.0, grype 0.121.0, osv-scanner 2.7.0, govulncheck v1.8.0, `trivy fs --scanners vuln --exit-code 1 .`): clean.

## Files
- `dxlib-1.133.0.cdx.json`: CycloneDX JSON, from syft
- `dxlib-1.133.0.spdx.json`: SPDX JSON, from the same syft run
- `reports/grype.json`: grype sbom:dxlib-1.133.0.cdx.json -o json
- `reports/osv-scanner.json`: osv-scanner scan source -r --allow-no-lockfiles --format json .
- `reports/osv-scanner.stderr.txt`: what osv-scanner wrote to stderr
- `reports/govulncheck-0.json`: govulncheck -format json ./... in .
- `reports/scanner-0.txt`: trivy fs --scanners vuln --exit-code 1 .
- `reports/scanner-0.stderr.txt`: what scanner-0 wrote to stderr
- `tools.json`: the tools and their versions, each step's command and exit status
- `allowlist-applied.json`: the .dependency-allowlist.json entries that applied, with their reasons
- `osv-scanner.toml`: osv-scanner's ignore entries for the SBOMs in this folder when the whole repository is scanned (the root's accepted finding); the reports above were made without it

## Left out of the scan

- `.build`
- `.worktrees`
- `references`
- `sbom` (the records of the earlier scans, #346)

## Run it again

`sbom-record v1.133.0` from the repository's root scans the tag's tree again and replaces this folder.
