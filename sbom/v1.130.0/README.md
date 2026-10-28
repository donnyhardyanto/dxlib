# SBOM of dxlib v1.130.0

- Tag: `v1.130.0`
- Commit scanned: `ff0ec306164badccc46a2b0fa5705d6fda85a5fc`
- Date: 2026-10-28 10:40 UTC
- Result: findings

SBOM: dxlib-1.130.0.cdx.json (CycloneDX, 128 components). Scan (syft 1.55.0, grype 0.121.0, osv-scanner 2.7.0, govulncheck v1.8.0): 3 findings not allowlisted: CVE-2026-116587/GHSA-mgcc-cww5-fp2q in github.com/go-jose/go-jose/v4 4.1.5 (go.mod, ./go.mod) (fixed in 4.1.6); CVE-2026-78661/GO-2026-6860 in golang.org/x/net 0.61.0 (go.mod, ./go.mod) (fixed in 0.62.0); CVE-2026-97045/GO-2026-6866 in golang.org/x/net 0.61.0 (go.mod, ./go.mod) (fixed in 0.62.0). Accepted: GO-2026-5932 in golang.org/x/crypto 0.58.0 (./go.mod) (.dependency-allowlist.json).

## Files
- `dxlib-1.130.0.cdx.json`: CycloneDX JSON, from syft
- `dxlib-1.130.0.spdx.json`: SPDX JSON, from the same syft run
- `reports/grype.json`: grype sbom:dxlib-1.130.0.cdx.json -o json
- `reports/osv-scanner.json`: osv-scanner scan source -r --allow-no-lockfiles --format json .
- `reports/osv-scanner.stderr.txt`: what osv-scanner wrote to stderr
- `reports/govulncheck-0.json`: govulncheck -format json ./... in .
- `tools.json`: the tools and their versions, each step's command and exit status
- `allowlist-applied.json`: the .dependency-allowlist.json entries that applied, with their reasons
- `osv-scanner.toml`: osv-scanner's ignore entries for the SBOMs in this folder when the whole repository is scanned (the root's accepted finding, and this release's own findings, named as such); the reports above were made without it

## Left out of the scan

- `.build`
- `.worktrees`
- `references`
- `sbom` (the records of the earlier scans, #346)

## Run it again

`sbom-record v1.130.0` from the repository's root scans the tag's tree again and replaces this folder.
