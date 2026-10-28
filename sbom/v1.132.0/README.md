# SBOM of dxlib v1.132.0

- Tag: `v1.132.0`
- Commit scanned: `15094f165b923c87f02fe8ac8f5d6cb685ba485b`
- Date: 2026-10-28 10:41 UTC
- Result: findings

SBOM: dxlib-1.132.0.cdx.json (CycloneDX, 128 components). Scan (syft 1.55.0, grype 0.121.0, osv-scanner 2.7.0, govulncheck v1.8.0): 3 findings not allowlisted: CVE-2026-116587/GHSA-mgcc-cww5-fp2q in github.com/go-jose/go-jose/v4 4.1.5 (go.mod, ./go.mod) (fixed in 4.1.6); CVE-2026-78661/GO-2026-6860 in golang.org/x/net 0.61.0 (go.mod, ./go.mod) (fixed in 0.62.0); CVE-2026-97045/GO-2026-6866 in golang.org/x/net 0.61.0 (go.mod, ./go.mod) (fixed in 0.62.0). Accepted: GO-2026-5932 in golang.org/x/crypto 0.58.0 (./go.mod) (.dependency-allowlist.json).

## Files
- `dxlib-1.132.0.cdx.json`: CycloneDX JSON, from syft
- `dxlib-1.132.0.spdx.json`: SPDX JSON, from the same syft run
- `reports/grype.json`: grype sbom:dxlib-1.132.0.cdx.json -o json
- `reports/osv-scanner.json`: osv-scanner scan source -r --allow-no-lockfiles --format json .
- `reports/osv-scanner.stderr.txt`: what osv-scanner wrote to stderr
- `reports/govulncheck-0.json`: govulncheck -format json ./... in .
- `tools.json`: the tools and their versions, each step's command and exit status
- `allowlist-applied.json`: the .dependency-allowlist.json entries that applied, with their reasons

## Left out of the scan

- `.build`
- `.worktrees`
- `references`
- `sbom` (the records of the earlier scans, #346)

## Run it again

`sbom-record v1.132.0` from the repository's root scans the tag's tree again and replaces this folder.
