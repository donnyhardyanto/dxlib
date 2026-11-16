# SBOM of dxlib v1.135.0

- Tag: `v1.135.0`
- Commit scanned: `da237fc06ca052ded949a3214e56cb8d896f2bab`
- Date: 2026-11-16 05:26 UTC
- Result: clean

SBOM: dxlib-1.135.0.cdx.json (CycloneDX, 182 components). Scan (syft 1.57.1, grype 0.121.3, osv-scanner 2.7.1, govulncheck v1.9.0, `trivy fs --scanners vuln --exit-code 1 .`): clean.

## Files
- `dxlib-1.135.0.cdx.json`: CycloneDX JSON, from syft
- `dxlib-1.135.0.spdx.json`: SPDX JSON, from the same syft run
- `reports/grype.json`: grype sbom:dxlib-1.135.0.cdx.json -o json
- `reports/osv-scanner.json`: osv-scanner scan source -r --allow-no-lockfiles --no-call-analysis=go --format json .
- `reports/osv-scanner.stderr.txt`: what osv-scanner wrote to stderr
- `reports/govulncheck-0.json`: govulncheck -format json ./... in .
- `reports/scanner-0.txt`: trivy fs --scanners vuln --exit-code 1 .
- `reports/scanner-0.stderr.txt`: what scanner-0 wrote to stderr
- `tools.json`: the tools and their versions, each step's command and exit status
- `allowlist-applied.json`: the .dependency-allowlist.json entries that applied, with their reasons
- `osv-scanner.toml`: osv-scanner's ignores for this folder: the root's entries and the findings of v1.135.0 recorded here, so a whole-tree osv-scanner run does not count this release's findings as the current tree's

## Left out of the scan

- `.build`
- `.worktrees`
- `references`
- `sbom` (the records of the earlier scans)

## Reused from the scan cache

- govulncheck-0 reused (key 262c231fe9d3, made 09:34 for dep-scan of dxlib at ef94466)

## Run it again

`sbom-record v1.135.0` from the repository's root scans the tag's tree again and replaces this folder (the same day, a step whose inputs, tools and database are unchanged is taken from the scan cache; `AGENTQ_SCAN_CACHE=off` runs every step).
