# Fixed bugs

Entries move here from [BUG_OUTSTANDING.md](BUG_OUTSTANDING.md) when fixed, newest first.

## BUG-001: releases claimed an SBOM they did not carry (v1.128.0 to v1.132.0)

- **Reported**: 2026-10-28, by a consumer who needs the SBOM for an audit. The releases v1.129.0 to
  v1.132.0 said in their notes "SBOM: dxlib-<version>.cdx.json (CycloneDX, 124 components)", but none
  had an asset, and the repository held no `sbom/<tag>/` records for any release from v1.128.0 on.
- **Cause**: `.github/workflows/release.yml` ran GoReleaser on every `v*` tag push and published the
  release at once (`release.draft` unset). GitHub makes a published release immutable, so the SBOM
  upload that followed was refused with HTTP 422 for each of v1.129.0 to v1.132.0; the scan summary
  still went into the notes, claiming the file. v1.128.0 was released before SBOMs were made at all.
  The `sbom/<tag>/` records were introduced on 2026-10-28, after these releases.
- **Fixed**: 2026-10-28. GoReleaser leaves a draft (`release.draft: true`, b2be0f4), published only once
  both SBOMs are on it; v1.132.1 was the first release made that way. `sbom/v1.128.0/` to `sbom/v1.132.0/`
  were scanned from their tags and committed, each with the findings its tree had then (fixed in v1.132.1)
  and an `osv-scanner.toml` naming them as that release's; `sbom/v1.132.1/` was scanned again without the
  component outside the tree that the first scan picked up. v1.128.0, not immutable, got both SBOMs
  attached (checksums equal to the committed files). v1.129.0 to v1.132.0 stay without an asset: an
  immutable release takes none, so their SBOMs are the records alone, and their notes are corrected to
  say so once the records are on `main`. The README's Compliance and Security section says where the
  SBOMs are and how to verify them, and `.agentq-release.json` adds trivy to the release scan.
