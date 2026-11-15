# Fixed bugs

Entries move here from [BUG_OUTSTANDING.md](BUG_OUTSTANDING.md) when fixed, newest first.

## BUG-003: a failed DXRedis.Connect left a Redis client running for the life of the process

- **Reported**: 2026-11-15, from the review of the dxlib_module Redis connection change. Every failed
  `DXRedis.Connect` on a single node (no Sentinel) left a go-redis Ring behind; a caller that retries
  during a Redis or Valkey outage piled up one per attempt, each pinging the dead address every 500 ms.
- **Cause**: up to v1.133.0, `Connect` built `redis.NewRing` and returned on a failed `Ping` without
  closing it. Closing it (added in 9823d1e) frees the connection pool but not the Ring's heartbeat: in
  go-redis v8.11.5 `NewRing` starts `ringShards.Heartbeat`, which loops on its ticker with no way out, so
  even a closed Ring keeps one goroutine and its ticker. go-redis v9 stops the heartbeat in `Close`
  (`heartbeatCancelFn`), but moving to v9 changes the type of the exported `Connection` field and is a
  change for every consumer.
- **Fixed**: 2026-11-15, in the commit that says `Fixes BUG-003`. On a single node `Connect` first pings
  through a plain one-connection client with the same address, database, credentials and TLS settings,
  closes it, and builds the Ring only once that ping has answered; the Ring is still closed when its own
  first `Ping` fails. The Sentinel path closes its failover client on a failed `Ping`, which stops all of
  its goroutines. `TestFailedConnectLeavesNoGoroutineBehind` makes 20 failed Connects against a refused
  port on each path and checks the goroutine count does not grow; on the code before the fix it grew by
  20 on a single node. Still open, by go-redis v8's design: a Ring that did connect keeps its heartbeat
  goroutine after `Disconnect`, so a process that connects and disconnects repeatedly keeps one idle
  goroutine per cycle until dxlib moves to go-redis v9.

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
