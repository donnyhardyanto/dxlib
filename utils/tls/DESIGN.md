# mTLS for the dxlib `api` package and its HTTP/WebSocket clients

## Summary

Inter-service traffic inside the bank's cluster has to run over mutually
authenticated TLS. dxlib had no TLS code at all: `api.DXAPI` called
`ListenAndServe`, and the three outbound call sites (`utils/http/client`,
`DXAPIEndPointRequest.HTTPClientDo`, and the WebSocket client) built bare
clients. What has been added, in one sentence: a `tls` block beside `address` in
each API's configuration and a `tls` block in a new `http-client` configuration,
both read by one new package, `utils/tls`, which builds the `crypto/tls`
configuration for the listener and for every outbound client from an explicit
transport mode, a named handshake policy and an explicitly named trust source,
subtracts from that policy whatever a deny-list names, reloads certificates and
the deny-list from disk without a restart, refuses callers by identity and by
revocation at the handshake, and reports the whole posture in the log and in a
preflight an operator can run on an air-gapped host.

Nothing changes for a service that does not write either block. With no `tls`
block the listener is plaintext and the clients are the bare defaults, byte for
byte as before; the only new line is a log line saying so.

The one rule everything else follows from: **every trust decision is named in
configuration, and nothing is defaulted into place.** `mode` and `tls-policy`
are required on the server, `ca-trust` and `tls-policy` on the client, and
`ca-trust` on the server whenever the mode verifies clients; each has a short
list of legal values. A missing, empty or misspelled value stops the process
from starting. There is no value meaning "no trust source", no key that turns
verification off on the server, and no way to widen a policy from
configuration. Configuration can only ever *remove* from the policy, and it can
do so two ways: by restating the list it wants, or -- for the day an advisory
lands -- by naming what has to go.

The server side, in configuration:

```json
"api": {
  "api": {
    "address": "0.0.0.0:58081",
    "tls": {
      "mode":        "mtls",
      "cert-file":   "/etc/dcc/tls/tls.crt",
      "key-file":    "/etc/dcc/tls/tls.key",
      "tls-policy":  "intermediate",
      "ca-trust":    "custom",
      "ca-files":    ["/etc/dcc/tls/ca.crt"],
      "deny-file":   "/etc/dcc/tls/deny.json",
      "allowed-client-sans": ["spiffe://cluster.local/ns/dcc/sa/queue-scheduler"],
      "allowed-client-sans-log-only": true
    }
  },
  "oam": { "address": "0.0.0.0:48081", "tls": { "mode": "http" } }
}
```

The client side:

```json
"http-client": {
  "tls": {
    "cert-file":  "/etc/dcc/tls/tls.crt",
    "key-file":   "/etc/dcc/tls/tls.key",
    "tls-policy": "intermediate",
    "ca-trust":   "custom",
    "ca-files":   ["/etc/dcc/tls/ca.crt"],
    "deny-file":  "/etc/dcc/tls/deny.json"
  }
}
```

And the deny-file both point at, empty between incidents:

```json
{}
```

The rest of this document is the detail, from the trust model down to the
files.

## 1. Trust: an explicit, named trust source on every hop

### 1.1 The trap this removes

`crypto/tls` treats a nil `ClientCAs` on a server, and a nil `RootCAs` on a
client, as "verify against the system root store". On a server that means any
certificate a public CA would issue is a valid caller identity, and it is
reached by leaving one field out. A first draft of this design had two boolean
knobs, `ca-use-system-roots` and `ca-files`, with system roots defaulting off
for the inbound side and on for the outbound side. Two different silent
defaults for the same-looking key, on either side of a hop, is exactly the trap:
a configuration review cannot tell what is trusted without knowing which side it
is reading and what the code does with an absent key.

So there is one key, `ca-trust`, required on the client and required on the
server whenever the server verifies clients (`mode: mtls`), with three values
and no default:

| value | trusts |
|---|---|
| `custom` | only the CA certificates in `ca-files` |
| `system` | only the host's root store |
| `system-and-custom` | both |

The resulting `*x509.CertPool` is always constructed and always assigned --
including for `system`, where `x509.SystemCertPool()` is called and its result
installed, and including under a migration rung where Go may never consult it.
Under `mode: https`, where there is no named pool because no client is verified,
`ClientCAs` is set to an *empty* pool rather than left nil: if anything ever
raised `ClientAuth` on that config, an empty pool fails closed where a nil would
have admitted every public CA. No code path exists from which the nil fallback
is reachable; what the operator named is what is installed, and where nothing
was named, nothing is trusted. A reviewer reads the trust source for every hop
off the configuration.

There is deliberately no fourth value meaning "no trust source". That absence is
what replaces a separate "refuse to start with no trust" check: the key is
required wherever a trust decision is made, its values all name a source, so a
process with no trust source cannot be described in configuration at all.

### 1.2 Choosing the value

For the inbound side of a bank service, `custom`. One internal CA issues every
service certificate, and a publicly issued certificate must not be a valid
service identity: anyone who can obtain a certificate for a name they control
would otherwise be a caller. `system` and `system-and-custom` exist for the
outbound side, where public upstreams are a fact of life. Two further reasons
`system` is the wrong inbound choice, from the air-gapped section below: on a
distroless image the store is empty and startup is refused, and on an
air-gapped host the store validates nothing the host will ever talk to.

### 1.3 Contradictions are refused, not ignored

`ca-files` given with `ca-trust: system` is an error naming both keys, not a
silently dropped list. `ca-trust: custom` or `system-and-custom` with no
`ca-files` is an error. `cert-file` without `key-file`, or the reverse, is an
error on either side. A `ca-files` entry with no `CERTIFICATE` block in it is an
error. Any TLS-bearing key under `mode: http` is an error naming the key;
`ca-trust`, `ca-files`, `allowed-client-sans` or `client-auth-migration` under
`mode: https` is an error naming the key; the retired `client-auth` key is an
error naming its replacement. An `allowed-client-sans` list under the `request`
migration rung, which never verifies, is an error, because a SAN in an
unverified certificate is a claim, not an identity. `allowed-client-sans-log-only`
without a list is an error. A deny token outside the vocabulary is an error.

One deliberate exception, stated with its reason in section 3.9: a correctly
spelled deny that removes nothing is not an error.

### 1.4 The empty system store

Go reports an empty root store as success: on Linux, `loadSystemRoots` returns a
pool with no certificates and a nil error when none of the well-known files or
directories exist, which is the state of a distroless or scratch image. Nothing
would be wrong until the first handshake, which would fail with "unknown
authority" that looks like the peer's fault.

The check is `pool.Equal(x509.NewCertPool())`. `Equal` compares the certificate
set and the `systemPool` flag. On Linux the store is loaded from files and the
flag is off, so an empty store is `Equal` to an empty pool. On macOS and
Windows the store is the platform verifier, the flag is on (`root.go` returns
`&CertPool{systemPool: true}`), and `Equal` is false regardless -- the check is
inert there, which is correct: Go cannot enumerate a platform store and the
distroless failure mode does not exist on those platforms. The deprecated
`Subjects()` is not used; it would also have been empty for a platform pool.

Verified empirically, not from the source alone: `go test` in a
`golang:1.27-alpine` container with `SSL_CERT_FILE=/nonexistent
SSL_CERT_DIR=/nonexistent` refuses startup with
`ca-trust:SYSTEM_ROOTS_EMPTY:THE_HOST_HAS_NO_ROOT_STORE(DISTROLESS_IMAGE?):USE_ca-trust=custom_OR_ADD_ca-certificates`,
and the same suite with the normal store passes.

The system store is checked for emptiness on its own, before anything from
`ca-files` is appended. Under `system-and-custom` a merged pool is never empty,
so a post-merge check would let a distroless image quietly degrade to
custom-only -- the emergent behaviour the explicit key exists to remove.

### 1.5 What happens when a required key is missing, empty or garbage

All three refuse to start. No fallback to a default, to system roots, to
plaintext, or to client-auth off. A service that boots with an unresolved trust
source and then makes accept/reject decisions is strictly worse than one that
never boots: under Kubernetes the pod goes to CrashLoopBackOff with the reason in
its log, the rollout stalls, and the previous healthy pods keep serving. That is
the right blast radius for a misconfigured trust store.

Three mistakes, three messages, each listing the legal values (with three of
them, the list is the whole "did you mean"):

```
TLS_CONFIG_ERROR:api.api.tls/ca-trust:REQUIRED_KEY_MISSING:VALID_VALUES=custom|system|system-and-custom
TLS_CONFIG_ERROR:api.api.tls/ca-trust:EMPTY_VALUE:VALID_VALUES=custom|system|system-and-custom
TLS_CONFIG_ERROR:api.api.tls/ca-trust:INVALID_VALUE:"ABSC":VALID_VALUES=custom|system|system-and-custom
```

The offending value is `%q`-quoted and cut at 64 characters, so a long or
newline-bearing value cannot split or forge a log line. `mode`, `tls-policy`,
`client-auth-migration` and (when present) `min-version` get the same
treatment, from the same reader.

The error reaches the log through `log.Log.FatalAndCreateErrorf` in
`ApplyConfigurations`, the same way `CONFIGURATION_NOT_FOUND:%s.%s/address`
already does, so the process exits the way it always has on a fatal
configuration error. This is tested in a child process
(`TestApplyConfigurationsRefusesToStartOnABadTrustSource`): the parent
re-executes the test binary, asserts exit status 1 and the exact message.

Values are trimmed and matched case-insensitively, and the canonical form is
what gets logged. `" System "` and `"System"` resolve to `system`; `"ABSC"`
still fails. Explicitness is about the trust source being named in
configuration, not about capitalisation: a hand-edited dev file refusing to
boot over a capital letter is friction with no security in it, and a trailing
space inside a JSON string is close to invisible when it does. This is a
decision, not looseness.

A hazard of an existing helper, found while writing this: `utils/json.GetString`
formats `"can not get %s as %T from %v"` with the entire enclosing map on
failure. A configuration map is where key material would sit if a future key
ever carried any. No `GetString` error text is allowed into a `tls` block's
error; `utils/tls` reads every key with its own readers, whose messages name
only the key path, the value's type, and (for enumerations) the capped, quoted
value. `TestConfigErrorsNeverEchoTheMap` pins this.

### 1.6 A present block is always validated

A `tls` block with `"enabled": false` is parsed and validated in full -- every
key, every enumeration, every contradiction -- and then not put in force.
Otherwise a typo behind `enabled: false` sits unnoticed until the day someone
flips it to `true` in production. What is skipped for a disabled block is only
what needs the files: loading them, building the pool, reading the deny-file, so
a dev host without the production certificate paths still starts. This holds
under `mode: mtls` as much as anywhere: `TestModeIsExplicitAndRefusesContradictions`
puts a bad value into each key of a disabled mtls block and asserts each is
refused. A wholly absent block skips everything and logs `API <name>: TLS
disabled (no tls block); listening in plaintext at <address>`, so "no TLS" is a
line in the log rather than the absence of one.

### 1.7 A consequence that used to be here

An earlier shape of this design had `ca-trust` required under every server
block, including one whose client-auth mode was `none`. The consequence, which
this section used to describe, was that a plain-TLS listener that authenticated
nobody could still refuse to start on an empty system store, because it was
made to name a pool Go would never read. That was the price of one rule with no
exceptions, and it was awkward.

The explicit `mode` key removes it. Under `mode: https` no client is verified,
so `ca-trust` and `ca-files` name a pool with no purpose, and they are refused
rather than required. There is nothing left to name and nothing left to refuse
to start on. The one rule is intact -- every trust decision is named -- because
under `https` there is no client-trust decision to make.

What replaces the old consequence is smaller and worth knowing: a listener
that wants TLS with *optional* client certificates is not `https`. It is
`mode: mtls` with `client-auth-migration: verify-if-given`, which names the pool
the optional certificates are checked against and is warn-logged as a migration
state, because "optional client certificates" is where an mTLS rollout passes
through, not where it stops.

### 1.8 The transport is one explicit word

Before `mode`, the transport was inferred: plaintext from an absent block, TLS
from `client-auth: none`, mTLS from `client-auth: require-and-verify`. A
reviewer had to know what the code did with a combination of keys to read the
transport off the configuration, which is the trap section 1.1 removed for the
trust source, reappearing one key over. So the server block has a required
`mode` with three values, read by the same reader as `ca-trust` -- same
missing/empty/invalid messages, same trim and case-insensitivity, same
canonical form in the log:

| `mode` | means | requires | refuses |
|---|---|---|---|
| `http` | no TLS; plaintext, said out loud | nothing | every TLS-bearing key in the block, by name |
| `https` | TLS with a server certificate; no client is authenticated (`ClientAuth: NoClientCert`) | `cert-file`/`key-file`, `tls-policy` | `ca-trust`, `ca-files`, `allowed-client-sans`, `client-auth-migration` |
| `mtls` | TLS with client certificates required and verified (`RequireAndVerifyClientCert`) | `cert-file`/`key-file`, `tls-policy`, `ca-trust` (and `ca-files` where the trust mode needs them) | -- |

A wholly absent `tls` block still means plaintext, exactly as before, with its
one log line. `mode: http` is for saying so in the file: an `oam` listener beside
an `mtls` `api` reads better with the word in it than with a gap.

`mode` subsumes `client-auth`, which is gone as a key. A block that still
carries it is refused with `client-auth:RETIRED_KEY:USE:mode=http|https|mtls_AND_OPTIONALLY:client-auth-migration=request|verify-if-given`,
rather than the key being ignored as unknown: under `mode: https` a leftover
`client-auth: require-and-verify` would otherwise sit in the file saying
something the listener does not do.

The rollout ladder's two intermediate states survive as an optional
`client-auth-migration` key, legal only under `mode: mtls`, with values
`request` (crypto/tls `RequestClientCert`: a certificate is asked for; whether
one arrives, and whether it is valid, changes nothing) and `verify-if-given`
(`VerifyClientCertIfGiven`: a certificate that arrives must be valid; a caller
that sends none is still admitted). Absent means full `require-and-verify`. This
is the one key in the whole design that loosens rather than narrows, and it is
treated accordingly: it is named as a migration state; it is refused under
`http` and `https`, where there is nothing to migrate towards; and on every
single start it warn-logs

```
TLS server: client-auth-migration=request -- this listener is NOT yet enforcing mTLS; effective client-auth=request (a certificate is asked for; whether one arrives, and whether it is valid, changes nothing). Remove the key when every caller presents a valid certificate.
```

naming the mode the listener is actually in, so it cannot be forgotten. The
summary line and the preflight say `NOT ENFORCING MTLS` beside it for the same
reason. Go's `RequireAnyClientCert` is deliberately not offered under any name:
it demands a certificate and verifies none of it, so the identity it yields is
whatever the peer typed.

## 2. Configuration reference

### 2.1 Server: `api.<name>.tls`

| key | required | values / default | notes |
|---|---|---|---|
| `mode` | yes | `http` \| `https` \| `mtls` | no default; section 1.8 |
| `enabled` | no | boolean, default `true` | `false` validates and does not enforce |
| `cert-file`, `key-file` | `https`, `mtls` | PEM paths | the leaf (and any intermediates) and its key; hot-reloaded |
| `tls-policy` | `https`, `mtls` | `modern` \| `intermediate` | see section 3 |
| `min-version` | no | `"1.2"` \| `"1.3"` (number accepted) | may only raise the policy floor |
| `cipher-suites` | no | IANA names | subset of the policy's TLS 1.2 list; refused under 1.3-only |
| `curves` | no | `X25519MLKEM768` \| `X25519` \| `P-256` \| `P-384` | subset of the allow-list |
| `min-rsa-bits` | no | number, default 2048 | may only be raised |
| `min-ecdsa-bits` | no | number, default 256 | may only be raised |
| `deny-cipher-suites` | no | family tokens or IANA names | subtracted after the above; section 3.9 |
| `deny-curves` | no | curve names, `P-521` included | subtracted after the above |
| `deny-certificates` | no | list of `{spki-sha256}` or `{issuer, serial}` objects | refused in `VerifyConnection`, whole chain; section 3.11 |
| `deny-certificate-signature-algorithms` | no | `x509.SignatureAlgorithm` names | refused in `VerifyConnection`, whole chain minus the anchor |
| `deny-file` | no | path to a JSON file carrying the four `deny-*` keys | hot-reloaded; section 3.10 |
| `client-auth-migration` | no, `mtls` only | `request` \| `verify-if-given` | absent = enforcing; warn-logged on every start |
| `ca-trust` | `mtls` | `custom` \| `system` \| `system-and-custom` | no default; refused under `https` |
| `ca-files` | with `custom`/`system-and-custom` | list of PEM paths, each may be a bundle | refused with `system` and under `https`; hot-reloaded |
| `allowed-client-sans` | no, `mtls` only | list of DNS names, URIs (SPIFFE), IPs | exact match; refused under the `request` rung |
| `allowed-client-sans-log-only` | no | boolean, default `false` | observe instead of enforce |

Under `mode: http` every key in this table except `mode` and `enabled` is
refused.

### 2.2 Client: `http-client.tls`

| key | required | values / default | notes |
|---|---|---|---|
| `enabled` | no | boolean, default `true` | |
| `cert-file`, `key-file` | no (as a pair) | PEM paths | absent: the client presents no certificate |
| `tls-policy`, `min-version`, `cipher-suites`, `curves`, `min-rsa-bits`, `min-ecdsa-bits` | as above | as above | |
| `deny-cipher-suites`, `deny-curves`, `deny-certificates`, `deny-certificate-signature-algorithms`, `deny-file` | as above | as above | certificate denies hot-reload; suite and curve denies from the file need a restart on this side (section 5) |
| `ca-trust`, `ca-files` | yes | as above | `RootCAs` is never nil |
| `server-name` | no | hostname | the SAN to verify when the URL host is not one the certificate carries |
| `insecure-skip-verify` | no | boolean, default `false` | client side only; warn-logged at every start; for a dev host; the deny list still applies to what was presented |
| `preflight-dial` | no | `host:port` | the one peer the preflight may dial when run over the OAM route |

`server-name` is the escape hatch that is not `insecure-skip-verify`: an
in-cluster call to `https://svc:8443` has a host the certificate may not carry,
and naming the SAN it does carry keeps verification on.

### 2.3 Where the blocks are read

`api.DXAPI.ApplyConfigurations` reads the server block from the per-API object
beside `address`, so the existing consumer pattern
`NewIfNotExistConfiguration("api", ..., utils.JSON{"api": {"address": ...}, "oam": {...}})`
gains a `tls` key per API; `oam` can stay plaintext (absent block, or
`mode: http`) while `api` is mTLS. `app.DXApp.loadConfiguration` looks for an
`http-client` configuration and calls
`utils/http/client.LoadFromConfiguration("http-client")` before loading `api`,
so an outbound misconfiguration is reported before any port is bound.

### 2.4 Coming from nginx

A platform engineer who has run nginx will look for these. What each maps to,
and where the mapping is not one-to-one, why:

### 3.9.1 OpenSSL and nginx spelling

A cipher policy does not arrive in IANA notation. It arrives as an nginx
`ssl_ciphers` line, a haproxy `ssl-default-bind-ciphers` line, or the output of
`openssl ciphers -v`, and all three speak OpenSSL names --
`ECDHE-RSA-AES256-GCM-SHA384`, not `TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384`.
Translating by hand is friction in the one situation where a mistyped suite name
is either a service that will not start or a service that quietly fails to
protect. So the deny keys accept both spellings, and the startup log always
reports the IANA name so there is no doubt what was understood.
`TestEveryGoSuiteHasAnOpenSSLName` asserts the table covers everything
`crypto/tls` reports, so a Go upgrade that adds a suite fails the build rather
than leaving a name that silently resolves to nothing.

The grammar gets the opposite answer, deliberately. An OpenSSL cipher string is
a small language: `!` excludes permanently, `-` excludes but lets a later rule
add the suite back, `+` reorders rather than adds, `@STRENGTH` sorts, and the
aliases compose. Almost nobody remembers those rules correctly, which is much of
why Mozilla publishes a configuration generator. Accepting half a dialect is
worse than accepting none: an operator pastes `HIGH:!aNULL:!MD5:@STRENGTH`, the
part we understood is honoured, and they believe a policy is in force that is
not. A token carrying `:`, `+`, `@`, `,`, `;` or a leading `-` is refused, with
a message naming what to do instead.

One exception. A bare leading `!` is stripped and the redundancy logged: an
nginx-trained operator writes it by reflex, and in a list that only ever
subtracts, `"!3DES"` can only mean deny 3DES. The tolerance stops there.

Aliases fall into three groups, and the split is the point:

| group | treatment | why |
|---|---|---|
| exact equivalents -- `kRSA` (RSA key exchange), `SHA` (the SHA-1 HMAC suites) | mapped to the family token | the sets are identical |
| never implemented -- `aNULL`, `eNULL`, `EXPORT`, `LOW`, `DES`, `MD5`, `DSS`, `ADH`, `DHE`, `PSK`, `SRP`, `IDEA`, `SEED`, `CAMELLIA`, `ARIA`, `KRB5`, `GOST` | recognised no-op, logged at info | every real nginx policy carries `!aNULL:!eNULL:!EXPORT:!MD5`; refusing blocks a correct transcription, and swallowing hides that the exclusion did nothing. The info line is also the compliance answer -- not offered, so nothing to exclude |
| overlapping but not equal -- `HIGH`, `MEDIUM`, `aRSA`, `AESGCM`, `AESCBC`, `ECDH` | refused, with the reason | `aRSA` is RSA *authentication*, so it covers ECDHE-RSA too; `HIGH`/`MEDIUM` are strength buckets whose membership has moved across OpenSSL releases. Denying a different set than the operator intended is the failure this whole section exists to avoid |

A token in none of those groups is still a startup error, unchanged. That is the
rule the forgiving layer must not weaken: `CHACHA2O` with a letter O has to fail
loudly, or it is an exclusion that protects nothing.

`deny-curves` takes the OpenSSL curve spelling too, since nginx writes
`ssl_ecdh_curve X25519:prime256v1`: `prime256v1` and `secp256r1` resolve to
`P-256`, `secp384r1` to `P-384`, `secp521r1` to `P-521`. A colon-joined list is
refused there for the same reason.

| nginx | here | notes |
|---|---|---|
| `ssl_protocols TLSv1.2 TLSv1.3;` | `tls-policy` (`intermediate` = 1.2+1.3, `modern` = 1.3) and `min-version` | the policy sets both ends; `min-version` may only raise the floor |
| `ssl_ciphers 'ECDHE+AESGCM:...';` (the positive list) | `cipher-suites` | intersected with the profile; a name outside it is refused, not offered |
| `ssl_ciphers '...:!3DES:!RC4:!aNULL';` (the `!` exclusions) | `deny-cipher-suites` | family tokens (`CBC`, `3DES`, `RC4`, `CHACHA20`, `RSA-KEY-TRANSPORT`, `AES-128`, `AES-256`, `SHA1`) or IANA names; no regexes; section 3.9 |
| `ssl_ecdh_curve X25519:prime256v1;` | `curves` / `deny-curves` | `curves` restates, `deny-curves` subtracts |
| `ssl_prefer_server_ciphers on;` | none, by design | `tls.Config.PreferServerCipherSuites` is documented "Deprecated: ignored"; Go selects the best mutually supported suite itself, from hardware and security, and offers no knob |
| `ssl_certificate` / `ssl_certificate_key` | `cert-file` / `key-file` | hot-reloaded on mtime without a reload signal |
| `ssl_client_certificate` + `ssl_verify_client on;` | `mode: mtls` + `ca-trust`/`ca-files` | `optional` is `client-auth-migration: verify-if-given`; `optional_no_ca` has no equivalent, deliberately |
| `ssl_verify_client off;` | `mode: https` | `ca-trust` is refused there, since no client is verified |
| `ssl_crl /path/to/crl.pem;` | `deny-certificates` in the `deny-file` | by SPKI or issuer+serial; air-gap safe (no fetch); whole chain; section 3.11 |
| `ssl_stapling on;` (fetches OCSP) | none | no network in the handshake path, section 7.1; a locally refreshed staple file is a follow-up |
| `nginx -s reload` | nothing to run | the deny-file, the certificate and the CA files are stat-checked on every handshake; a push takes effect on the next one |
| `ssl-default-bind-ciphers` (haproxy, TLS 1.2 and below) | `cipher-suites` / `deny-cipher-suites` | same OpenSSL grammar caveat; token by token |
| `ssl-default-bind-ciphersuites` (haproxy, TLS 1.3) | none, by design | Go hardcodes the three TLS 1.3 suites; that haproxy needs a separate directive is the same fact section 3.4 states |
| `ssl-default-bind-options no-sslv3 no-tlsv10` | `tls-policy` / `min-version` | |
| `verify required` + `ca-file` (haproxy) | `mode: mtls` + `ca-trust`/`ca-files` | |
| `verify optional` (haproxy) | `client-auth-migration: verify-if-given` | |
| `crt-list` (haproxy) | `cert-file` | hot-reloaded on mtime; no `reload` needed |
| `ssl_conf_command SignatureAlgorithms ...` | none, cannot exist | `tls.Config` has no such field; see section 3.12 |
| FIPS builds of OpenSSL | `GODEBUG=fips140=on` | Go's native FIPS 140-3 mode, section 3.12 |

## 3. Handshake policy: an allow-list, with subtraction behind it

### 3.1 Why an allow-list

A deny-list of broken parameters can only ever name what has already been
published; it is permanently one disclosure behind. A named profile that lists
the small set this library intends to speak is complete by construction --
everything unnamed is refused, including whatever gets broken next year. So the
parameters a handshake offers come from the profile, and configuration can only
remove from it.

Removal has two shapes. The narrowing overrides (`cipher-suites`, `curves`,
`min-version`) restate the list the operator wants and are intersected with the
profile. The deny keys (section 3.9) name what has to go from whatever that
left. Both only remove. Go's own `tls.InsecureCipherSuites()` sits behind both
as a third layer with one job: refusing an operator who writes something
already known to be broken into a narrowing override, usually for a checklist.
That list is the standard library's, not one kept in this repository, so it
moves as Go demotes suites without anyone here having to notice. On Go 1.27 it
holds RC4, 3DES, every `TLS_RSA_WITH_*` key-transport suite (demoted in 1.22),
and the ECDHE CBC-SHA256 suites. A name from it is refused as
`INSECURE_SUITE:...:ON_GO_INSECURE_LIST`; a name Go still considers fine but the
profile does not is refused as `NOT_IN_POLICY:...:OVERRIDES_MAY_ONLY_NARROW`.
The two messages differ so the operator knows whether the thing is broken or
merely disallowed.

### 3.2 The profiles

`tls-policy` is required, for the same reason `ca-trust` is: it is a security
decision, and a defaulted security decision is one a review cannot see.

**`modern`** -- TLS 1.3 only. This is reachable for in-cluster service-to-service
traffic in a way it never is for a public endpoint, because both ends of every
connection are ours. That is the payoff of doing this inside the mesh.

**`intermediate`** -- TLS 1.2 and 1.3, with the 1.2 handshake restricted to ECDHE
key exchange and AEAD ciphers:

```
TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256   TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256
TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384   TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384
TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256   TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256
```

What that excludes, by the attack that motivates the exclusion, since this is
the part an auditor reads:

- every CBC suite: Lucky13 and the padding-oracle family (BEAST; POODLE's TLS
  variant). Go 1.27 still lists four ECDHE+CBC-SHA1 suites as secure; they are
  not in the profile.
- every RSA key-transport suite (`TLS_RSA_WITH_*`): no forward secrecy, and
  ROBOT.
- 3DES: Sweet32. RC4: the NOMORE biases.
- static and anonymous DH: `crypto/tls` never implemented them, so there is
  nothing to exclude, but the question will be asked.

### 3.3 Overrides may only narrow

`min-version` may raise the floor (`intermediate` + `"1.3"` is 1.3-only) and
never lower it (`modern` + `"1.2"` is `WIDENS_POLICY`). `cipher-suites` and
`curves` are intersected with the profile; a name outside it is an error naming
the entry; an empty list is an error. Under a 1.3-only result a `cipher-suites`
list is refused as `NO_EFFECT_UNDER_TLS_1.3`, because it would apply to nothing.
`min-rsa-bits` and `min-ecdsa-bits` may be raised and not lowered below 2048 and
256. Without these rules "hardening configuration" would be the mechanism by
which the stack gets un-hardened.

### 3.4 What `CipherSuites` does not do

Nothing, under TLS 1.3. Go hardcodes the three 1.3 suites (AES-128-GCM,
AES-256-GCM, ChaCha20-Poly1305) and exposes no knob; `tls.Config.CipherSuites`
is ignored for 1.3 handshakes. Any statement that "weak ciphers were
blacklisted" is vacuous for 1.3 traffic. Compliance paperwork routinely asks
for a 1.3 cipher list; the honest answer is that the protocol and the standard
library already constrain it, and there is deliberately no key here that would
silently do nothing. The same holds for a deny: `deny-cipher-suites: ["CHACHA20"]`
removes the two 1.2 ChaCha suites and leaves `TLS_CHACHA20_POLY1305_SHA256`
exactly where Go put it, and the startup line says so (section 3.9).

### 3.5 Curves

`CurvePreferences` is set to `X25519MLKEM768, X25519, P-256, P-384`. The
post-quantum hybrid leads: Go 1.24+ offers it by default when `CurvePreferences`
is nil, and setting the field at all would have dropped it. Keeping it is what
protects today's recordings from a harvest-now-decrypt-later adversary, so it is
named rather than left implicit. P-521 is not broken but not needed; P-224 does
not exist in `crypto/tls`.

### 3.6 Certificate-strength floor

Chain validation says nothing about key strength: a 1024-bit RSA leaf that chains
correctly to the internal CA verifies fine, and Go's own floor (`checkKeySize`
in the handshake) is 1024. The policy applies RSA >= 2048 and ECDSA >= 256 (Ed25519
passes; any other key type is refused) at three points: our own certificate at
load and on every reload (a weak rotation is refused and the previous
certificate stays in service), every CA certificate in `ca-files` at load, and
every certificate in the peer's verified chain inside `VerifyConnection`.
`TestKeyStrengthFloorRefusesAValidWeakCertificate` proves the last with a real
handshake from a 1024-bit client that chains correctly, and the same from the
client side against a 1024-bit server.

A leaf carrying `BasicConstraints CA:TRUE` chains fine as a leaf and is refused
in `VerifyConnection` on both sides, closing the door on a mis-issued
intermediate being used as an identity.

Go already requires `ExtKeyUsageClientAuth` on a verified client certificate --
`handshake_server.go` sets `KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}`
at lines 522 and 975 in the Go 1.27 source -- so that is not duplicated.
`TestCACertificateAsLeafAndMissingClientEKUAreRefused` confirms Go's check with
a serverAuth-only client certificate rather than assuming it.

### 3.7 Hardened by configuration versus structurally absent

An auditor's checklist asks for every one of these. Presenting the absent ones
as things this library hardened would be theatre; the point of this list is to
stop someone adding a knob that does nothing to satisfy a checkbox. Each was
checked against the Go 1.27 `crypto/tls` and `crypto/x509` source in use here,
not taken from a summary.

| item | status | where |
|---|---|---|
| TLS 1.0 / 1.1 | refused by configuration (policy floor is 1.2 or 1.3) | `policy.go`; Go's own default is also 1.2 since 1.22 (`common.go` `supportedVersions`: `MinVersion == 0 && v < VersionTLS12` skips), but the explicit value is what the log reports |
| SSLv2 / SSLv3 | not implemented; `VersionSSL30` exists only as a constant for error text | `common.go` |
| export suites, static/anonymous DH | not implemented | `cipher_suites.go` |
| CBC, RSA key transport, 3DES, RC4 | refused by the profile; RSA/3DES/RC4 also on Go's insecure list; also deniable by family token, as a no-op against this profile | `policy.go`, `deny.go` |
| SHA-1 handshake signatures (`PKCS1WithSHA1`, `ECDSAWithSHA1`) | structurally absent: removed in TLS 1.3 by the protocol, and removed from TLS 1.2 in Go 1.25 behind `GODEBUG=tlssha1=1`; nothing to deny | `auth_test.go` carries the `tlssha1=1` cases |
| SHA-1 and MD5 *certificate* signatures on a verified chain | refused by `crypto/x509` during chain verification; the `x509sha1` GODEBUG that re-enabled SHA-1 was removed in Go 1.24 (`godebugs/table.go:102`, `Removed: 24`). `SHA1-RSA` is a recognised no-op in `deny-certificate-signature-algorithms`, not a typo | `x509.go` (`SHA1WithRSA // Only supported for signing, and verification of CRLs, CSRs, and OCSP responses`) |
| handshake signature algorithms, configurable | no knob exists; section 3.12 | `common.go:467,527` |
| server cipher preference (`ssl_prefer_server_ciphers`) | no knob exists; `PreferServerCipherSuites` is "Deprecated: ignored" | `common.go:745` |
| TLS compression (CRIME) | not implemented; only `compressionNone` is offered or accepted | `handshake_client.go:73`, `handshake_server.go:223` |
| renegotiation (triple handshake) | `Renegotiation` zero value is `RenegotiateNever`; the server side never supports it | `common.go:563` |
| server-side 0-RTT early data (replay) | not implemented for TLS over TCP; `earlyData` exists only on the client hello path for QUIC | `handshake_client.go:286,482` |
| post-quantum key exchange | on by default and kept | section 3.5 |
| FIPS 140-3 | available natively, not built here; section 3.12 | `defaults_fips140.go`, `common.go:1795` |

### 3.8 The effective policy is logged

At every start, one line per side:

```
TLS server: mode=mtls cert=... key=... subject="api.test" not-after=2027-09-03T06:35:10Z client-auth=require-and-verify ca-trust=custom ca-certs=1 ca-files=[/etc/dcc/tls/ca.crt] allowed-client-sans=1 entries (log-only) deny-file=/etc/dcc/tls/deny.json deny-in-force=[cipher-suites=CHACHA20] tls-policy=intermediate versions=1.2..1.3 suites-1.2=[TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,...] curves=[X25519MLKEM768,X25519,P-256,P-384] min-rsa-bits=2048 min-ecdsa-bits=256
```

`suites-1.2` and `curves` are what is offered after every override and every
deny, so the line is the enforced posture and not the configured intent. When a
deny is in force a second line precedes it saying what the deny removed and
which of its tokens removed nothing:

```
TLS server deny: [cipher-suites=CHACHA20,CBC] removed-suites=[TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256,TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256] no-op=[deny-cipher-suites=CBC] -- matched nothing in the effective set; not an error, so a fleet-wide deny does not fail closed on a service that was already safe
```

An operator and an auditor read the enforced posture out of the log without
inferring it from what is missing. A compliance-pinned deployment should set
`min-version` explicitly rather than rely on the profile's floor, so the pin is
visible in configuration as well as in the log.

### 3.9 Subtractive deny-lists

The question that produced this section, in the operator's words: in nginx you
exclude a broken cipher with `ssl_ciphers '...:!3DES:!RC4'`; how is that done
here. Until this section, the only answer was "restate the whole `cipher-suites`
list without it", and that is the wrong ergonomics for the day it is needed. An
advisory lands, it says "all CBC modes", and the operator has to expand that to
six suite names, retype the survivors correctly under pressure, push it
fleet-wide, and hope nothing was mistyped into a service outage.

`deny-cipher-suites` and `deny-curves`, in both blocks, name what goes. They are
applied **after** the profile resolves and **after** any narrowing override, so
they subtract from what the operator actually ended up with. A deny can only
ever remove: it cannot put back anything the profile or an override took out,
so it cannot widen a policy, and the narrow-only rule of section 3.3 is
untouched. That is the whole reason it is safe to add. The allow-list still
bounds what can ever be offered; the deny only moves inside that bound.

**Family tokens.** The vocabulary is closed and named: `CBC`, `3DES`, `RC4`,
`CHACHA20`, `RSA-KEY-TRANSPORT`, `AES-128`, `AES-256`, `SHA1`, matched over the
IANA name Go reports for each suite (`_CBC_`, `3DES`, `RC4`, `CHACHA20`, the
`TLS_RSA_WITH_` prefix, `AES_128`, `AES_256`, and a bare `_SHA` suffix, which in
IANA naming is the SHA-1 HMAC). An exact IANA name is accepted beside them.
Tokens are trimmed and case-insensitive, and logged in canonical form. Regular
expressions are refused, with a message saying so: a pattern over cipher names
is a footgun -- `AES` matches every suite in the profile -- and a closed
vocabulary is also what makes the next two rules possible, because only a closed
vocabulary can tell a typo from a no-op.

**A correctly spelled deny that matches nothing is a no-op, logged at info, not
an error.** This is a deliberate exception to the refuse-contradictions rule of
section 1.3, and the reason has to be written down because the rule and the
exception look contradictory until it is: an incident deny-list is pushed
fleet-wide. Some services never offered the thing being denied -- every
`modern` listener, for a start, offers no 1.2 suite at all -- and they were
already safe. Failing closed on them would take healthy pods down in the middle
of a breach, which is the one moment that must not happen. So `CBC` against
this profile removes nothing, and the startup line says
`no-op=[deny-cipher-suites=CBC]` and why. Under a 1.3-only policy the line adds
that Go fixes the 1.3 suites, so it is not read as the deny having worked.

**An unrecognised token is a startup error listing the valid tokens.** `CHACHA2O`
with a letter O, `AES.*`, `DES`: each is refused with
`INVALID_VALUE:...:VALID_VALUES=CBC|3DES|RC4|CHACHA20|RSA-KEY-TRANSPORT|AES-128|AES-256|SHA1|<an IANA suite name ...>`.
A typo must never silently fail to protect. The two rules together: spelled
right and removing nothing is fine; spelled wrong is never fine.

**Denying down to an empty set is a startup error**
(`EMPTY_AFTER_DENY:...:YOU_CANNOT_DENY_YOUR_WAY_TO_NO_CIPHERS`). The check is
against a base that had something in it: under `modern` the 1.2 list is already
empty by design and every suite deny is a no-op there, not an error. Curves
have the same rule and no exemption, since every policy has curves.

`deny-curves` takes the four allow-list names plus `P-521`, so that an advisory
naming P-521 resolves to a recognised no-op rather than a typo.
`TestDenyListsSubtractFromTheResolvedPolicy` covers each rule at the
configuration level; `TestDenyListsAreEnforcedAtTheHandshake` has a server
denying `CHACHA20` refuse a real TLS 1.2 client that offers only ChaCha (server
alert, server log line) while admitting an AES client and a 1.3 client, and a
client denying the two X25519 curves fail against a server narrowed to X25519
and succeed against one that still offers P-256.

### 3.10 The deny-file, and what hot reload buys

`deny-file`, in both blocks, is a path to a JSON object carrying any of the
four `deny-*` keys and nothing else -- a key outside the four is refused,
because a misspelled key would be a file that denies nothing while looking as
though it did. `{}` is the steady state between incidents. It is read at
startup (an unreadable, malformed or policy-emptying file is a startup error
naming the path) and then watched by `DXDenyFileReloader`, the same
stat-per-handshake reloader in `reload.go` that watches the certificate and CA
files. The block's own `deny-*` keys and the file's are merged; a union of
denies is still only a deny.

On the server this is what makes incident response a file push.
`GetConfigForClient` already returns a fresh config per handshake; when the
deny-file's stamp moves it hands back a clone with the suite and curve lists
re-derived from the narrowed base minus the new list, and `VerifyConnection`
reads the new certificate denies from an atomic pointer. Go swaps to the
returned config before it chooses the version, suite and curve
(`handshake_server.go:174`, `c.config = configForClient`, ahead of
`mutualVersion` at line 191), so a suite the file just took out is not offered
to the very handshake that noticed the change. Nothing restarts; nothing is
signalled. Where nginx needs `nginx -s reload` and Kubernetes a rolling restart
of every pod, here the deny takes effect on the next handshake of every process
that can see the file. `TestDenyFileHotReloadsOnTheServer` pushes a `CHACHA20`
deny to a running listener and has the next ChaCha handshake refused, and
asserts the clone kept HTTP/2.

A malformed push keeps the previous list in force and warns once, exactly as a
broken certificate rotation does: `TLS_DENY_FILE_RELOAD_FAILED:<why>:KEEPING_CURRENT_DENY_LIST:[<what is still in force>]`.
That covers unparseable JSON, a token outside the vocabulary, an unknown key, a
malformed certificate entry, and a list that would leave the policy with
nothing to offer. The warning goes through `log.Log.Warn`, which this library
forwards to Telegram, so a typo in a pushed file is loud even though it does
not take the service down; and the preflight (section 7.3) reads the same file
and reports the same error, so a push pipeline can gate on it before the push.
The same test pushes each of those six bad files in turn and asserts the
previous deny stays in force through all of them.

**The asymmetry on the client side, stated plainly.** The client has no
`GetConfigForClient`. Its `tls.Config` sits on an `http.Transport` that clones
`TLSClientConfig` per dial (`transport.go:1783`, `cloneTLSConfig`), and
crypto/tls offers no client-side hook that could swap `CipherSuites` or
`CurvePreferences` between dials. So a deny-file change to *suites or curves*
takes effect on the outbound side at the next restart, and the reload logs
`TLS_DENY_FILE_CLIENT_RESTART_NEEDED` saying exactly that. The certificate-level
denies in the same file do not have this limitation: they are enforced in
`VerifyConnection`, which crypto/tls calls on the client in every handshake
(`handshake_client.go:588,1198`), and that hook re-reads the file. So a pushed
key or certificate deny is in force on both sides without a restart; only the
algorithm half of the file is restart-bound on the client, which is the less
urgent half. `TestDenyFileCertificateDeniesHotReloadOnTheClient` proves both:
the pushed server-key deny refuses the next dial, and a pushed suite deny is
read and reported but leaves the offered suites unchanged. The follow-up, if
the algorithm half ever needs to be hot on the client, is the same
`DialTLSContext` that section 5 names for the root pool.

### 3.11 Certificate-level denies: the more important half

"Because it was breached" is the phrase this exists for, and a breach is
usually a key or a CA, not an algorithm. Algorithm breaks arrive with years of
warning and a literature; a compromised intermediate is same-day. So the
deny-file also carries `deny-certificates`, a list of entries in one of two
forms:

```json
"deny-certificates": [
  {"spki-sha256": "b662cbb0...d33b",                          "reason": "key compromised 2026-09-01"},
  {"issuer": "CN=dxlib test intermediate", "serial": "0x1a2b3c", "reason": "mis-issued"}
]
```

**`spki-sha256`** is the SHA-256 of the certificate's `RawSubjectPublicKeyInfo`,
as 64 hex characters (colons between bytes allowed, as openssl prints them) or
as 44 characters of standard base64 (as a pin is usually quoted); the two are
told apart by length. It identifies a *key*, and so survives re-issuance of the
same key under a new serial -- which is precisely what the holder of a
compromised key does next, and precisely what a deny of the old serial would
miss. This is the form to reach for.

**`issuer` + `serial`** identifies one certificate. The issuer is matched
against `Issuer.String()` exactly as the preflight report and the rejection log
print it (`CN=...,O=...`, RFC 2253 form); openssl's `CN = X, O = Y` spacing will
not match, and the report is the place to copy from. The serial is accepted as
decimal (what this package's own log and preflight print, alongside the hex),
as `0x`-prefixed hex, as `aa:bb:cc` hex as openssl prints it, or as bare hex
when it carries a letter. A bare string of digits is decimal; openssl's
prefixless hex of a serial that happens to hold only digits has to be written
with `0x`. Both forms are printed wherever a certificate is described, so the
value never has to be computed by hand on an air-gapped host.

An entry with neither form, both, or a key outside `spki-sha256`, `issuer`,
`serial`, `reason` is refused. `reason` is free text, echoed in the rejection
log, so the line an operator sees six months later says why.

**Enforcement is over every certificate in every verified chain, on both
sides**, not only the leaf. A denied intermediate must reject every leaf under
it, or denying the intermediate would mean nothing; and it is checked across
every chain Go verified rather than the first, so a cross-signed path around
the denied certificate does not change the answer. The check runs first in
`VerifyConnection`, ahead of the key-strength floor and the SAN allow-list, so
the log says `REVOKED` for a revoked certificate even when it would also have
failed a later check. Under `insecure-skip-verify` on the client, the one
check left is applied to every certificate the peer presented.
`TestDenyCertificatesRevokeAtTheHandshake` has the victim refused by SPKI in
all four spellings and by issuer-serial in all four, the bystander admitted, the
same key re-issued under a new serial refused by the SPKI deny and admitted by
the issuer-serial deny, a denied intermediate take both leaves under it while a
root-issued leaf passes, and the client refuse a server by its leaf, by its
intermediate, and under `insecure-skip-verify`.

The rejection is a new handshake class, **`REVOKED`**, distinct from `TRUST` on
purpose: the chain is valid, and a certificate in it is on the deny list. The
fix is not a CA file. The server logs
`TLS_CLIENT_REJECTED:REVOKED:TLS_PEER_REVOKED:<identity>:chain[<n>]="<subject>":<entry>`,
`http.Server.ErrorLog` classifies the same line as `REVOKED`, and the client's
own refusal of a server classifies the same way. The advice attached says the
refusal is deliberate and to check the deny list before the peer.

**This is the Chrome CRLSet / Firefox OneCRL pattern**, and it is here because
it is the **only revocation mechanism that works air-gapped**. Section 7.1
establishes the production constraint: no OCSP or CRL fetch is permissible in
the handshake path, because on an air-gapped host every handshake would block
until the fetch timed out. A browser vendor solves the same problem by shipping
a small, curated list of revoked keys and certificates with the software and
updating it out of band; that is what the deny-file is, with the update being a
file push and the "software" being every process that can see the file. It is
not a full CRL: it names what the operator chose to name, and a certificate the
PKI revoked that nobody wrote into the file is still trusted. That is the same
limitation CRLSet has, and the same trade: a list that is always available and
always current to the last push, against one that is complete and cannot be
fetched.

**`deny-certificate-signature-algorithms`** is the algorithm half at the
certificate level: `x509.SignatureAlgorithm` names (`SHA256-RSA`,
`SHA256-RSAPSS`, `ECDSA-SHA384`, `Ed25519`, `ML-DSA-65`, ...), refused over the
verified chain in `VerifyConnection`, **exempting the trust anchor** at the end
of it. Nobody verifies a trust anchor's own signature -- it is trusted by
identity -- and a bank's internal root self-signed years ago with an algorithm
since retired is not a weakness in any handshake that chains to it; refusing
every service in the cluster over it would be a fail-closed with nothing
behind it. Under `insecure-skip-verify` a self-signed certificate is exempt the
same way. Two things to know before writing the key: `SHA1-RSA`, `ECDSA-SHA1`
and `MD5-RSA` are recognised but are no-ops on a verified chain, because
`crypto/x509` already refuses them during chain verification and has since the
`x509sha1` GODEBUG was removed in Go 1.24; and the algorithms the key can
meaningfully refuse are therefore the ones Go still accepts -- PKCS#1 v1.5 RSA
where a policy demands PSS, a hash size, or ML-DSA. `TestDenyCertificateSignatureAlgorithms`
uses a leaf signed `ECDSA-SHA384` under an `ECDSA-SHA256` intermediate under an
`ECDSA-SHA512` root: denying the leaf's algorithm refuses that leaf and not its
sibling; denying the intermediate's refuses both leaves (the whole chain is
checked); denying the root's refuses nothing (the anchor is exempt); and the
client applies the same rule to a server's chain.

### 3.12 What cannot be denied by configuration, and why

Three things an nginx operator will look for do not exist here, and a key that
pretended otherwise would do nothing.

**Handshake signature algorithms.** There is no signature-algorithm knob on
`tls.Config`. The two `SignatureSchemes` fields in the Go 1.27 source
(`crypto/tls/common.go:467` on `ClientHelloInfo`, `:527` on
`CertificateRequestInfo`) are read-only views of what the *peer* advertised,
for a `GetCertificate` or `GetClientCertificate` callback to pick a certificate
by. Nothing on the config restricts what this side offers or accepts. So the
handshake's signature algorithms cannot be denied from configuration, and only
*certificate* signature algorithms can, in `VerifyConnection` (section 3.11).
This is less of a gap than it looks: SHA-1 handshake signatures are already
gone (section 3.7), and what remains -- RSA-PSS, ECDSA, Ed25519 over SHA-256
and up -- is the set a policy would allow anyway.

**Server cipher preference.** `PreferServerCipherSuites` is a documented
no-op: "a legacy field and has no effect ... Deprecated: PreferServerCipherSuites
is ignored" (`common.go:745`). Go selects the best mutually supported suite
itself, from inferred client hardware, server hardware and security. There is
no `ssl_prefer_server_ciphers` equivalent, by design, and the order of the
profile's list is the order Go breaks ties in, not a preference the operator
sets.

**FIPS 140-3.** Go has a native FIPS 140-3 mode (`GODEBUG=fips140=on`, or
`=only`); under it `fips140tls.Required()` restricts the signature algorithms
to `allowedSignatureAlgorithmsFIPS` (`common.go:1795`, `defaults_fips140.go`)
and the suites and curves likewise. It is the lever for a bank compliance
requirement that names FIPS, it is a build and runtime setting rather than a
configuration key, and nothing was built for it here. It composes with this
package: the profile is already inside the FIPS set except for ChaCha20 and
X25519, which FIPS mode drops on its own.

## 4. Identity, not just chain validity

### 4.1 Chain validation is not authorization

Inside one cluster a single internal CA issues every service certificate, so
"chains to our CA" is true of every service. A server that stops there has
authenticated the caller as "something in the cluster" -- not nothing, but not
an identity, and not a basis for letting the queue scheduler call the admin API.
The identity is in the certificate's subject alternative names.

`allowed-client-sans` is a list of DNS names, URI SANs (SPIFFE IDs, when the PKI
issues them) and IP SANs. Matching is exact -- no wildcards, no suffix rules;
DNS names compare case-insensitively, IPs as parsed addresses, URIs as strings.
The common name is not consulted; an entry has to name a SAN. It is enforced in
`tls.Config.VerifyConnection`, which Go calls inside the handshake after chain
verification and before any byte of HTTP is read. A caller that is not on the
list gets a `bad certificate` alert and no request object is ever built: not
`PreProcessRequest`, not the E2EE unpack, not a middleware runs for it.
`TestAllowedClientSANsRejectAtHandshake` asserts the handler's hit counter stays
at zero.

### 4.2 Log-only mode, and the ladder

Nobody can safely switch an allow-list on in a bank production without first
knowing who would be refused. `allowed-client-sans-log-only: true` admits every
verified caller and logs
`TLS_CLIENT_WOULD_BE_REJECTED:NOT_IN_ALLOWED_CLIENT_SANS:<identity>:sans=[...]:LOG_ONLY_MODE_ADMITTED`
for the ones the list would have refused. The staged rollout, each rung
observable before the next enforces anything:

1. `mode: mtls` with `client-auth-migration: request` -- nothing is refused; a
   certificate that arrives is logged (through the audit entry and metrics) so
   you learn which callers are ready. Zero enforcement risk. The listener
   warn-logs on every start that it is not enforcing.
2. `client-auth-migration: verify-if-given` -- a presented certificate must now
   be valid; absence is still allowed. Callers with a wrong CA surface here.
   Still warn-logged on every start.
3. Remove `client-auth-migration` -- every caller needs a valid certificate.
   The warning stops.
4. `allowed-client-sans` with `allowed-client-sans-log-only: true` -- the
   would-have-been-refused list is read for as long as it takes.
5. `allowed-client-sans-log-only: false` -- enforcing.

Every rung answers a question you would otherwise be guessing at. The ladder
exists because guessing, in a bank, means an outage or a hole. The two rungs
that loosen are the only thing in this design that does, which is why they
carry a name that says so and a warning that repeats.

### 4.3 The peer on the request

`DXAPIEndPointRequest` gains `PeerCertificate *x509.Certificate` and
`PeerIdentity string`, populated in `NewEndPointRequest`. They are gated on
`r.TLS.VerifiedChains`, **not** `r.TLS.PeerCertificates[0]`: under the
`request` rung Go fills `PeerCertificates` with whatever the client sent and
verifies none of it, and treating that as an identity would let a caller choose
its own. `TestModesAndRungsAtTheHandshake` has the discriminating case -- the
`request` rung, a certificate from the wrong CA, 200 with no verified chain --
and its companion: even a *good* certificate under `request` is unverified, so
`PeerCertificate` stays nil in that rung by design.

`PeerIdentity` reduces the certificate to one string: first URI SAN, else first
DNS SAN, else first IP SAN, else `CN=<name>`, else the serial. It is what
`OnBeforePreProcessRequest`, middlewares and handlers can make per-caller
decisions on; a refusal there happens after the handshake, so the allow-list is
still the place for anything that should never reach a handler.

### 4.4 The audit log, and the gap in `GetIPAddress`

`DXAPIAuditLogEntry` gains `PeerIdentity`, filled at `OnAuditLogStart` and
`OnAuditLogUserIdentified`. The comment above `GetIPAddress` states its
limitation plainly: `X-Proxy-Client-IP` is trustworthy only for traffic that
arrived through api-proxy-v4; a service reachable by some other ingress can be
sent a forged one, as it could a forged `X-Forwarded-For`. Under direct mTLS
between services there is no proxy setting any header, so `IPAddress` in the
audit entry is whatever the caller wrote, and `PeerIdentity` is the field that
was actually authenticated. That is the answer to the gap the comment flags.

## 5. Hot reload

cert-manager and a bank PKI issue short-lived leaves, and Kubernetes rewrites a
mounted Secret in place (an atomic swap of the `..data` symlink). A process that
parsed its certificate once at startup serves an expired one until somebody
restarts the pod. And an incident deny-list that needed a rolling restart to
take effect would be a deny-list that takes effect after the incident.

`DXCertificateReloader` sits behind `tls.Config.GetCertificate` (server leaf)
and `GetClientCertificate` (client leaf); `DXCAPoolReloader` sits behind
`GetConfigForClient` (server CA pool); `DXDenyFileReloader` sits behind
`GetConfigForClient` on the server and `VerifyConnection` on both sides. On
every handshake they `stat` the files and re-read only when the modification
time or size has moved. The hot path is a stat, not a read: a few microseconds
against a handshake that costs milliseconds of asymmetric crypto, so there is no
throttle -- one less thing to get wrong. `os.Stat` follows the symlink, so the
Kubernetes swap shows as a new mtime on the target. mtime and size are compared
together, because a filesystem with coarse timestamps can rewrite within one
tick.

A failed reload keeps the previous state. A half-written file, a key that does
not match its certificate, a rotated leaf that fails the strength floor, or a
deny-file with a typo in it logs a warning once per distinct problem (not once
per handshake) and the process carries on with what it had. Every successful
swap logs `TLS_CERTIFICATE_RELOADED`, `TLS_CA_POOL_RELOADED` or
`TLS_DENY_FILE_RELOADED` with what is now in force.

Three findings from doing it, each of which changed the code:

**`GetConfigForClient` and HTTP/2 on Go 1.27.** The natural design returns a
clone of the config with the new pool. On this toolchain `http.Server.ServeTLS`
no longer mutates `TLSConfig.NextProtos` in place -- `setupTLSConfig` puts `h2`
and `http/1.1` on a private clone -- so a clone made from `TLSConfig` has no
ALPN list, and every connection after the first CA rotation would silently drop
to HTTP/1.1. The first version of `TestClientCAPoolHotReloadKeepsHTTP2` failed
exactly this way. `DXServerTLS.ConfigForHTTPServer()` pins `NextProtos` to
`["h2", "http/1.1"]` before the config is handed to `http.Server`, so the clone
matches the config in use, and both that test and the deny-file test assert
`ProtoMajor == 2` after a reload. On no change `GetConfigForClient` returns
nil, which tells Go to keep the config already in use.

**Two reloaders behind one hook.** With the deny-file joining the CA pool
behind `GetConfigForClient`, the clone's `ClientCAs` is always taken from the
pool reloader's *current* pool, never from the original config: a deny-file
change after a CA rotation must not put the retired pool back.

**The client's root pool does not hot-reload, and neither do its suite and
curve denies.** `crypto/tls` has no per-handshake hook on the client side that
could swap `RootCAs`, `CipherSuites` or `CurvePreferences`; `http.Transport`
clones `TLSClientConfig` per dial. A CA rotation on the outbound side, or a
deny-file change to suites or curves, needs a restart. CA certificates live for
years and are rotated by adding the successor to the bundle well before the
predecessor expires, so a restart at the next deploy is the normal cadence; the
limitation is stated in `NewClientTLS` and logged at the reload so nobody has
to discover it. The client's certificate-level denies are the exception, since
`VerifyConnection` is a per-handshake hook the client does have; section 3.10.
The follow-up, if either is ever needed hot, is a `DialTLSContext` on the
transport that builds a config per dial, with `ForceAttemptHTTP2` kept on.

The system store, when a mode includes it, is loaded once. `crypto/x509` itself
loads it once per process and hands out copies, so a change to the host store
needs a restart regardless.

## 6. HTTP/2 and WebSocket

Enabling TLS on `http.Server` turns HTTP/2 on. gorilla's upgrader does not
speak h2, so this had to be tested rather than reasoned about.
`TestDXAPIWebSocketOverMTLS` starts a real `DXAPI` through `StartAndWait` with
a `NewWSEndPoint` echo, dials it with the library's own WebSocket client over
mTLS, and round-trips a message carrying the peer identity;
`TestDXAPIServesMTLSAndExposesThePeer` on the same kind of listener asserts a
plain request negotiated `HTTP/2`. Both pass: gorilla does its own handshake on
the TCP connection and never offers ALPN, so the upgrade goes through the
HTTP/1.1 server path while ordinary requests use h2. Browsers behave the same
way for `wss://` -- Go's h2 server does not advertise
`SETTINGS_ENABLE_CONNECT_PROTOCOL`, so a browser falls back to an HTTP/1.1
upgrade. `TLSNextProto` is therefore left alone; there was nothing to fix.

## 7. Air-gapped production

Production has no internet, no openssl on the image, and no test suite. This
changes the design, not only the test plan.

### 7.1 No network in the handshake path

`crypto/tls` fetches nothing during verification: no OCSP, no CRL download, no
AIA chasing. That is a property to preserve. Nothing added here opens a
connection during a handshake, and nothing should: on an air-gapped host every
handshake would block until the fetch timed out. If bank policy demands
revocation checking, the air-gap-safe form is the one built in section 3.11:
`deny-certificates` in a locally pushed deny-file, checked against every
certificate in the verified chain in `VerifyConnection`, with no fetch anywhere
-- the CRLSet pattern. OCSP stapling on the server (`tls.Certificate.OCSPStaple`,
filled from a file the PKI refreshes locally) is the other air-gap-safe form
and is a follow-up. Someone will ask for live OCSP. The answer is no, for this
reason.

### 7.2 Clock skew

A host without NTP fails certificate validity checks with "certificate has
expired or is not yet valid", which looks nothing like a clock problem. Three
things address it. The preflight prints the current time beside every
certificate's validity window and says "if the certificate was just issued,
this host's clock is behind -- check NTP before the PKI". Both sides classify
handshake failures: `ClassifyHandshakeError` sorts the typed `x509` errors (and,
for the side that only received an alert, the text) into `VALIDITY_WINDOW`,
`TRUST`, `NAME`, `PEER_REJECTED_US`, `NO_CLIENT_CERT`, `POLICY`, `IDENTITY`,
`REVOKED`, `KEY_STRENGTH`, `TRANSPORT`, each with advice. On the server, refused
handshakes are only ever reported through `http.Server.ErrorLog`; with TLS on,
that is now a writer that classifies each line into the dxlib log as
`TLS_HANDSHAKE_REJECTED:<class>:...`. Plaintext listeners keep a nil `ErrorLog`,
as before.

Two things about that writer to know. This library forwards WARN and ERROR to
Telegram, and one misconfigured caller retrying every second, or a port
scanner, would otherwise produce a page per handshake. `TRANSPORT` failures
(a health checker opening and closing a socket, plaintext on a TLS port) go to
debug outright. Every other class is logged at warn on its first occurrence and
then at most once per class per `HandshakeWarnInterval` (one minute), with the
count of what was suppressed in between; the suppressed lines still go to
debug. And because `ErrorLog` is now set on a TLS listener, its non-handshake
lines (`http: Accept error`, `http: panic serving`) reach `log.Log.Error` --
and so Telegram -- where they used to go to stderr. That is a TLS-only change;
it is also arguably where they should always have gone. `TestValidityWindowIsDistinguishableFromTrust` generates a
not-yet-valid leaf and a wrong-CA leaf and asserts they land in different
classes on both sides. On macOS the platform verifier's wording ("certificate
is not trusted") arrives as plain text with no type; it is matched by text.

### 7.3 The preflight

`utilsTLS.PreflightServer(name, block)` and `PreflightClient(block, dialAddr)`
read the configured certificate, key, CA and deny files and nothing else. For
each certificate: subject, issuer and serial (the last in both decimal and hex,
spelled as a `deny-certificates` entry has to spell them), `spki-sha256`,
signature algorithm, SANs, key type and size, `NotBefore`/`NotAfter` beside
`now`, days remaining (a problem under 30). They verify that the configured
leaf chains to the configured `ca-trust` pool -- the classic wrong-CA-mounted
mistake, caught at deploy time instead of at the first refused caller. They
print the resolved posture: mode, client-auth and any migration rung (marked
`NOT ENFORCING MTLS`), policy, versions, the 1.2 suite list, curves, floors,
allow-list and its mode. They read the deny-file, report what it holds and what
the policy offers once it is subtracted (with the no-op report), or the reason
it could not be read as a problem, and then check our own certificate, its
intermediates and every CA certificate against the merged deny list: a service
whose own key or CA is on the list will refuse or be refused by everyone, and
that is worth knowing before the first handshake. Under `mode: http` the report
says so and stops. The client report optionally dials one `host:port` and
reports the negotiated version, suite and peer identity; that dial is the only
network action in the file and it happens only when asked.

Where the address comes from matters. On the CLI path,
`TLSPreflightReport(dialAddr)` may take it from a flag; whoever runs it already
has the shell. On the OAM route it is **not** taken from the request: a route
reachable over the network that dialled whatever `host:port` a caller named
would be a way of opening connections from inside the service to addresses of
the caller's choosing. The route dials only the `preflight-dial` address in the
`http-client` block, if one is configured, and nothing otherwise.

The chain check assumes the CA that issued this process's certificate is the CA
its peers are verified against -- true in a single-CA cluster. A deployment with
a separate issuing CA for leaves sees it as a problem; the report says so.

`api.TLSPreflightReport(dialAddr)` runs it over every `tls` block the process is
configured with (each API in `api`, and `http-client`) and returns one text
report plus a pass/fail. It is the callable entry point for a `--tls-preflight`
CLI flag. `api.APIHandlerTLSPreflight` serves it as `text/plain`, 200 when
everything passed and 503 otherwise, so a pipeline can gate on the status. Its
doc comment carries the registration in the exact shape the services' working
ping route uses (`GET`, `EndPointTypeHTTPJSON`, `RequestContentTypeNone`): in
this product family an OAM command registered with an empty method panics at
call time with `OAMCommandConfigurationErrorMethodEmpty`, and a route that
exists to be called when something is wrong must not itself be the thing that
is wrong. The handler is tested directly, not through `routeHandler`; the
registration shape is the services' own and was not re-run through the router
here.

### 7.4 Observability, since production is observed rather than tested

`http.server.request.duration` and `http.server.request.count` gain
`tls.version`, `tls.cipher` and `tls.peer_identity` attributes (the last only
when a peer was verified); the client-side pair in `utils/http/client` gains
the same from `resp.TLS`. Peer identity is bounded by the number of services, so
it is safe as a metric dimension. "Is any caller still on 1.2" and "who calls
this route" are dashboard queries.

`dxlib.tls.certificate.expiry` is an observable gauge in days, one series per
certificate in service (`tls.certificate.role`, `.file`, `.subject`),
registered on the global OpenTelemetry meter so initialisation order does not
matter (a process without a provider gets a no-op). Certificate expiry is the
single most common cause of an mTLS outage; this makes it alertable rather than
discoverable.

### 7.5 The dev stack

`deploy/dev_local/scripts/gen-dev-tls.sh` (in the deploy tree, not the library)
issues a dev CA and one leaf per compose service into
`deploy/dev_local/secrets/tls/` (not committed), with `serverAuth` and
`clientAuth` on every leaf because every service is both. SANs: the compose
service name (what containers dial), `localhost`/`127.0.0.1`, and the Tailscale
`HOST_LAN_IP` from `.env` on haproxy only, since only haproxy and FreeSWITCH are
reached that way. The script's header spells out the decision it does not make:
haproxy in **passthrough** (mode tcp; the client certificate reaches the
service, mTLS is end to end, but path routing has to move to SNI or ports)
versus **terminate-and-re-encrypt** (two hops with two identities; every
service sees haproxy as the peer, the browser's identity is a header haproxy
sets -- the `X-Proxy-Client-IP` trust model -- and the allow-list must name
haproxy). The second looks encrypted end to end and is not. haproxy resolves
container names once at configuration load and needs its `resolvers docker`
section; those names are the SANs, so renaming a service means reissuing its
leaf. openssl is used there because a laptop has it; nothing in the library or
its tests depends on it. The configuration snippet it prints says `mode: mtls`.

## 8. New package `utils/tls`

Import it aliased; the name collides with `crypto/tls` at call sites:
`utilsTLS "github.com/donnyhardyanto/dxlib/utils/tls"`.

| file | what it holds |
|---|---|
| `tls.go` | package doc; `CATrust*`, `Mode*`, `ClientAuthMigration*` constants; `retiredKeys`; `DXConfigError`; the key readers (`readString`, `readStringSlice`, `readBool`, `readEnum`, `readCATrust`, `readCertPair`); `ClientAuthName`, `refuseRetiredKeys`, `refuseKeysUnder`; `loadCAFile`, `newSystemPool`, `buildPool` |
| `policy.go` | `PolicyModern`, `PolicyIntermediate`; `DXPolicy` with `CheckKeyStrength`, `CheckChainStrength`, `Summary`, the `effective` subtraction and `denyReport`; `readPolicy`, `narrowSuites`, `narrowCurves`, `readFloor`, `readMinVersion`; `MinRSABitsFloor`, `MinECDSABitsFloor`; `KeyDescription` |
| `deny.go` | the family-token vocabulary; `DXDenyList`, `DXDeniedCertificate`; `readDenyList`, `loadDenyFile`, `mergeDenyLists`, `subtractDeny`, `checkDeniedChain`; `SPKISHA256Hex`, `SerialDescription`, the SPKI and serial parsers |
| `reload.go` | `DXCertificateReloader` (`Get`, `Leaf`, `GetCertificate`, `GetClientCertificate`); `DXCAPoolReloader` (`Get`, `Pool`, `Count`, `Certificates`); `DXDenyFileReloader` (`Get`, `List`) |
| `identity.go` | `PeerIdentity`, `SANs`, `MatchesAllowedSAN` |
| `server.go` | `DXServerTLSSettings`, `ParseServerSettings`, the per-mode key rules; `DXServerTLS` with `NewServerTLS`, `BuildServerConfig`, `ConfigForHTTPServer`, `DenyList`, `Summary`; the `getConfigForClient` and `verifyConnection` hooks |
| `client.go` | `DXClientTLSSettings`, `ParseClientSettings`; `DXClientTLS` with `NewClientTLS`, `BuildClientConfig`, `DenyList`, `Summary`; its `verifyConnection` and `refreshDenyFile` |
| `handshake_error.go` | `HandshakeClass*` constants; `ClassifyHandshakeError`, `ClassifyHandshakeText`; `NewHandshakeErrorLogWriter` for `http.Server.ErrorLog` |
| `preflight.go` | `DXPreflightReport`; `PreflightServer`, `PreflightClient`; `PreflightExpiryWarningDays` |
| `metrics.go` | the expiry gauge; `ObservedCertificates` |
| `tlstest/tlstest.go` | in-memory PKI for tests: `NewRootCA`, `NewRootCASignedWith`, `NewIntermediate`, `Issue` with `LeafOptions` (SANs, validity, key size, EKUs, signature algorithm, key reuse), `CertPEM`, `KeyPEM`, `WriteFile`, `WriteLeaf`, `WriteCA` |

`utils/tls` imports `dxlib/log`, `dxlib/errors`, `dxlib/utils` and the OpenTelemetry
API; nothing imports it except `api`, `utils/http/client`, `websocket/client`
and (through the client) `app`. No cycle.

## 9. Files touched

New:

| file | lines | purpose |
|---|---|---|
| `utils/tls/tls.go` | 453 | readers, trust source, transport mode, pool building |
| `utils/tls/policy.go` | 509 | handshake policy and the deny subtraction |
| `utils/tls/deny.go` | 708 | deny-lists: tokens, certificates, signature algorithms, the deny-file format |
| `utils/tls/reload.go` | 399 | hot reload |
| `utils/tls/server.go` | 480 | server settings and config |
| `utils/tls/client.go` | 301 | client settings and config |
| `utils/tls/identity.go` | 98 | SAN identity |
| `utils/tls/handshake_error.go` | 184 | failure classification, rate-limited error log |
| `utils/tls/preflight.go` | 351 | preflight |
| `utils/tls/metrics.go` | 97 | expiry gauge |
| `utils/tls/tlstest/tlstest.go` | 268 | in-test PKI |
| `utils/tls/tls_test.go` | 1997 | handshake-level tests |
| `api/api_tls.go` | 134 | `TLSPreflightReport`, `APIHandlerTLSPreflight`, `tlsAttributes` |
| `api/api_tls_test.go` | 406 | real-`DXAPI` tests |
| `deploy/dev_local/scripts/gen-dev-tls.sh` | 141 | dev PKI (deploy tree, not the library) |

Changed:

| file | change |
|---|---|
| `api/api.go` (+85/-9) | `DXAPI.TLS`; `DXAPIAuditLogEntry.PeerIdentity`; `ApplyConfigurations` reads the `tls` block, fatal on any error, logs plaintext when absent and when `mode: http`; `routeHandler` fills `PeerIdentity` in both audit entries and adds TLS metric attributes; `StartAndWait` sets `TLSConfig` via `ConfigForHTTPServer`, the classifying `ErrorLog`, logs the mode, and calls `ListenAndServeTLS("", "")` |
| `api/api_endpoint.go` (+23/-0) | `NewEndPointRequest` fills `PeerCertificate`/`PeerIdentity`; `VerifiedPeerCertificate`, `PeerIdentityFromRequest` |
| `api/api_endpoint_request.go` (+17/-0) | the two fields, with the `VerifiedChains` reasoning in the comment |
| `api/api_endpoint_request_proxy.go` (+12/-2) | both `http.Client`s come from `utilsHttpClient.NewHTTPClient(HTTPClientDoTimeout)`; handshake failures classified into the request log |
| `utils/http/client/client.go` (+151/-8) | `ClientTLS`, `LoadFromConfiguration`, `ApplyTLSConfiguration`, `Transport`, `TLSClientConfig`, `NewHTTPClient`, `LogHandshakeFailure`; `HTTPClient` uses the shared client; OTel attributes from `resp.TLS` |
| `websocket/client/client.go` (+37/-1) | `NewDialer`, `Dial` -- the file was a 30-line manager stub with no dial code at all, so the WebSocket client call site was created rather than modified |
| `app/app.go` (+14/-0) | `IsHTTPClientExist`; loads `http-client` before `api` |

Every consumer builds unchanged. The nine modules under
`digital-contact-center/src/cmd/*` and `src/library/contact-center-common` reach dxlib
through `replace` directives, not `go.work` (which does not list it, despite the
repository's working notes saying otherwise), and all nine compile against the modified
library with no edit of their own. That is the proof of "additive"; the library's own
green suite is not.

Not touched: `LIBRARY.md`. It claims to list every exported identifier but
already omits `utils/http/client` and `websocket/client`; adding `utils/tls`
alone would not make it accurate, so it is left for a separate pass.

## 10. Defaults and why

| what | default | why |
|---|---|---|
| `mode` | none, required | the transport used to be inferred from a combination of keys; one word a review can read replaces the inference |
| `ca-trust`, `tls-policy` | none, required where a trust decision is made | a defaulted security decision is one a review cannot see; two sides with different defaults is the trap this replaces |
| `client-auth-migration` | absent = `require-and-verify` | the enforcing state is the one that needs no key; the loosening states need a key that names them as temporary and a warning on every start |
| `min-version` | the policy floor (1.2 for intermediate, 1.3 for modern) | 1.2 is the lowest version with no protocol-level weakness; a pinned deployment sets it explicitly and the log shows it either way |
| `cipher-suites`, `curves` | the profile's lists | narrow-only overrides |
| `deny-cipher-suites`, `deny-curves`, `deny-certificates`, `deny-certificate-signature-algorithms` | none | subtract-only; a recognised token that removes nothing is a no-op, so a fleet-wide push never fails closed on a service that was already safe |
| `deny-file` | none | the hot-reloaded half; `{}` is its steady state |
| `min-rsa-bits` / `min-ecdsa-bits` | 2048 / 256 | Go's own floor is 1024; a 1024-bit leaf that chains is otherwise accepted |
| `enabled` | `true` | a block that is written is meant |
| `allowed-client-sans` | none | opt-in; without it every certificate the CA issued is a caller, and the log says so |
| `allowed-client-sans-log-only` | `false` | enforcing when set; the ladder says when to use `true` |
| `insecure-skip-verify` | `false`, client only, warn-logged | for a developer's self-signed endpoint; does not exist on the server |
| `server-name` | the URL host | the escape hatch that keeps verification on |
| `ClientCAs` under `https` | an empty pool | never nil, so the system-roots fallback is unreachable even if `ClientAuth` were raised on the config |
| outbound transport | nil (`http.DefaultTransport`) when no block | byte-for-byte the previous client; with a block, a `Clone()` of `DefaultTransport` so timeouts, proxy-from-env and `ForceAttemptHTTP2` survive |
| `ErrorLog` on the listener | nil without TLS, classifying writer with it | plaintext behaviour unchanged |
| reload throttle | none (stat per handshake) | a stat is noise beside the handshake; one less knob |

## 11. Tests

All in-process, standard library only, no fixtures, no network, no openssl. The
same suite runs on a laptop, in CI and on an air-gapped build host, which is
what the no-fixtures rule was for. `tlstest` builds a root, an intermediate,
and leaves with chosen SANs, validity windows, key sizes, EKUs, signature
algorithms and reused keys; the bad set is wrong CA, expired/not-yet-valid,
RSA-1024, SAN not in the list, `CA:TRUE` as a leaf, serverAuth-only as a
client, a denied key, a denied certificate, a denied intermediate, a denied
signature algorithm.

Servers in `utils/tls/tls_test.go` are `net.Listen` + `http.Server.ServeTLS(ln,
"", "")`, which is exactly what `ListenAndServeTLS` does. `httptest.Server.StartTLS`
is not used, for two reasons that would each make a test pass or fail for the
wrong reason: it injects its own certificate into `Certificates` when the slice
is empty, and Go then skips `GetCertificate` for a dial to an IP address (no
SNI) -- so the hot-reload tests would have been testing httptest's certificate;
and it forces `NextProtos` to `http/1.1`, hiding the HTTP/2 path the WebSocket
test exists to exercise. The `api` tests drive the real `DXAPI.StartAndWait` on
a free `127.0.0.1` port.

| requirement | test |
|---|---|
| three trust sources on the server; `ClientCAs` never nil | `TestServerTrustSources` |
| three trust sources on the client; `RootCAs` never nil; wrong CA classified `TRUST`; `insecure-skip-verify` client-only | `TestClientTrustSources` |
| "neither" is inexpressible; missing / `""` / garbage are three messages for `mode`, `ca-trust`, `tls-policy`; long value capped | `TestRequiredEnumsHaveThreeDistinctFailures` |
| error text never carries the map | `TestConfigErrorsNeverEchoTheMap` |
| trim + case-insensitive, canonical form, `mode` and `client-auth-migration` included | `TestEnumsAreTrimmedAndCaseInsensitive` |
| contradictions refused | `TestCATrustContradictionsAreRefused`, `TestFileAndAllowListContradictionsAreRefused` |
| `client-auth` retired; `http` refuses every TLS-bearing key by name; `https` refuses `ca-trust`/`ca-files`/`allowed-client-sans`/`client-auth-migration`, needs cert and policy, sets `NoClientCert` with a non-nil empty `ClientCAs`, serves a no-cert and an unknown-CA client; `mtls` needs `ca-trust`; `enabled: false` under `mtls` refuses a bad value in every key and needs no file | `TestModeIsExplicitAndRefusesContradictions` |
| migration rungs: only `request`/`verify-if-given`, map to the right `ClientAuthType`, summary says `NOT ENFORCING MTLS` (enabled and disabled), enforcing summary does not | `TestClientAuthMigrationIsAStateNotAMode` |
| no cert refused under `mtls`, allowed under `https` and both rungs; `request` leaves even a good cert unverified; wrong CA under `request` is 200 with no verified chain | `TestModesAndRungsAtTheHandshake` |
| overrides narrow only; insecure suite vs not-in-policy; 1.3 cipher key refused | `TestPolicyOverridesMayOnlyNarrow` |
| deny tokens subtract after narrowing, in profile order; family tokens and IANA names; recognised no-ops accepted and reported; 1.3-only no-op explained; typo and regex refused with the vocabulary; deny-to-empty refused; curves likewise with `P-521` as a no-op; client block takes the same keys | `TestDenyListsSubtractFromTheResolvedPolicy` |
| a server denying `CHACHA20` refuses a real ChaCha-only 1.2 client (server alert, server log) and admits AES and 1.3 clients; a client denying two curves fails against a server narrowed to X25519 and succeeds on P-256 | `TestDenyListsAreEnforcedAtTheHandshake` |
| deny-file must exist; `{}` denies nothing; a pushed suite deny refuses the next handshake with no restart and keeps HTTP/2; six bad pushes each keep the previous list in force; lifting is a push; a policy-emptying file is a startup error | `TestDenyFileHotReloadsOnTheServer` |
| deny by SPKI in hex, upper hex, colon hex, base64; by issuer-serial in decimal, `0x`, colon hex, bare hex; bystander admitted; same key re-issued is caught by SPKI and missed by issuer-serial; a wrong issuer with the same serial does not match; a denied intermediate takes every leaf under it, a root-issued leaf passes; server log line classifies `REVOKED`, handler never runs; the client refuses a server by leaf, by intermediate, and under `insecure-skip-verify`, classified `REVOKED`; malformed entries refused | `TestDenyCertificatesRevokeAtTheHandshake` |
| unknown algorithm name refused with the list; SHA-1/MD5 names are recognised no-ops; denying the leaf's algorithm refuses that leaf only; denying the intermediate's refuses both leaves; denying the anchor's refuses nothing; the client applies the same rule | `TestDenyCertificateSignatureAlgorithms` |
| the client's deny-file certificate denies hot-reload through `VerifyConnection`; its suite denies are read and reported but leave the offered suites unchanged | `TestDenyFileCertificateDeniesHotReloadOnTheClient` |
| preflight reads the deny-file, reports holdings, removals and no-ops; flags our own certificate and our own CA when denied; reports a broken file as a problem; client too | `TestPreflightReportsTheDenyList` |
| disabled block still validated; files not needed | `TestDisabledBlockIsStillValidated` |
| empty system store refused (Linux) | `TestSystemPoolEmptinessDetection`, run in `golang:1.27-alpine` with the store pointed at nothing |
| intermediate chain accepted | `TestIntermediateChainIsAccepted` |
| SAN not in list refused at handshake, handler never runs; allowed gets 200; DNS case-insensitive; log-only admits | `TestAllowedClientSANsRejectAtHandshake` |
| RSA-1024 leaf that chains is refused (server and client); floor may be raised; own weak cert refused at load | `TestKeyStrengthFloorRefusesAValidWeakCertificate` |
| `CA:TRUE` leaf refused; Go's EKU check confirmed | `TestCACertificateAsLeafAndMissingClientEKUAreRefused` |
| real clients offering only TLS 1.0, TLS 1.1, CBC, 3DES, RSA key transport, P-521 are refused; compliant 1.2 and 1.3 clients accepted; `modern` refuses a 1.2-max client | `TestPolicyRefusesDowngradedClients` |
| server leaf hot reload; broken key keeps old; weak rotation refused | `TestServerCertificateHotReload` |
| client leaf hot reload | `TestClientCertificateHotReload` |
| CA pool hot reload; HTTP/2 kept after rotation | `TestClientCAPoolHotReloadKeepsHTTP2` |
| `server-name` override; failure classified `NAME` | `TestServerNameOverride` |
| clock problem distinguishable from trust problem, both sides | `TestValidityWindowIsDistinguishableFromTrust` |
| preflight: good, `mode: http`, `mode: https`, wrong CA, not-yet-valid, weak key, expiring soon, config error, dial; prints `spki-sha256` | `TestPreflightReportsWhatAHandshakeWouldFind` |
| expiry gauge sees the certificate | `TestCertificatesInServiceAreObservedForTheExpiryGauge` |
| real `DXAPI` over mTLS: `PeerCertificate`/`PeerIdentity` on the request, audit entry, HTTP/2, TLS 1.3; `HTTPClient` and `HTTPClientDo` call sites; no-cert client refused | `TestDXAPIServesMTLSAndExposesThePeer` |
| WebSocket over mTLS through the library's dialer, with HTTP/2 on | `TestDXAPIWebSocketOverMTLS` |
| `ApplyConfigurations`: absent block is plaintext with `TLS == nil`; `mode: mtls` block built with `RequireAndVerifyClientCert`; disabled block validated | `TestApplyConfigurationsReadsTheTLSBlock` |
| bad `ca-trust` stops the process (exit 1, exact message), in a child process | `TestApplyConfigurationsRefusesToStartOnABadTrustSource` |
| preflight over the process configuration; handler 200/503 | `TestTLSPreflightReportCoversEveryBlock` |

On negative handshakes: Go's client will construct a TLS 1.0-only or 1.1-only
hello when `MinVersion`/`MaxVersion` are set explicitly, and will offer a
CBC-only, 3DES-only, RSA-key-transport-only or ChaCha-only suite list when
`CipherSuites` names them, so each negative case is a real handshake against
the real listener and not a claim; no `GODEBUG` was needed. Each refused case
asserts two things that a hello Go declined to build could not produce: the
client's error is the server's alert (`remote error: tls: ...`), and the
server's error log grew by a handshake error for it.

The `utils/tls` suite runs green under `-race` on macOS (the development host),
which matters for the atomic pointers the deny-file reload introduced. `api`
runs green as well. An earlier revision of the suite also ran in a
`golang:1.27-alpine` container, and `utils/tls` a second time in that container
with the root store pointed at nothing, where the empty-store refusal fired as
designed; nothing in this revision touches the system-store code.

What a unit test cannot cover: the system store on a real distroless image
(covered by the container run with the store pointed at nothing), a Kubernetes
Secret swap (covered by rewriting the file in place; `os.Stat` follows the
symlink the same way), and a real clock skew (covered by a not-yet-valid leaf,
which produces the same error).

## 12. Decisions worth flagging

- **No inline PEM keys.** Inline `ca-pem`/`key-pem` material was considered
  and not built: an inline certificate cannot hot-reload, which is mandatory
  here; a private key in the configuration map travels through
  `DXConfiguration.AsString`, which does not mask; and files are what
  Kubernetes and a Vault agent give you. Files only. The `key` masking
  heuristic in `FilterSensitiveData` masks the `key-file` *path* in logs, which
  is harmless.
- **`mode` replaces inference, and `client-auth` is refused rather than
  ignored.** Section 1.8. The retired key is the one unknown key this package
  does not ignore, because ignoring it would leave a file saying something the
  listener does not do.
- **`https` refuses `ca-trust` instead of requiring a pool Go never reads.**
  Section 1.7. `ClientCAs` is an empty pool there, not nil, so the invariant
  "never nil" holds without a named source.
- **The no-op exception to refuse-contradictions.** Section 3.9. It is the
  only place a correctly spelled key is allowed to do nothing, and the reason
  -- a fleet-wide push must not fail closed on services that were already safe
  -- is written beside both the rule and the exception, in the code and here.
- **The trust anchor is exempt from the signature-algorithm deny.** Section
  3.11. Nobody verifies an anchor's signature; refusing a cluster over its
  root's algorithm would be fail-closed with nothing behind it. It is *not*
  exempt from `deny-certificates`: a denied root key is a denied root key.
- **Client-side certificate denies hot-reload; client-side suite denies do
  not.** Section 3.10. The brief assumed a blanket client-side restart; the
  half that matters in a breach turned out not to need one, because
  `VerifyConnection` is a per-handshake hook the client does have.
- **`PeerCertificate` from `VerifiedChains`, not `PeerCertificates[0]`.** The
  obvious source is the latter; under the `request` rung that is an
  unverified certificate. Section 4.3.
- **`utils/http/client` imports `configuration`.** So that `LoadFromConfiguration`
  mirrors the other managers. `configuration` does not import it back.
- **`websocket/client` had no dial code.** The file was a manager stub with no
  `Dialer` in it. `NewDialer` and `Dial` were added.
- **`ConfigForHTTPServer`.** Needed because Go 1.27's `ServeTLS` does not
  mutate `TLSConfig.NextProtos` in place, contrary to older behaviour. Section 5.
- **`httptest.StartTLS` not used.** Section 11.
- **The OAM preflight route does not take a dial target.** Section 7.3.
- **Handshake warnings are rate-limited per class.** Section 7.2.

## 13. Out of scope, one line each

- Redis TLS: `redis` package, `redis.Options.TLSConfig`, should take
  `BuildClientConfig` from a `tls` block in the redis configuration.
- PostgreSQL: pgx `sslmode=verify-full` with `sslrootcert`/`sslcert`/`sslkey`
  in the connection string, or `pgx.ConnConfig.TLSConfig` from the same builder.
- MinIO / `object_storage`: `minio.Options.Transport` from
  `utilsHttpClient.Transport()`.
- Vault API client: `vault/api.Config.ConfigureTLS` from the same block.
- OTLP exporters: `otlptracehttp.WithTLSClientConfig` / `otlpmetrichttp`
  likewise.
- OCSP stapling from a locally refreshed file, if a staple is demanded:
  section 7.1. Full CRL files from the PKI are not planned; `deny-certificates`
  is the air-gap-safe form of the same thing, section 3.11.
- Client-side root-pool and suite/curve-deny hot reload via `DialTLSContext`:
  section 5.
- FIPS 140-3: available as `GODEBUG=fips140=on`; nothing to build, section 3.12.
- `LIBRARY.md` entry for `utils/tls`.

## 14. Example configurations

Inbound mTLS with an allow-list in log-only mode, outbound with a client
certificate, a shared deny-file, OAM plaintext by an explicit word:

```json
{
  "api": {
    "api": {
      "address": "0.0.0.0:58081",
      "tls": {
        "mode":        "mtls",
        "cert-file":   "/etc/dcc/tls/tls.crt",
        "key-file":    "/etc/dcc/tls/tls.key",
        "tls-policy":  "intermediate",
        "min-version": "1.2",
        "ca-trust":    "custom",
        "ca-files":    ["/etc/dcc/tls/ca.crt"],
        "deny-file":   "/etc/dcc/tls/deny.json",
        "allowed-client-sans": [
          "spiffe://cluster.local/ns/dcc/sa/queue-scheduler",
          "spiffe://cluster.local/ns/dcc/sa/push-notification-server"
        ],
        "allowed-client-sans-log-only": true
      }
    },
    "oam": { "address": "0.0.0.0:48081", "tls": { "mode": "http" } }
  },
  "http-client": {
    "tls": {
      "cert-file":  "/etc/dcc/tls/tls.crt",
      "key-file":   "/etc/dcc/tls/tls.key",
      "tls-policy": "intermediate",
      "ca-trust":   "custom",
      "ca-files":   ["/etc/dcc/tls/ca.crt"],
      "deny-file":  "/etc/dcc/tls/deny.json"
    }
  }
}
```

The deny-file during an incident -- an advisory against ChaCha20 and a
compromised intermediate, pushed to every pod's ConfigMap and in force on the
next handshake:

```json
{
  "deny-cipher-suites": ["CHACHA20"],
  "deny-certificates": [
    {"spki-sha256": "b662cbb0c98ac4d2a14c77a3f9a40d4ea1099a076527d70973a45ba3599ed33b",
     "reason": "INC-2026-0901 intermediate key compromised"}
  ]
}
```

The same file the week after, with the advisory lifted and the intermediate
still denied, because the key stays compromised:

```json
{
  "deny-certificates": [
    {"spki-sha256": "b662cbb0c98ac4d2a14c77a3f9a40d4ea1099a076527d70973a45ba3599ed33b",
     "reason": "INC-2026-0901 intermediate key compromised"}
  ]
}
```

A deny written inline, for a service that must never offer AES-128 and must
refuse PKCS#1 v1.5 certificate signatures:

```json
"tls": {
  "mode":        "mtls",
  "cert-file":   "/etc/dcc/tls/tls.crt",
  "key-file":    "/etc/dcc/tls/tls.key",
  "tls-policy":  "intermediate",
  "ca-trust":    "custom",
  "ca-files":    ["/etc/dcc/tls/ca.crt"],
  "deny-cipher-suites": ["AES-128"],
  "deny-certificate-signature-algorithms": ["SHA256-RSA", "SHA384-RSA", "SHA512-RSA"]
}
```

TLS 1.3 only, once every peer is known to speak it:

```json
"tls": {
  "mode":        "mtls",
  "cert-file":   "/etc/dcc/tls/tls.crt",
  "key-file":    "/etc/dcc/tls/tls.key",
  "tls-policy":  "modern",
  "ca-trust":    "custom",
  "ca-files":    ["/etc/dcc/tls/ca.crt"]
}
```

The first rung of the ladder -- mTLS switched on without refusing anyone yet,
warn-logged on every start:

```json
"tls": {
  "mode":                  "mtls",
  "client-auth-migration": "request",
  "cert-file":             "/etc/dcc/tls/tls.crt",
  "key-file":              "/etc/dcc/tls/tls.key",
  "tls-policy":            "intermediate",
  "ca-trust":              "custom",
  "ca-files":              ["/etc/dcc/tls/ca.crt"]
}
```

A server-authenticated listener for a browser-facing endpoint behind haproxy in
passthrough, where the browser has no client certificate:

```json
"tls": {
  "mode":        "https",
  "cert-file":   "/etc/dcc/tls/tls.crt",
  "key-file":    "/etc/dcc/tls/tls.key",
  "tls-policy":  "intermediate"
}
```

Outbound to a public upstream as well as internal services:

```json
"http-client": {
  "tls": {
    "tls-policy": "intermediate",
    "ca-trust":   "system-and-custom",
    "ca-files":   ["/etc/dcc/tls/ca.crt"]
  }
}
```

A block parked for a dev host that has no certificates yet -- validated, not in
force:

```json
"tls": {
  "enabled":     false,
  "mode":        "mtls",
  "cert-file":   "/etc/dcc/tls/tls.crt",
  "key-file":    "/etc/dcc/tls/tls.key",
  "tls-policy":  "intermediate",
  "ca-trust":    "custom",
  "ca-files":    ["/etc/dcc/tls/ca.crt"]
}
```
