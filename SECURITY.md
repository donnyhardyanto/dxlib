# Security policy

## Reporting a vulnerability

Report a security weakness in dxlib only through GitHub private vulnerability
reporting: <https://github.com/donnyhardyanto/dxlib/security/advisories/new>
(the repository's **Security** tab, **Report a vulnerability**). The report
stays private until a fix is released.

Do not open a public issue or pull request about a weakness, and do not
describe it anywhere public before the fix is released.

Issues are turned off on this repository, and pull requests from outside
contributors are not accepted.

## Fixes and disclosure

A confirmed weakness is fixed on `main` and shipped in a new tagged release;
the advisory is published once that release is out. There are no maintenance
branches: consumers pin dxlib by tag in their own `go.mod` and move the pin to
take a fix. Every release carries its SBOM and scan results, described in
`README.md` under Compliance and Security.
