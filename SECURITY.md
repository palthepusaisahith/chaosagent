# Security Policy

## Supported versions

ChaosAgent is a pre-1.0 project with no stable release or production security
guarantee. Security fixes currently target the latest revision of the default
branch only.

| Version                        | Supported |
| ------------------------------ | --------- |
| Latest default-branch revision | Yes       |
| Older commits and forks        | No        |

## Reporting a vulnerability

Do not open a public issue, discussion, or pull request for a suspected
vulnerability. Use
[GitHub private vulnerability reporting](https://github.com/palthepusaisahith/chaosagent/security/advisories/new)
as the project's official confidential vulnerability-reporting channel. If
GitHub private vulnerability reporting is temporarily unavailable, wait for the
confidential channel to be restored and retry. Do not post vulnerability details
or sensitive findings publicly in the meantime.

Never send real credentials, access tokens, customer data, exploit payloads
containing third-party secrets, or other live sensitive material. Revoke exposed
credentials before reporting them and use minimal synthetic evidence where
possible.

Include:

- the affected commit, version, file, or component;
- the vulnerability type and expected security boundary;
- reproducible steps or a minimal proof of concept;
- realistic impact and required attacker capabilities;
- relevant platform and dependency versions;
- suggested remediation, if known; and
- whether the issue is already public or has been reported elsewhere.

## Response process

The maintainer aims to acknowledge a report within 7 days and provide an initial
assessment within 14 days. Confirmed issues will be triaged by severity, fixed
on the supported branch, and disclosed after a remediation is available when
practical. A 90-day remediation/disclosure target may be used, but timing
depends on severity, complexity, upstream coordination, and active exploitation.
These are targets, not service-level guarantees.

Please allow reasonable time for investigation before public disclosure and
coordinate any advisory or credit through the private report.

## Scope

Only vulnerabilities in currently implemented repository functionality are in
scope. This presently includes repository code, dependency handling, CI
workflows, unsafe defaults, and credential exposure. Planned or future
ChaosAgent controls are not implemented security surfaces and cannot yet be
reported as vulnerabilities. Security or design concerns about planned features
may be raised through normal feature or design discussions, provided they do not
include secrets, exploit details, or other sensitive security information.

At this stage the repository contains bootstrap and governance tooling only; it
does not operate a hosted service, accept arbitrary real-world credentials, or
integrate with real payment systems.

General feature requests, unsupported forks, social engineering unrelated to
this project, and vulnerabilities entirely in third-party services should be
reported through their appropriate public or upstream channels.
