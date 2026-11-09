# Security Policy

Thank you for helping to keep oxia.net and its users safe. This repository is an **unofficial community port** of the
[Oxia](https://github.com/oxia-db/oxia) Go client to .NET. It contains the client library only; it does not contain the Oxia server.

## Reporting a vulnerability

**Please do not report security vulnerabilities through public GitHub issues, discussions or pull requests.**

Report them privately through GitHub's
[private vulnerability reporting](https://github.com/danrubAtPlanview/oxia.net/security/advisories/new) form of this repository.

Please include:

* A description of the vulnerability and of its potential impact
* The affected component (for example: wire codec, TLS configuration, authentication, pooled read API) and version or commit
* Steps to reproduce the issue, or a proof of concept
* Any known mitigation or suggested fix

If you cannot use the form, open an issue that only says that you would like to report a vulnerability (no details), and a
maintainer will arrange a private channel.

### Where to report what

| The problem is in... | Report to |
|---|---|
| This .NET client library (code in `src/`) | This repository, through the form above |
| The Oxia server, the coordinator, the Helm charts, or the Go client | The upstream project: <https://github.com/oxia-db/oxia/security/advisories/new> (see its [SECURITY.md](https://github.com/oxia-db/oxia/blob/main/SECURITY.md)) |
| A third-party dependency (`Grpc.Net.Client`, `Google.Protobuf`, ...) | The maintainers of that dependency. Tell us too if this library needs a release to pick up the fix |

## What is in scope

Examples of reports we treat as vulnerabilities of this library:

* TLS validation that is weaker than configured (certificate or host name checks skipped, `SslClientAuthenticationOptions` ignored).
* Credentials sent when they should not be, for example a bearer token sent over a plaintext connection although the
  `IAuthentication` implementation requires transport security.
* Tokens, keys or record contents disclosed through logs, exception messages or metric tags.
* Memory-safety or ownership defects in the pooled read API (`Oxia.Client.Pooled`) or in the response pooling that expose the bytes of
  another operation, or let a stale result read recycled memory.
* Defects in the hand-written wire codecs (`Oxia.Common.Proto`) that let a malicious or compromised server crash the process, make it
  allocate without bound, or corrupt state.

## Trust model and what is not a vulnerability

oxia.net follows the trust model of Oxia itself, described in the upstream
[SECURITY.md](https://github.com/oxia-db/oxia/blob/main/SECURITY.md): Oxia is deployed on a trusted network, supports
authentication (OIDC bearer tokens) and TLS, and has **no authorization** (no per-principal access control). In particular:

* A party that can reach an Oxia endpoint, and that the server accepts, can read and write any key. A client library cannot change that,
  so reports that come down to "an authenticated client can act on another client's keys, sessions or ephemeral records" describe
  expected behavior of the system and are not vulnerabilities of this library.
* `ClientIdentity` is a client-chosen provenance tag, not an identity claim.
* The `Failpoints` and `WithFailureInjection` facilities exist for tests only.

A way to get around TLS or token authentication that the operator has enabled, on the client side, is in scope.

## Response process

This is a volunteer-run project. Our targets, on a best-effort basis:

* acknowledge your report within **7 days**;
* give an initial assessment, with an estimated timeline, within **14 days**;
* keep you informed of the progress toward a fix, and ask for additional information if needed.

## Disclosure policy

We follow coordinated disclosure. When a vulnerability is confirmed, the maintainers will:

1. develop and test a fix privately (GitHub's temporary private fork of the advisory can be used for this);
2. request a CVE identifier, if appropriate;
3. release a patched version;
4. publish a [security advisory](https://github.com/danrubAtPlanview/oxia.net/security/advisories), crediting the reporter unless
   they prefer to remain anonymous.

We ask you to keep the details private until the advisory is published.

## Supported versions

No release has been published yet. Until the first release, security fixes are made on the default branch. After the first release, security
fixes are released as a patch release of the latest minor version; older versions are not maintained.

This client targets .NET 10 and the Oxia server version pinned in [PORTING.md](PORTING.md). Run a supported .NET runtime and keep your Oxia server
up to date: server-side fixes are published by the upstream project.

## Security response team

The [maintainers](MAINTAINERS.md) act as the security response team and handle reports according to this policy.
