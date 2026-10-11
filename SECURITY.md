# Security Policy

The Oxia maintainers take security seriously and appreciate your efforts to responsibly disclose your
findings.

## Reporting a Vulnerability

**Please do not report security vulnerabilities through public GitHub issues, discussions or pull
requests.**

Instead, report them privately through GitHub's
[private vulnerability reporting](https://github.com/oxia-db/oxia/security/advisories/new) form. Use the
same form for vulnerabilities in any other repository of the [oxia-db](https://github.com/oxia-db)
organization (client libraries, Helm charts, etc.) and mention the affected repository in the report.

Please include in your report:

* A description of the vulnerability and of its potential impact
* The affected component and version(s)
* Steps to reproduce the issue, or a proof of concept
* Any known mitigation or suggested fix

## Trust Model

This section describes what Oxia does and does not protect against. Please check it before reporting.

### Authentication and authorization

Oxia has authentication but no authorization (yet).

* The data server public port can authenticate clients with OIDC bearer tokens (`--auth-provider-name oidc`,
  or `server.public.auth` in the configuration file). It is off by default.
* When authentication is on, the server validates the token and then discards the principal: no RPC
  handler knows who is calling. Every authenticated caller can read, write and delete any key in any
  namespace, and keep alive or close any session.
* The coordinator admin API accepts the same opt-in authentication (`server.public.auth` in the coordinator
  configuration file, `server.admin.auth` in 0.16) and has no authorization either.

Per-principal access control is tracked in [#1310](https://github.com/oxia-db/oxia/issues/1310).

### Trusted network

Oxia is deployed on a trusted network. Every endpoint must be reachable only by parties that the operator
trusts with all of the data:

* The data server public port (6648 by default): the client API.
* The data server internal port (6649 by default): the coordinator and the peer replicas
  (`OxiaCoordination` and `OxiaLogReplication`).
* The coordinator internal port (6649 by default, 6650 in the Helm chart): gRPC health checks.
* The coordinator admin port (6651 by default): the admin API (`OxiaAdmin`).
* The metrics port of the data servers and of the coordinator (8080 by default): Prometheus metrics.
* The coordinator Raft address, with the `raft` metadata provider: coordinator metadata replication.

### Internal ports

The internal ports are the cluster-internal control plane, and by default they do not authenticate
callers. Operators who need authentication there should enable mutual TLS:

* On each data server, the internal server TLS (`server.internal.tls`) with `clientAuth: true` and
  `trustedCaFile` set to the CA that issues the cluster's certificates (`--internal-tls-client-auth`,
  `--internal-tls-trusted-ca-file`). Set both: without `trustedCaFile`, client certificates are verified
  against the system roots.
* On each data server, the replication client TLS (`replication.tls`, `--peer-tls-*` flags), with a
  certificate signed by that CA, to connect to the peer replicas.
* On the coordinator, the controller client TLS (`controller.tls`, `--peer-tls-*` flags), with a certificate
  signed by that CA, to connect to the data servers.

The internal servers also accept an `auth` section, but neither the coordinator nor the peer replicas send
a bearer token, so mutual TLS is the only way to authenticate callers there. The Raft transport of the coordinator
metadata has no TLS option.

### What is not an authentication mechanism

* The data server instance ID. Since 0.17, the data server internal port rejects coordination and
  replication RPCs that do not carry the cluster's ID in the `instance-id` gRPC metadata. This keeps
  internal RPCs from crossing coordinator instances; it does not authenticate the caller. The ID is a
  single UUID for the whole cluster, kept in the coordinator metadata, and it is sent in clear text when
  TLS is off.
* `ClientIdentity`. It is a client-chosen provenance tag (a random UUID by default) that lets a client
  recognize ephemeral records left by an earlier session of itself. It is returned to every reader and
  must not be treated as an identity claim.

### What is not a vulnerability

Until authorization lands ([#1310](https://github.com/oxia-db/oxia/issues/1310)), reports that come down to
"a party that can reach an Oxia endpoint can do X" describe expected behavior and are not vulnerabilities.
This includes acting on another client's keys, sessions or ephemeral records, and calling the internal or
admin APIs of a cluster that does not enable authentication on them. A way to get past OIDC authentication
or mutual TLS that the operator has enabled is in scope.

## Response Process

The maintainers will acknowledge your report within **3 business days** and will provide an initial
assessment, with an estimated timeline for a fix, within **10 business days**.

We will keep you informed of the progress toward a fix and may ask you for additional information.

## Disclosure Policy

Oxia follows a coordinated disclosure process. When a vulnerability is confirmed, the maintainers will:

1. Develop and test a fix privately
2. Request a CVE identifier, if appropriate
3. Release a patched version
4. Publish a [security advisory](https://github.com/oxia-db/oxia/security/advisories), crediting the
   reporter unless they prefer to remain anonymous

We ask you to keep the details of the vulnerability private until the advisory is published.

## Supported Versions

Security fixes are released as a patch release of the latest minor version of Oxia. They may also be
backported to an earlier minor version that still has an active `release-X.Y` branch, at the discretion
of the maintainers.

We recommend running the latest [release](https://github.com/oxia-db/oxia/releases).

## Security Response Team

The Oxia [maintainers](MAINTAINERS.md) act as the security response team of the project and handle the
reports according to this policy. See [GOVERNANCE.md](GOVERNANCE.md) for more details.
