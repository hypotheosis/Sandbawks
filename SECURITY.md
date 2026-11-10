# Security Policy

## Reporting a vulnerability

Please report security vulnerabilities privately by email to
**Peter Grace <pete.grace@gmail.com>**. Do not open a public issue,
pull request, or discussion for a suspected vulnerability.

Include as much of the following as you can:

- the affected component (`sandbawks-api`, `sandbawks-controller`,
  `sandbawks-node`, `envd`, `containerd-shim-sandbawks-v1`, the Helm chart,
  or `scripts/install-runsc-k3s.sh`) and version or commit;
- a description of the issue and its impact (for example, sandbox escape,
  cross-tenant access, authentication bypass, or credential exposure);
- steps to reproduce, a proof of concept, or relevant logs;
- any suggested fix or mitigation.

You can expect an acknowledgement within a few days. Once the report is
confirmed, a fix will be prepared privately and released, and you will be
credited in the release notes unless you prefer to remain anonymous.

## Supported versions

sandbawks is pre-1.0. Security fixes are made on `main` and shipped in the
next release; only the latest release is supported.

## Scope

sandbawks runs untrusted code inside sandboxes, so isolation boundaries are
in scope: escapes from a sandbox to the node or to other sandboxes, bypasses
of a sandbox's egress NetworkPolicy, and access to another team's sandboxes,
files, or snapshots through the API.

Weaknesses in the underlying sandbox runtimes themselves (gVisor, Kata
Containers) should be reported to those projects.

Resource limits: a sandbox's CPU and memory limits are enforced on the host
only when runsc runs with the systemd cgroup driver, which
`scripts/install-runsc-k3s.sh` configures. Cgroup limits set from *inside* a
gVisor sandbox are not enforced. See
`changelog/20260610T014430Z-issue-309-gvisor-memory-oom-synthesis.md`.
