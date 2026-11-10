# Security policy

Date: 2026-11-10T19:03:05Z

Added `SECURITY.md`: vulnerabilities are reported privately by email to
Peter Grace <pete.grace@gmail.com>; only the latest release is supported. It
scopes in sandbox escapes, egress-policy bypasses and cross-team access, routes
gVisor/Kata runtime bugs upstream, and notes that host-side CPU/memory
enforcement needs runsc's systemd cgroup driver (configured by
`scripts/install-runsc-k3s.sh`, see #448).

Also replaced a third party's name in four changelog entries about the
gVisor memory/OOM epic (#304) with a neutral "external user".
