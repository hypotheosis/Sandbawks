# How-to: allow sandbox egress

Date: 2026-11-10T20:34:40Z

Added `site/src/content/docs/how-to/allow-sandbox-egress.md` and its sidebar entry. It covers:

- the default-deny NetworkPolicy and IP/CIDR vs DNS-name `egressAllow` entries;
- enabling Cilium FQDN egress (`controller.fqdnEgress`);
- a template allow-list and the per-create `egressAllow` union, including on
  warm claims;
- the `sandbox-<id>-egress` / `sandbox-<id>-fqdn` policies, and the
  infrastructure egress added when telemetry is on.
