---
title: Allow sandbox egress
description: Open specific network destinations for sandboxes, which deny all egress by default, with a template allow-list, a per-request override, and Cilium FQDN rules for DNS names.
slug: how-to/allow-sandbox-egress
head:
  - tag: title
    content: Allow sandbox egress · Sandbawks docs
tableOfContents: true
---

This guide shows you how to let sandboxes reach specific network destinations,
such as a package index or a Git server. Every sandbox starts with a NetworkPolicy
that denies all egress. You open destinations with an `egressAllow` list on the
template, on a create request, or on both.

## Before you begin

- A CNI that enforces NetworkPolicy. Without one, the policy is ignored and egress
  stays open.
- To allow destinations by DNS name, the Cilium CNI with the
  `CiliumNetworkPolicy` CRD installed. IP addresses and CIDR blocks work with any
  NetworkPolicy-aware CNI.

## Choose IP and CIDR entries or DNS names

Each `egressAllow` entry is one of these:

- An IP address or CIDR block, for example `10.20.0.0/16` or `2001:db8::1`. Each
  one opens all ports to that destination.
- A DNS name, for example `pypi.org`. A standard NetworkPolicy can't express a
  DNS name, so on its own a name entry only opens DNS (port 53). To allow
  connections to the name's addresses, enable FQDN egress, described below.

## Enable FQDN egress on Cilium

Skip this step if you only use IP and CIDR entries.

Set `controller.fqdnEgress.enabled` in your Helm values:

```yaml
controller:
  fqdnEgress:
    enabled: true
    # Defaults; change them if your cluster DNS runs elsewhere.
    dnsNamespace: kube-system
    dnsPodLabel: k8s-app=kube-dns
```

```sh
helm upgrade sandbawks oci://ghcr.io/hypotheosis/charts/sandbawks \
  --version 0.1.29 --namespace sandbawks --reuse-values \
  --set controller.fqdnEgress.enabled=true
```

The controller then creates a `CiliumNetworkPolicy` for every sandbox with a DNS
name in its `egressAllow`. The policy sends the sandbox's DNS lookups through
Cilium's DNS proxy, to the resolver selected by `dnsNamespace` and `dnsPodLabel`,
and allows connections to the addresses those names resolve to.

## Set an allow-list on the template

Add `networkPolicy.egressAllow` to the `SandboxTemplate`. Every sandbox created
from the template through the API, and every warm-pool member, gets these
destinations.

```yaml
apiVersion: sandbawks.dev/v1alpha1
kind: SandboxTemplate
metadata:
  name: python-base
  namespace: team-acme
spec:
  image: registry.example.com/sandboxes/python:3.12
  defaultRuntimeClass: gvisor
  defaultTimeoutSeconds: 1800
  defaultResources:
    cpu: "1"
    memory: 1Gi
  networkPolicy:
    egressAllow:
      - pypi.org
      - files.pythonhosted.org
      - 10.20.0.0/16
```

```sh
kubectl apply -f python-base.yaml
```

A sandbox copies the template's allow-list when it is created. Changing the
template doesn't change sandboxes that already exist.

:::note
A `Sandbox` you apply directly with `kubectl` doesn't inherit the template's
allow-list. Set `spec.networkPolicy.egressAllow` on the `Sandbox` itself.
:::

## Add destinations for one sandbox

To add destinations for a single sandbox, send `egressAllow` in the create
request. The sandbox gets the union of the template's list and the request's
list; a request can add destinations but can't remove the template's.

The E2B SDKs don't send this field, so call the API directly:

```sh
curl -s -X POST "$E2B_API_URL/sandboxes" \
  -H "X-API-Key: $E2B_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"templateID": "python-base", "timeout": 600, "egressAllow": ["git.example.com"]}'
```

The override also applies when the request is served from a warm pool: the
claimed member's policy is widened to the union.

## Check the policies

The controller names a sandbox's policies after the sandbox:

```sh
kubectl get networkpolicy sandbox-<sandbox-id>-egress -n team-acme -o yaml
kubectl get ciliumnetworkpolicy sandbox-<sandbox-id>-fqdn -n team-acme -o yaml
```

The NetworkPolicy lists an `ipBlock` rule for each IP or CIDR entry, and a DNS
rule if any entry is a name. The `CiliumNetworkPolicy` exists only when FQDN
egress is enabled and the sandbox has at least one DNS-name entry; it lists a
`toFQDNs` `matchName` for each name.

To test from inside the sandbox, run a command through your SDK, for example:

```python
sbx.commands.run("curl -sS -o /dev/null -w '%{http_code}' https://pypi.org/simple/")
```

A destination that isn't allowed times out or fails to connect.

:::note
When OpenTelemetry export is enabled, every sandbox's policy also allows DNS and
the telemetry collector, so `envd` can export its own telemetry. This doesn't
depend on `egressAllow`.
:::

## See also

- [Sandbox reference](/reference/custom-resources/sandbox/) — `networkPolicy`.
- [SandboxTemplate reference](/reference/custom-resources/sandboxtemplate/) —
  how the template's allow-list combines with a create request.
- [Helm chart reference](/reference/helm-chart/) — `controller.fqdnEgress`.
