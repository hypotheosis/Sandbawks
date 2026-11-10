# Explanation: multi-namespace tenancy

Date: 2026-11-10T20:47:24Z

Added `site/src/content/docs/explanation/multi-namespace-tenancy.md` and its sidebar
entry, and updated the tenancy section of the E2B-mapping explanation to match.
The new page covers:

- the cluster-wide controller and node watches;
- why TeamAPIKeys live in the system namespace;
- the `sandbawks-tenant` ServiceAccount, Role and RoleBinding;
- gateway impersonation, so the API server enforces the boundary;
- the current limit that envd traffic resolves only in the gateway default
  namespace (#13).
