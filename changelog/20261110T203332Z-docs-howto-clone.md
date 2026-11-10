# How-to: clone a sandbox

Date: 2026-11-10T20:33:32Z

Added `site/src/content/docs/how-to/clone-a-sandbox.md` and its sidebar entry. It covers
creating a sandbox from another sandbox's saved state with `restoreFrom`:

- prerequisites for `Snapshot` and `Filesystem` mode;
- cloning through the API (`restoreFrom` / `sourceSandboxID`) or a `Sandbox`
  resource;
- checking the clone and reading its `Failed` condition messages.

It also documents two current limitations: snapshot clones keep the source's
guest identity, and a warm pool makes the API ignore `restoreFrom` (#6).
