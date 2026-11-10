---
title: Point your agent at Sandbawks
description: Run the standard E2B Python SDK against your own Sandbawks cluster — create a sandbox, run a command, write and read a file, and shut it down.
slug: tutorials/point-your-agent-at-sandbawks
head:
  - tag: title
    content: Point your agent at Sandbawks · Sandbawks docs
tableOfContents: true
---

Sandbawks speaks the E2B API, so the code your agent already uses to drive
e2b.dev sandboxes works against your own cluster with a one-line change. In this
tutorial we will prove that end to end: together we'll create a real sandbox on
Sandbawks with the standard E2B Python SDK, run a command inside it, write and
read a file, then shut it down. By the end you'll have watched unmodified E2B
code do real work on infrastructure you control.

We'll use Python, and we'll stay on a single path the whole way — no detours.

## What you need

- Python 3 with `pip`.
- An endpoint to talk to: an `E2B_API_URL`, an `E2B_API_KEY`, and the name of a
  template registered on it. The rest of this tutorial assumes the values from
  the local development endpoint below.

:::note[No endpoint yet?]
If nobody has handed you a Sandbawks URL and key, bring up a local one from the
product source repo:

```bash
SANDBAWKS_KIND_GVISOR=1 just cluster-up
```

That gives you a gateway at `http://localhost:8080`, the template
`api-smoke-template`, and the development API key
`sbk_smoke_smoke_smoke_smoke_smoke` (published for local smoke testing only —
never reuse it in production). A dedicated how-to for standing up an endpoint is
coming; for now this one command is enough to follow along.
:::

## Step 1: Install the E2B SDK

Install the official E2B Python SDK:

```bash
pip install "e2b>=2.21,<3"
```

You should see pip report a successful install ending in a line like
`Successfully installed e2b-2.21.0`. That's the same SDK people use against
e2b.dev — we're not installing anything Sandbawks-specific.

## Step 2: Point the SDK at Sandbawks

The SDK reads its target from environment variables at sandbox-construction
time. Set them to your endpoint:

```bash
export E2B_API_URL=http://localhost:8080
export E2B_SANDBOX_URL=$E2B_API_URL
export E2B_API_KEY=sbk_smoke_smoke_smoke_smoke_smoke
```

Before we drive the SDK, let's confirm the endpoint answers and your key is
accepted. List the sandboxes in your team:

```bash
curl -s -H "X-API-Key: $E2B_API_KEY" "$E2B_API_URL/sandboxes"
```

You should get a JSON array back — `[]` if you have no sandboxes yet:

```json
[]
```

Notice that an empty list is exactly what we want here: it means the gateway is
reachable *and* it accepted your API key. If you instead see `401` or a
connection error, fix that now — the rest of the tutorial builds on this working
connection.

## Step 3: Create your first sandbox

Now we'll drive the SDK. Start a Python session (`python3`) and create a
sandbox:

```python
from e2b import Sandbox

sbx = Sandbox.create(template="api-smoke-template", timeout=300)
print("created:", sbx.sandbox_id)
```

You should see:

```
created: sbx-XXXXXXXX
```

Notice that you got an ID back. That `sbx-...` is a live, gVisor-isolated
sandbox running on your cluster right now — booted from the template, on demand,
by the same SDK call your agent would make.

## Step 4: Run a command inside the sandbox

Let's run something in there. In the same Python session:

```python
result = sbx.commands.run("echo hello-from-sandbawks")
print("stdout:", result.stdout.strip())
```

You should see:

```
stdout: hello-from-sandbawks
```

That output came back from a process executed *inside* the sandbox and streamed
out through Sandbawks to your terminal. The exec path works.

## Step 5: Write and read a file

Sandboxes have a filesystem your code can drive too. Write a file, then read it
back:

```python
sbx.files.write("/tmp/probe.txt", "ok")
print("readback:", sbx.files.read("/tmp/probe.txt"))
```

You should see:

```
readback: ok
```

Notice that the value you wrote came straight back. The filesystem API is
working end to end — write and read both round-tripped through the sandbox.

## Step 6: Shut the sandbox down

When you're done with a sandbox, kill it so it stops consuming cluster
resources:

```python
sbx.kill()
print("killed:", sbx.sandbox_id)
```

You should see:

```
killed: sbx-XXXXXXXX
```

That's the same ID from Step 3 — the sandbox we created is now gone. You can run
Steps 3 through 6 again as many times as you like; each run creates a fresh
sandbox and tears it down cleanly.

## What you just did

You drove a real sandbox on your own Sandbawks cluster — create, exec, file I/O,
and kill — using the **unmodified** E2B Python SDK. The only Sandbawks-specific
thing in the whole exercise was the `E2B_API_URL` you exported in Step 2. That's
the core promise: point existing E2B code at Sandbawks and it just works.

From here:

- **How-to guides** (coming soon) will cover pausing and resuming sandboxes,
  and deploying Sandbawks to a real cluster.
- **Reference** (coming soon) will document the full E2B API surface and the
  Sandbawks custom resources.
- **Explanation** (coming soon) will cover how Sandbawks maps the E2B model onto
  Kubernetes.

For the full SDK surface, see the [E2B SDK documentation](https://e2b.dev).
