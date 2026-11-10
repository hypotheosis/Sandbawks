# install-runsc-k3s.sh: fix containerd crash loop from a comment that expanded

## Symptom

After running `install-runsc-k3s.sh` and restarting k3s, `k3s-agent.service`
crash-looped. containerd's own log showed:

```
failed to unmarshal TOML at row 37 column 1: toml: invalid character at start of key: `
```

k3s renders `config.toml` from our `config.toml.tmpl`, then containerd exits
status 1 before registering any handler.

## Root cause (two bugs)

**1. The crash — a Go-template action inside a comment.** The template's header
comment literally contained `` `{{ template "base" . }}` ``. k3s renders the
file with Go `text/template`, which has no notion of a TOML `#` comment, so it
expanded **both** occurrences of `{{ template "base" . }}`: the real include on
its own line *and* the one inside the comment. k3s's entire base config was
spliced into the middle of the comment, and the trailing backtick ended up at
column 1 of a line — `invalid character at start of key`.

**2. Latent: wrong CRI plugin key.** The node's base config is `version = 2`
(plugin `io.containerd.grpc.v1.cri`), but the script wrote the `sandbawks-runsc`
handler under the v3 key `io.containerd.cri.v1.runtime`. The detection grepped
`config.toml` for the v3 plugin string — and a prior run's own mis-keyed block
made the detection self-confirm, so it kept choosing the wrong key. Even once
the TOML parsed, `sandbawks-runsc` would silently fail to register.

## Fix

- Reworded the template comment so it contains no `{{ }}` action; the include
  appears exactly once, on its own line. Added a warning comment in the script
  source so the trap isn't reintroduced.
- Detection now keys off k3s's own `version =` line (`version = 3` -> v3 key,
  else v2), not the plugin string, breaking the self-confirming feedback loop.

## Verification

- `bash -n` and `shellcheck -S warning` clean.
- Ran `write_tmpl` directly: output contains exactly one `{{ template "base" .
  }}` action, zero `{{` inside any comment, and the `sandbawks-runsc` block
  under `io.containerd.grpc.v1.cri` when the v2 key is selected.

## Operator steps

1. Recover the crash-looping node (reverts to k3s's default containerd config):

   ```bash
   sudo rm -f /var/lib/rancher/k3s/agent/etc/containerd/config.toml.tmpl
   sudo systemctl restart k3s-agent
   ```

   This also resets `config.toml` to a clean `version = 2` baseline, so the
   re-run detects the plugin key correctly.

2. Re-run the fixed `install-runsc-k3s.sh` (from this branch until #253 merges
   and publishes it to `https://raw.githubusercontent.com/hypotheosis/sandbawks/main/scripts/install-runsc-k3s.sh`).

## Files touched

- `scripts/install-runsc-k3s.sh`
