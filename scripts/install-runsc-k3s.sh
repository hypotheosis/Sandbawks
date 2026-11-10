#!/usr/bin/env bash
# Install gVisor (`runsc`) on a k3s node and register it — together with the
# sandbawks custom shim — as CRI runtime handlers in k3s's embedded
# containerd. Run this once per node (`sudo`), on every node that will run
# sandbox Pods, as a node-provisioning step (alongside installing a CNI or a
# container runtime). It is self-contained: it depends on nothing already
# running on the node — in particular NOT on the `sandbawks-node` DaemonSet,
# whose image isn't guaranteed present on a freshly-joined node.
#
# Why this is needed: the Helm chart ships two RuntimeClasses —
#   - `gvisor`         -> handler `runsc`          (from the gVisor package)
#   - `sandbawks-runsc`-> handler `sandbawks-runsc`(the containerd-shim-sandbawks-v1)
# but k3s's embedded containerd doesn't know either handler until it's
# registered here. Without that, sandbox Pods fail to create with
# "RuntimeHandler ... not supported" / "failed to find runtime". The
# DaemonSet pod itself runs as a plain runc pod (no runtimeClassName), so it
# does not need either handler to start — this script runs independently of it.
#
# The shim binary is fetched from a published, versioned artifact (a GitHub
# Release asset of this repository by default) and sha512-verified —
# mirroring how the node/kind Dockerfiles fetch `runsc` by URL. It is NOT
# scavenged from a locally-pulled image, which would race the DaemonSet's async
# image pull on a fresh node.
#
# Idempotent: a runsc already installed at the pinned version is left alone;
# an already-installed shim of the same checksum is left untouched; the
# containerd config template is rewritten in place each run; k3s is restarted
# only when something changed. Every run also removes the legacy gVisor *apt*
# source/package left by pre-#522 nodes, which a host `apt-get upgrade` would
# otherwise bump off the pin (see purge_legacy_apt_runsc / issue #455).
#
# Usage:
#   sudo ./scripts/install-runsc-k3s.sh
#   sudo ./scripts/install-runsc-k3s.sh --shim-version sha-1a2b3c4d
#   sudo ./scripts/install-runsc-k3s.sh --skip-sandbawks-runsc   # gVisor only
#
# Flags:
#   --runsc-version <ver>      gVisor release version to fetch (default: env
#                              SANDBAWKS_RUNSC_VERSION or "20260525.0").
#   --shim-version <ver>       Shim artifact version to fetch (default: env
#                              SANDBAWKS_SHIM_VERSION or "latest").
#   --shim-url <url>           Full URL to the shim binary, overriding the
#                              derived release URL. Its checksum is expected at
#                              "<url>.sha512" (best-effort).
#   --require-sandbawks-runsc  Make a failed shim fetch fatal instead of
#                              skipping the sandbawks-runsc handler.
#   --skip-sandbawks-runsc     Register only `runsc`; never fetch the shim.
#   -h | --help                Show this help.
#
# Env:
#   SANDBAWKS_RUNSC_VERSION    Default gVisor release version when
#                              --runsc-version is omitted.
#   SANDBAWKS_RUNSC_BASE_URL   Base URL for gVisor release binaries.
#   SANDBAWKS_SHIM_BASE_URL    Download base for the shim (see the layout
#                              below); point it at a mirror to self-host.
#   SANDBAWKS_SHIM_VERSION     Default version when --shim-version is omitted.
#   SANDBAWKS_SHIM_TOKEN       Optional bearer token for a private mirror; sent
#                              as "Authorization: token <tok>" on fetch. The
#                              default public release needs no token.
#   SANDBAWKS_K3S_UNIT         systemd unit to restart so containerd reloads the
#                              new config (e.g. k3s.service or k3s-agent.service).
#                              Default: auto-detect (k3s-agent.service if active,
#                              else k3s.service).
set -euo pipefail

# gVisor binary release base URL and pinned default version. The version is
# exposed as an environment variable / flag so bumps are deliberate and
# reviewable; the default is kept in lockstep with
# crates/sandbawks-node/Dockerfile and ci/kind-node/Dockerfile so the host
# runsc, the sandbawks-node DaemonSet runsc, and the kind-node runsc match.
# Releases are published at <base>/<version>/<arch>/{runsc,containerd-shim-runsc-v1}.
runsc_base_url="${SANDBAWKS_RUNSC_BASE_URL:-https://storage.googleapis.com/gvisor/releases/release}"
runsc_version="${SANDBAWKS_RUNSC_VERSION:-20260525.0}"

# GitHub Releases of this repository, downloadable anonymously over HTTPS once
# the repository is public. The matching uploader is
# .github/workflows/shim-publish.yaml, which tags each release shim-<version>.
# Layout:
#   <base>/shim-<version>/containerd-shim-sandbawks-v1[.sha512]
shim_base_url="${SANDBAWKS_SHIM_BASE_URL:-https://github.com/hypotheosis/sandbawks/releases/download}"
shim_version="${SANDBAWKS_SHIM_VERSION:-latest}"
shim_url=""
require_sandbawks_runsc=0
skip_sandbawks_runsc=0

while [[ $# -gt 0 ]]; do
    case "$1" in
        --runsc-version) runsc_version="${2:?--runsc-version needs a value}"; shift 2 ;;
        --shim-version)  shim_version="${2:?--shim-version needs a value}";  shift 2 ;;
        --shim-url)      shim_url="${2:?--shim-url needs a value}";          shift 2 ;;
        --require-sandbawks-runsc) require_sandbawks_runsc=1; shift ;;
        --skip-sandbawks-runsc)    skip_sandbawks_runsc=1;    shift ;;
        -h|--help)
            sed -n '2,/^set -euo pipefail/{/^set -euo pipefail/!p;}' "$0"
            exit 0
            ;;
        *)
            echo "error: unknown argument '$1' (try --help)" >&2
            exit 2
            ;;
    esac
done

[[ "$require_sandbawks_runsc" -eq 1 && "$skip_sandbawks_runsc" -eq 1 ]] \
    && { echo "error: --require-sandbawks-runsc and --skip-sandbawks-runsc are mutually exclusive" >&2; exit 2; }

log()  { echo "[install-runsc-k3s] $*"; }
fail() { echo "[install-runsc-k3s] error: $*" >&2; exit 1; }

# curl is used both by runsc install and by shim fetch; ensure it is present
# on minimal node images before either path runs. apt-get is idempotent, so
# re-running this on an already-provisioned node is a cheap no-op.
ensure_curl() {
    command -v curl >/dev/null 2>&1 && return 0
    export DEBIAN_FRONTEND=noninteractive
    apt-get update -qq
    apt-get install -y -qq ca-certificates curl
}

# --- Preconditions -----------------------------------------------------------

[[ "$(id -u)" -eq 0 ]] || fail "must run as root (use sudo)"

arch="$(uname -m)"
case "$arch" in
    x86_64|aarch64) ;;
    *) fail "gVisor supports only x86_64 and aarch64; this node is '$arch'" ;;
esac

command -v k3s >/dev/null 2>&1 || fail "k3s not found on PATH — is this a k3s node?"

ensure_curl

# k3s renders its containerd config from this template (if it exists) on every
# (re)start. We add runtime handlers by appending to k3s's own base config via
# the `{{ template "base" . }}` include, so we inherit k3s's defaults verbatim.
k3s_etc="/var/lib/rancher/k3s/agent/etc/containerd"
config_toml="${k3s_etc}/config.toml"
config_tmpl="${k3s_etc}/config.toml.tmpl"
# gVisor shim config, referenced by the runsc/sandbawks-runsc handlers' ConfigPath.
# Its [runsc_config] keys become runsc CLI flags; `systemd-cgroup` is what makes
# runsc nest the sandbox cgroup under the kubelet's limited pod slice (#448).
runsc_shim_toml="${k3s_etc}/runsc.toml"

# Write the gVisor shim config that carries `--systemd-cgroup=true` to runsc.
# Must exist before containerd loads a handler whose ConfigPath points at it.
write_runsc_shim_config() {
    cat > "$runsc_shim_toml" <<'EOF'
# Managed by scripts/install-runsc-k3s.sh — re-run that script to regenerate.
# The gVisor containerd shim maps each [runsc_config] key to a `runsc --<key>`
# flag. `systemd-cgroup = "true"` makes runsc treat the kubelet's systemd-form
# cgroupsPath as a systemd slice and nest the sandbox cgroup under the limited
# kubepods.slice pod cgroup, so memory/cpu/pids limits actually bind (#448).
[runsc_config]
  systemd-cgroup = "true"
EOF
    log "wrote gVisor shim config ${runsc_shim_toml} (systemd-cgroup=true)"
}

# --- Install the runsc binary + its shim -------------------------------------

# Remove the legacy gVisor apt footprint that pre-#522 versions of this script
# left on the host (issue #455 follow-up). That installer registered a *rolling*
# channel at /etc/apt/sources.list.d/gvisor.list ("...releases release main")
# and ran `apt-get install runsc`, so the apt-managed /usr/bin/runsc gets
# silently bumped to whatever gVisor published most recently on the next host
# `apt-get upgrade` (apt-daily timer / unattended-upgrades / an ops run) — off
# our pin, with no review. Now that we install a pinned, checksummed binary to
# /usr/local/bin, the apt source + package are pure drift risk, so drop both.
#
# This runs every invocation, BEFORE the version check below: the stale
# /usr/bin/runsc is typically shadowed on PATH by the pinned /usr/local/bin
# copy, so `runsc --version` may report "already pinned" while the apt package
# still sits underneath, armed for the next upgrade. Idempotent — a clean no-op
# on nodes that never had the apt install (or that lack apt entirely).
purge_legacy_apt_runsc() {
    local removed=0
    local f
    for f in /etc/apt/sources.list.d/gvisor.list \
             /usr/share/keyrings/gvisor-archive-keyring.gpg; do
        [[ -e "$f" ]] || continue
        # Tolerate a failing rm (e.g. immutable file / read-only mount) the same
        # way the apt-get purge below is tolerated: warn, but never abort a node
        # provision under `set -e` over best-effort cleanup. `if rm` puts the
        # command in a condition, so its non-zero exit doesn't trip `set -e`.
        if rm -f "$f"; then
            removed=1
        else
            log "warning: could not remove ${f} (left in place)"
        fi
    done
    # Purge whenever dpkg still tracks runsc in ANY state other than fully
    # absent — `install ok installed` (present) OR `deinstall ok config-files`
    # (removed-but-not-purged, the `rc` state, which leaves stale config behind).
    # Guard on dpkg-query so this no-ops on non-dpkg hosts instead of erroring.
    if command -v dpkg-query >/dev/null 2>&1 \
        && dpkg-query -W -f='${Status}' runsc 2>/dev/null | grep -qE 'ok (installed|config-files)'; then
        log "purging apt-managed 'runsc' (superseded by pinned /usr/local/bin binary)"
        # Capture combined output so a failure (dpkg lock, held package, ...) is
        # surfaced in the log instead of silently swallowed — still non-blocking.
        local purge_out
        if purge_out="$(DEBIAN_FRONTEND=noninteractive apt-get purge -y -qq runsc 2>&1)"; then
            removed=1
        else
            log "warning: 'apt-get purge runsc' failed (continuing): ${purge_out}"
        fi
    fi
    [[ "$removed" -eq 1 ]] \
        && log "removed legacy gVisor apt source/package (was the rolling 'release' channel)"
    return 0
}

# Download a pinned gVisor release directly from GCS, sha512-verify it, and
# install runsc + containerd-shim-runsc-v1 onto /usr/local/bin. This mirrors the
# pattern already used for the sandbawks shim (fetch_shim) and the node/kind
# Dockerfiles, and avoids silent version drift from the rolling apt `release`
# channel (issue #455).
install_runsc() {
    local expected_release="release-${runsc_version}"

    # Strip the rolling-apt drift vector first, regardless of what's installed.
    purge_legacy_apt_runsc

    if command -v runsc >/dev/null 2>&1; then
        local current_version
        current_version="$(runsc --version 2>&1 | head -1)"
        if echo "$current_version" | grep -qF "$expected_release"; then
            log "runsc already at pinned version (${expected_release}): ${current_version}"
            return 0
        fi
        log "runsc version mismatch (want ${expected_release}, have ${current_version}) — reinstalling"
    fi

    local tmp; tmp="$(mktemp -d)"
    # shellcheck disable=SC2064  # expand $tmp now, not at trap time
    trap "rm -rf '$tmp'" RETURN

    local base="${runsc_base_url}/${runsc_version}/${arch}"
    log "fetching gVisor ${expected_release} for ${arch} from ${runsc_base_url}"
    for bin in runsc containerd-shim-runsc-v1; do
        curl -fsSL -o "${tmp}/${bin}"        "${base}/${bin}" \
            || fail "failed to download ${bin} (${base}/${bin})"
        curl -fsSL -o "${tmp}/${bin}.sha512" "${base}/${bin}.sha512" \
            || fail "failed to download ${bin}.sha512"
        (cd "$tmp" && sha512sum -c "${bin}.sha512") \
            || fail "sha512 verification failed for ${bin}"
        install -m 0755 "${tmp}/${bin}" "/usr/local/bin/${bin}" \
            || fail "failed to install ${bin} to /usr/local/bin"
    done

    log "installed: $(runsc --version 2>&1 | head -1)"
}

install_runsc

command -v containerd-shim-runsc-v1 >/dev/null 2>&1 \
    || fail "containerd-shim-runsc-v1 not on PATH after install"

# --- CRI plugin key: resolve from the *rendered base*, not the standalone file

# Our handler tables MUST live under the same containerd CRI plugin key that
# `{{ template "base" . }}` actually emits. If they don't, containerd mixes a
# v2 base (`io.containerd.grpc.v1.cri`) with v3 handlers
# (`io.containerd.cri.v1.runtime`) in one file and silently corrupts the
# *default* `runc` runtime: runc is launched with `--systemd-cgroup` but its
# CRI cgroup path is never converted to systemd `slice:prefix:name` form, so
# every ordinary (no-runtimeClassName) Pod on the node dies at create with
#   expected cgroupsPath to be of format "slice:prefix:name" ... got "/kubepods/..."
#
# The trap is that the two keys disagree depending on how you look: on
# k3s 1.32 + containerd 2.x the *standalone* config.toml k3s generates is
# `version = 3` (`io.containerd.cri.v1.runtime`), but the `base` template
# include renders `version = 2` (`io.containerd.grpc.v1.cri`). Keying off the
# standalone file (as a prior version of this script did) therefore picks the
# WRONG key for the base we actually include. So we render the base ALONE
# first (see the base-only `write_tmpl` pass below) and read the key from that
# — never from the standalone file, and never from our own appended blocks
# (which would self-confirm a wrong key).
detect_cri_plugin() {
    # Inspect the base's own `runc` runtime table in the freshly-rendered
    # config. Default to the v2 key, which is what `{{ template "base" . }}`
    # emits today; this still tracks a future k3s that flips the base to v3.
    if grep -qE '^\[plugins\."io\.containerd\.cri\.v1\.runtime"\.containerd\.runtimes\.runc\]' \
        "$config_toml"; then
        echo "io.containerd.cri.v1.runtime"
    else
        echo "io.containerd.grpc.v1.cri"
    fi
}
cri_plugin=""  # resolved after the base-only render, just before we add handlers

# --- Fetch + install the sandbawks shim, then decide on its handler ----------

# k3s's containerd resolves shim binaries on its own service PATH, which
# includes /usr/local/bin; install there to sit next to the gVisor shim.
shim_dest="/usr/local/bin/containerd-shim-sandbawks-v1"

# Download the published static shim and verify it against the sibling
# .sha512 before installing. Returns non-zero on any fetch/verify failure so
# the caller can decide whether that's fatal (--require-sandbawks-runsc).
fetch_shim() {
    [[ -n "$shim_url" ]] || shim_url="${shim_base_url}/shim-${shim_version}/containerd-shim-sandbawks-v1"

    # The default public release downloads anonymously; a private mirror may
    # need a token. Build the curl auth args as an array so an unset token
    # contributes no empty positional argument.
    local auth=()
    [[ -n "${SANDBAWKS_SHIM_TOKEN:-}" ]] \
        && auth=(-H "Authorization: token ${SANDBAWKS_SHIM_TOKEN}")

    local tmp; tmp="$(mktemp -d)"
    # shellcheck disable=SC2064  # expand $tmp now, not at trap time
    trap "rm -rf '$tmp'" RETURN

    log "fetching shim: ${shim_url}"
    curl -fsSL "${auth[@]}" -o "${tmp}/shim" "$shim_url" \
        || { log "shim download failed"; return 1; }

    if curl -fsSL "${auth[@]}" -o "${tmp}/shim.sha512" "${shim_url}.sha512" 2>/dev/null; then
        # Compare hashes directly rather than `sha512sum -c` so we don't depend
        # on the filename recorded in the .sha512 matching our temp name.
        local expected actual
        expected="$(awk '{print $1}' "${tmp}/shim.sha512")"
        actual="$(sha512sum "${tmp}/shim" | awk '{print $1}')"
        [[ "$expected" == "$actual" ]] \
            || { log "shim checksum mismatch (expected ${expected}, got ${actual})"; return 1; }
        log "shim sha512 verified"
    else
        log "warning: no .sha512 published alongside shim — installing unverified"
    fi

    install -m 0755 "${tmp}/shim" "$shim_dest"
    log "installed ${shim_dest}: $("$shim_dest" --version 2>/dev/null | head -1 || echo 'version n/a')"
}

register_sandbawks=0
if [[ "$skip_sandbawks_runsc" -eq 1 ]]; then
    log "--skip-sandbawks-runsc given — registering only the 'runsc' handler"
elif command -v containerd-shim-sandbawks-v1 >/dev/null 2>&1 && [[ -z "$shim_url" && "$shim_version" == "latest" ]]; then
    # Already present and the caller didn't pin a specific version: keep it.
    register_sandbawks=1
    log "containerd-shim-sandbawks-v1 already on PATH — will register 'sandbawks-runsc'"
elif fetch_shim; then
    register_sandbawks=1
    log "will register 'sandbawks-runsc' handler"
elif [[ "$require_sandbawks_runsc" -eq 1 ]]; then
    fail "could not obtain containerd-shim-sandbawks-v1 and --require-sandbawks-runsc was given"
else
    log "could not obtain the shim — skipping 'sandbawks-runsc' (only 'runsc' will be registered)"
    log "  re-run with --shim-version/--shim-url (or SANDBAWKS_SHIM_TOKEN for a private mirror)"
fi

# --- Render the containerd config template -----------------------------------

# `runsc` is intentionally NOT written here on first pass: modern k3s (v1.30+)
# auto-detects an installed runsc binary and adds the handler itself, and a
# second identical TOML table would be a duplicate-key parse error. We append
# `runsc` explicitly only as a fallback below if k3s didn't auto-register it.
#
# The heredoc delimiter is quoted ('EOF') so the shell leaves the Go template
# directive `{{ template "base" . }}` untouched for k3s to render.
write_tmpl() {
    local include_runsc="$1"
    # base_only=1 emits ONLY the base include, with no handler tables. Used for
    # the first render pass so detect_cri_plugin can read the key the base uses
    # off a clean file (no appended blocks to self-confirm a wrong key).
    local base_only="${2:-0}"
    {
        cat <<'EOF'
# Managed by scripts/install-runsc-k3s.sh — re-run that script to regenerate.
# The include directive below pulls in k3s's own generated containerd config,
# so we inherit every k3s default and only *add* runtime handlers.
# (Do NOT write a Go-template action like the include inside a comment: k3s
# renders this file with text/template, which has no notion of a TOML comment
# and would expand it a second time mid-comment, producing invalid TOML.)
{{ template "base" . }}
EOF
        if [[ "$base_only" -eq 1 ]]; then
            : # base include only; handler tables are added on the next pass
        elif [[ "$include_runsc" -eq 1 ]]; then
            cat <<EOF

[plugins."${cri_plugin}".containerd.runtimes.runsc]
  runtime_type = "io.containerd.runsc.v1"

[plugins."${cri_plugin}".containerd.runtimes.runsc.options]
  # gVisor's shim must run runsc with --systemd-cgroup, matching k3s's kubelet
  # (which addresses pod cgroups as systemd slices). Without it, runsc treats the
  # systemd-form cgroupsPath ("slice:prefix:name") as a literal cgroupfs path and
  # creates the sandbox cgroup under system.slice instead of nesting it under the
  # kubelet's limited kubepods.slice pod cgroup — so NO memory/cpu/pids limit
  # binds the sandbox (issue #448; root cause #446). NOTE: the runc-style
  # \`SystemdCgroup = true\` key is IGNORED by the gVisor shim; the flag must be
  # delivered via a runsc config file's [runsc_config] section (see runsc.toml,
  # written below) referenced by ConfigPath. Verified on-node: with this, the
  # sandbox nests under kubepods.slice and is OOM-killed / CPU-throttled at its cap.
  TypeUrl = "io.containerd.runsc.v1.options"
  ConfigPath = "${runsc_shim_toml}"
EOF
        fi
        if [[ "$base_only" -ne 1 && "$register_sandbawks" -eq 1 ]]; then
            # Handler name is `sandbawks-runsc` (matches the RuntimeClass), but
            # runtime_type is `io.containerd.sandbawks.v1` — containerd maps that
            # to the binary `containerd-shim-sandbawks-v1`. `pod_annotations`
            # allowlists the restore-path annotation so the shim actually sees it.
            cat <<EOF

[plugins."${cri_plugin}".containerd.runtimes.sandbawks-runsc]
  runtime_type = "io.containerd.sandbawks.v1"
  pod_annotations = ["io.sandbawks/restore-image-path"]

[plugins."${cri_plugin}".containerd.runtimes.sandbawks-runsc.options]
  # Same --systemd-cgroup requirement as the \`runsc\` handler above (#448). The
  # sandbawks shim wraps runsc and accepts the runsc options proto + ConfigPath
  # (verified on-node: containerd starts, a sandbawks-runsc sandbox nests under
  # kubepods.slice with its limits applied).
  TypeUrl = "io.containerd.runsc.v1.options"
  ConfigPath = "${runsc_shim_toml}"
EOF
        fi
    } > "$config_tmpl"
}

# --- Restart k3s + verify ----------------------------------------------------

# The systemd unit to restart so k3s re-renders config.toml and its embedded
# containerd reloads the new handlers. Overridable via SANDBAWKS_K3S_UNIT for
# nodes whose k3s ships under a non-standard unit name; otherwise auto-detect
# server vs agent (the embedded agent runs as k3s-agent.service).
k3s_service="${SANDBAWKS_K3S_UNIT:-}"
if [[ -z "$k3s_service" ]]; then
    k3s_service="k3s.service"
    systemctl is-active --quiet k3s-agent.service 2>/dev/null && k3s_service="k3s-agent.service"
fi

restart_k3s() {
    log "restarting ${k3s_service}"
    systemctl restart "$k3s_service"
    # containerd comes up a beat after the service; poll crictl rather than sleep.
    for _ in $(seq 1 30); do
        k3s crictl info >/dev/null 2>&1 && return 0
        sleep 1
    done
    fail "containerd did not become ready within 30s of restarting ${k3s_service}"
}

handler_present() {
    # crictl `info` reports registered runtimes in its JSON config; grep is
    # enough and avoids a hard dependency on jq being installed.
    k3s crictl info 2>/dev/null | grep -q "\"$1\""
}

# Pass 0: render the base ALONE and restart, so the on-disk config.toml is the
# pure k3s base. detect_cri_plugin then reads which CRI plugin key that base
# uses, and we write every handler under that same key — preventing the
# v2-base / v3-handler mix that corrupts the default runc runtime.
log "rendering base-only config to detect the CRI plugin key"
write_tmpl 0 1
restart_k3s
cri_plugin="$(detect_cri_plugin)"
log "containerd CRI plugin key (from rendered base): ${cri_plugin}"

log "writing ${config_tmpl} with runtime handlers (runsc via k3s auto-detect)"
write_runsc_shim_config   # ConfigPath target must exist before containerd loads it
write_tmpl 0
restart_k3s

if ! handler_present runsc; then
    log "k3s did not auto-register 'runsc' — appending an explicit handler block"
    write_tmpl 1
    restart_k3s
fi

# --- Report ------------------------------------------------------------------

if handler_present runsc; then
    log "OK: 'runsc' handler registered (RuntimeClass 'gvisor')"
else
    fail "'runsc' handler still missing after explicit registration — check 'k3s crictl info'"
fi

if [[ "$register_sandbawks" -eq 1 ]]; then
    if handler_present sandbawks-runsc; then
        log "OK: 'sandbawks-runsc' handler registered (RuntimeClass 'sandbawks-runsc')"
    else
        fail "'sandbawks-runsc' handler missing — verify containerd-shim-sandbawks-v1 is on the k3s service PATH"
    fi
fi

log "done. Sanity check: kubectl get runtimeclass; kubectl run gv --rm -it \\"
log "  --image=busybox --overrides='{\"spec\":{\"runtimeClassName\":\"gvisor\"}}' -- dmesg | grep -i gvisor"
