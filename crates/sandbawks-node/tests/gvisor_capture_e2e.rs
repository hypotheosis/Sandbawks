//! Live-gVisor acceptance test for capture-in-checkpoint envd (issue #470,
//! epic #466).
//!
//! This is the make-or-break end-to-end proof that an envd which has
//! self-relocated onto the checkpoint-captured overlay (`/var/tmp/envd/run/envd`,
//! the `SANDBAWKS_ENVD_RELOCATE_TARGET` contract from #467/#468) survives a
//! `runsc checkpoint` / `runsc restore` round-trip **with its code pages served
//! from `pages.img`**, and that the node-staged host binary is genuinely
//! irrelevant at restore. Without this proof the whole epic rests on an
//! unverified gVisor assumption.
//!
//! ## What it does
//!
//! 1. Builds two envd binaries with distinct compile-time build ids (`BUILD_ID`,
//!    surfaced by `GET /__version`): an `OLD` seed and a `NEW` replacement.
//! 2. Assembles a minimal OCI bundle whose entrypoint is the `OLD` seed at
//!    `/usr/local/bin/envd`, with the relocate env set, and cold-starts it under
//!    `runsc` so envd self-relocates onto `/var/tmp/envd/run/envd` and serves.
//! 3. Asserts the running sandbox reports the `OLD` id over `/__version`.
//! 4. `runsc checkpoint`s the sandbox.
//! 5. **Replaces the on-disk host-staged seed binary with the `NEW` build** —
//!    the standalone analogue of swapping `/var/lib/sandbawks/envd/envd` on the
//!    node between checkpoint and restore.
//! 6. `runsc restore`s and asserts the sandbox still serves and **still reports
//!    the `OLD` id** — proving the restored code pages came from `pages.img`,
//!    not the swapped on-disk binary.
//!
//! ## Gating
//!
//! Live gVisor is not available in ordinary CI, so this test is **opt-in**: it
//! no-ops (printing why) unless `SANDBAWKS_GVISOR_E2E` is set in the
//! environment *and* every host prerequisite is present (`runsc`, `ip`,
//! `busybox`, and either root or passwordless `sudo`). This mirrors how the
//! repo's other live-gVisor coverage is exercised out-of-band (the
//! `pause-resume-smoke` / kind+gVisor flows) rather than on every `cargo test`.
//! Run it deliberately with:
//!
//! ```text
//! SANDBAWKS_GVISOR_E2E=1 cargo test -p sandbawks-node --test gvisor_capture_e2e -- --nocapture
//! ```
//!
//! The two envd binaries are built automatically; override with
//! `SANDBAWKS_ENVD_OLD_BIN` / `SANDBAWKS_ENVD_NEW_BIN` to point at pre-built
//! ones (the build ids must differ).
//!
//! Caveat: when built (not overridden), this test shells out to `cargo build -p
//! envd` while itself running under cargo. A nested cargo invocation can block
//! on the target-directory build lock if the outer process is still building;
//! the opt-in, single-threaded path this test runs under avoids that in
//! practice, but prefer the `SANDBAWKS_ENVD_*_BIN` overrides (e.g. from a `just`
//! recipe that builds both first) to sidestep it entirely.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

// The relocate contract is owned by `sandbawks-types` (a dev-dependency of this
// crate); import it rather than re-declaring the literals so the test cannot
// drift from the controller/envd value it is meant to exercise.
use sandbawks_types::{ENVD_RELOCATE_TARGET, ENVD_RELOCATE_TARGET_ENV, ENVD_RUN_DIR};

/// Env var that opts this test in. Any value (even empty) enables it.
const OPT_IN_ENV: &str = "SANDBAWKS_GVISOR_E2E";
/// Optional overrides pointing at pre-built envd binaries.
const OLD_BIN_ENV: &str = "SANDBAWKS_ENVD_OLD_BIN";
const NEW_BIN_ENV: &str = "SANDBAWKS_ENVD_NEW_BIN";

/// Distinct compile-time build ids baked into the two envd binaries via
/// `SANDBAWKS_ENVD_BUILD_ID` (read by `envd`'s `option_env!`-derived
/// `BUILD_ID`). The whole test turns on telling these two apart over
/// `/__version`.
const OLD_ID: &str = "envd-capture-e2e-old";
const NEW_ID: &str = "envd-capture-e2e-new";

/// envd's HTTP listener port (`/__version`, `/__state`). A test-local choice
/// (envd's own default port constants are private to its binary), bound on the
/// wildcard so the sandbox netstack accepts it.
const HTTP_PORT: u16 = 49984;
/// envd's gRPC listener port.
const GRPC_PORT: u16 = 49983;

/// Skip helper: print a reason and return `None` so the test no-ops cleanly.
fn skip(reason: &str) -> Option<()> {
    println!("SKIP gvisor_capture_e2e: {reason}");
    None
}

/// Whether this process is already root (no `sudo` needed).
fn is_root() -> bool {
    Command::new("id")
        .arg("-u")
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map(|o| String::from_utf8_lossy(&o.stdout).trim() == "0")
        .unwrap_or(false)
}

/// Whether a binary is resolvable on `PATH`.
fn have(bin: &str) -> bool {
    Command::new("sh")
        .arg("-c")
        .arg(format!("command -v {bin} >/dev/null 2>&1"))
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

/// Resolve every prerequisite, returning a `sudo` prefix (`[]` when root, or
/// `["sudo","-n"]` otherwise) or a skip reason via `None`.
fn preflight() -> Option<Vec<String>> {
    if std::env::var_os(OPT_IN_ENV).is_none() {
        return skip(&format!("{OPT_IN_ENV} not set (opt-in live-gVisor test)"))
            .map(|_| Vec::new());
    }
    for bin in ["runsc", "ip", "busybox", "timeout"] {
        if !have(bin) {
            return skip(&format!("`{bin}` not found on PATH")).map(|_| Vec::new());
        }
    }
    if is_root() {
        return Some(Vec::new());
    }
    // Need passwordless sudo: `sudo -n true` must succeed.
    let ok = Command::new("sudo")
        .args(["-n", "true"])
        .status()
        .map(|s| s.success())
        .unwrap_or(false);
    if !ok {
        return skip("not root and passwordless `sudo -n` unavailable").map(|_| Vec::new());
    }
    Some(vec!["sudo".to_owned(), "-n".to_owned()])
}

/// Default wall-clock cap (seconds) for any single command. Bounds an
/// occasional `runsc` stall so a flake surfaces as a fast, legible failure
/// rather than a multi-minute hang of the whole test.
const DEFAULT_TIMEOUT_SECS: u64 = 45;

/// Run a command with the default per-command timeout. See [`run_to`].
fn run(prefix: &[String], args: &[&str]) -> (bool, String, String) {
    run_to(prefix, DEFAULT_TIMEOUT_SECS, args)
}

/// Run a command (privilege prefix + `timeout <secs>`), returning
/// `(success, stdout, stderr)`. Every external command is wrapped in
/// `timeout(1)` so no single `runsc`/`ip` invocation can block the test
/// indefinitely; a timed-out command exits non-zero (code 124), which callers
/// treat as a failure they can retry.
fn run_to(prefix: &[String], secs: u64, args: &[&str]) -> (bool, String, String) {
    // `--kill-after=5`: if the initial SIGTERM is ignored (a wedged `runsc`
    // mid-syscall, or a `sudo` that does not forward it), escalate to SIGKILL
    // 5s later so a single stuck command cannot wedge the whole test.
    let mut full: Vec<String> = vec![
        "timeout".to_owned(),
        "--kill-after=5".to_owned(),
        secs.to_string(),
    ];
    full.extend(prefix.iter().cloned());
    full.extend(args.iter().map(|s| s.to_string()));
    let out = Command::new(&full[0])
        .args(&full[1..])
        .output()
        .unwrap_or_else(|e| panic!("spawn {:?}: {e}", full));
    (
        out.status.success(),
        String::from_utf8_lossy(&out.stdout).into_owned(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

/// Run a *detached* command (`runsc run`/`restore --detach`), returning whether
/// it exited successfully.
///
/// Crucially this does **not** capture stdout/stderr through pipes: a detached
/// `runsc` sandbox inherits the spawning process's stdio fds and keeps them
/// open for its whole lifetime, so a pipe-capturing `Command::output()` would
/// block forever waiting for an EOF that never arrives (even after the
/// immediate child exits). We send the child's stdout to `/dev/null`, let its
/// stderr inherit ours (visible under `--nocapture`, never read by us), and
/// wait only for the immediate child via `status()`.
fn run_detached(prefix: &[String], secs: u64, args: &[&str]) -> bool {
    let mut full: Vec<String> = vec![
        "timeout".to_owned(),
        "--kill-after=5".to_owned(),
        secs.to_string(),
    ];
    full.extend(prefix.iter().cloned());
    full.extend(args.iter().map(|s| s.to_string()));
    Command::new(&full[0])
        .args(&full[1..])
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::inherit())
        .status()
        .unwrap_or_else(|e| panic!("spawn {:?}: {e}", full))
        .success()
}

/// Locate the cargo workspace root from this crate's manifest dir
/// (`<root>/crates/sandbawks-node`).
fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("workspace root is two levels above the node crate")
        .to_path_buf()
}

/// Build (or locate, via the override env) an envd binary with `build_id` baked
/// into its `BUILD_ID`, copied to `dest`. Builds into the shared workspace
/// target so dependency artifacts are reused; `build.rs`'s
/// `rerun-if-env-changed=SANDBAWKS_ENVD_BUILD_ID` guarantees the bin is
/// recompiled when the id changes.
fn build_envd(build_id: &str, override_env: &str, dest: &Path) {
    if let Some(path) = std::env::var_os(override_env) {
        std::fs::copy(&path, dest)
            .unwrap_or_else(|e| panic!("copy override {override_env}={path:?}: {e}"));
        return;
    }
    let root = workspace_root();
    let status = Command::new(std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into()))
        .current_dir(&root)
        .args(["build", "-p", "envd", "--bin", "envd"])
        .env("SANDBAWKS_ENVD_BUILD_ID", build_id)
        .status()
        .unwrap_or_else(|e| panic!("spawn cargo build for envd ({build_id}): {e}"));
    assert!(status.success(), "cargo build envd ({build_id}) failed");
    let built = root.join("target/debug/envd");
    std::fs::copy(&built, dest)
        .unwrap_or_else(|e| panic!("copy {} -> {}: {e}", built.display(), dest.display()));
}

/// Copy `src` to `dst`, creating parents and preserving the executable bit.
fn install(src: &Path, dst: &Path) {
    if let Some(parent) = dst.parent() {
        std::fs::create_dir_all(parent).expect("mkdir -p");
    }
    std::fs::copy(src, dst)
        .unwrap_or_else(|e| panic!("copy {} -> {}: {e}", src.display(), dst.display()));
    let mut perms = std::fs::metadata(dst).expect("stat").permissions();
    use std::os::unix::fs::PermissionsExt;
    perms.set_mode(0o755);
    std::fs::set_permissions(dst, perms).expect("chmod 0755");
}

/// Copy a dynamically-linked binary's shared-library dependencies (and the
/// dynamic linker) into `rootfs`, preserving their absolute paths — so the
/// binary loads inside the otherwise-empty sandbox rootfs. Robust across hosts
/// because it parses the real `ldd` output rather than hardcoding lib paths.
fn copy_ldd_deps(binary: &Path, rootfs: &Path) {
    let out = Command::new("ldd").arg(binary).output().expect("run ldd");
    let text = String::from_utf8_lossy(&out.stdout);
    for line in text.lines() {
        // Two shapes: "libfoo.so => /abs/path (0x...)" and "/abs/ld-linux.so (0x...)".
        let path = if let Some((_, rhs)) = line.split_once("=> ") {
            rhs.split_whitespace().next()
        } else {
            line.split_whitespace().next()
        };
        let Some(path) = path else { continue };
        if !path.starts_with('/') {
            continue; // e.g. linux-vdso.so.1 (kernel-provided, no file)
        }
        let src = Path::new(path);
        if !src.exists() {
            continue;
        }
        let dst = rootfs.join(path.trim_start_matches('/'));
        install(src, &dst);
    }
}

/// Lay out a minimal OCI rootfs: the `OLD` envd seed, busybox (+ applet
/// symlinks for the in-sandbox probe), the seed's shared libs, and the
/// writable directories envd needs. envd relocates onto
/// `/var/tmp/envd/run/envd` and writes its state under `/var/tmp/envd`, so we
/// create `var/tmp/envd` world-writable and let envd `mkdir -p` its own `run/`
/// relocate subdir there — exactly as it does in production (#656).
fn build_rootfs(rootfs: &Path, old_envd: &Path) {
    for dir in [
        "usr/local/bin",
        "bin",
        "var/tmp/envd",
        "tmp",
        "proc",
        "sys",
        "dev",
        "etc",
    ] {
        std::fs::create_dir_all(rootfs.join(dir)).expect("mkdir rootfs dir");
    }
    // Seed envd at the "host-staged" path that the entrypoint execs.
    install(old_envd, &rootfs.join("usr/local/bin/envd"));
    copy_ldd_deps(old_envd, rootfs);
    // busybox provides the in-sandbox `wget` probe (and a shell).
    let busybox = which("busybox");
    install(&busybox, &rootfs.join("bin/busybox"));
    use std::os::unix::fs::symlink;
    for applet in ["sh", "wget", "cat", "ls"] {
        let link = rootfs.join("bin").join(applet);
        let _ = std::fs::remove_file(&link);
        symlink("busybox", &link).expect("symlink busybox applet");
    }
    // World-writable so envd (running as the spec's uid, without
    // CAP_DAC_OVERRIDE) can create files — and its own `run/` relocate subdir —
    // under these host-uid-owned dirs.
    use std::os::unix::fs::PermissionsExt;
    for dir in ["var/tmp/envd", "tmp"] {
        std::fs::set_permissions(rootfs.join(dir), std::fs::Permissions::from_mode(0o777))
            .expect("chmod 0777");
    }
}

/// Resolve an absolute path for a `PATH` binary.
fn which(bin: &str) -> PathBuf {
    let out = Command::new("sh")
        .arg("-c")
        .arg(format!("command -v {bin}"))
        .output()
        .expect("run command -v");
    PathBuf::from(String::from_utf8_lossy(&out.stdout).trim())
}

/// Write the OCI `config.json` for the bundle.
///
/// Generated with `runsc spec` and then patched, rather than hand-rolled, so
/// the bundle carries the full default mount/capability/rlimit set runsc
/// expects (`/dev/pts`, `/dev/shm`, `/dev/mqueue`, `/sys/fs/cgroup`, …). A
/// minimal hand-written config cold-starts fine but makes `runsc restore`
/// unreliable, so we keep the spec's defaults and only override what the test
/// needs: envd as the entrypoint with the relocate env, telemetry disabled,
/// a writable rootfs, and the network namespace pointed at the pre-built netns
/// (so the sandbox netstack mirrors a working loopback).
fn write_config(bundle: &Path, rootfs: &Path, netns_path: &str) {
    let status = Command::new("runsc")
        .arg("spec")
        .current_dir(bundle)
        .status()
        .expect("run `runsc spec`");
    assert!(status.success(), "`runsc spec` failed");

    let path = bundle.join("config.json");
    let mut config: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&path).expect("read generated config.json"))
            .expect("parse generated config.json");

    // Patch via `Value`'s auto-vivifying `IndexMut<&str>` (never panics on a
    // missing key, unlike indexing a `serde_json::Map` directly).
    config["process"]["terminal"] = serde_json::json!(false);
    config["process"]["cwd"] = serde_json::json!("/");
    config["process"]["args"] = serde_json::json!([
        "/usr/local/bin/envd",
        "--listen",
        format!("0.0.0.0:{GRPC_PORT}"),
        "--http-listen",
        format!("0.0.0.0:{HTTP_PORT}"),
        "--root",
        "/tmp",
        "--state-file",
        "/var/tmp/envd/state.json",
    ]);
    config["process"]["env"] = serde_json::json!([
        "PATH=/bin:/usr/local/bin",
        "OTEL_SDK_DISABLED=1",
        "RUST_LOG=info",
        format!("{ENVD_RELOCATE_TARGET_ENV}={ENVD_RELOCATE_TARGET}"),
    ]);

    // Writable rootfs so envd can relocate onto the captured overlay-tmpfs.
    config["root"] = serde_json::json!({
        "path": rootfs.to_string_lossy(),
        "readonly": false,
    });

    // Point the network namespace at the pre-built netns (with loopback up);
    // leave every other namespace as `runsc spec` declared it.
    if let Some(namespaces) = config["linux"]["namespaces"].as_array_mut() {
        for ns in namespaces.iter_mut() {
            if ns["type"] == serde_json::json!("network") {
                ns["path"] = serde_json::json!(netns_path);
            }
        }
    }

    std::fs::write(
        &path,
        serde_json::to_vec_pretty(&config).expect("serialize config"),
    )
    .expect("write config.json");
}

/// RAII guard that tears the sandbox + netns down on drop, so a failed
/// assertion never leaks a live sandbox or a network namespace.
struct Teardown {
    prefix: Vec<String>,
    runsc_root: String,
    cid: String,
    netns: String,
}

impl Drop for Teardown {
    fn drop(&mut self) {
        let _ = run(
            &self.prefix,
            &[
                "runsc",
                "--root",
                &self.runsc_root,
                "--ignore-cgroups",
                "delete",
                "-force",
                &self.cid,
            ],
        );
        let _ = run(&self.prefix, &["ip", "netns", "delete", &self.netns]);
    }
}

/// Build the shared `runsc` global flags (`--root`, isolation toggles).
fn runsc_globals(runsc_root: &str) -> Vec<String> {
    vec![
        "runsc".into(),
        "--root".into(),
        runsc_root.into(),
        "--ignore-cgroups".into(),
        "--network".into(),
        "sandbox".into(),
        "--overlay2=root:memory".into(),
    ]
}

/// Probe `GET /__version` from *inside* the sandbox's own network namespace via
/// `runsc exec` + busybox `wget`, returning the trimmed body (empty if not yet
/// serving). Probing from within the netns sidesteps host<->sandbox networking.
fn probe_version(prefix: &[String], globals: &[String], cid: &str) -> String {
    let mut args: Vec<&str> = globals.iter().map(String::as_str).collect();
    let url = format!("http://127.0.0.1:{HTTP_PORT}/__version");
    args.extend(["exec", cid, "/bin/wget", "-qO-", &url]);
    let (_, stdout, _) = run(prefix, &args);
    stdout.trim().to_owned()
}

/// Poll `probe_version` until it returns non-empty, up to `deadline`.
fn wait_for_serve(prefix: &[String], globals: &[String], cid: &str, deadline: Duration) -> String {
    let start = Instant::now();
    loop {
        let v = probe_version(prefix, globals, cid);
        if !v.is_empty() {
            return v;
        }
        if start.elapsed() >= deadline {
            return String::new();
        }
        std::thread::sleep(Duration::from_millis(500));
    }
}

#[test]
fn capture_in_checkpoint_survives_host_binary_swap() {
    let Some(prefix) = preflight() else { return };

    let tmp = tempfile::tempdir().expect("tempdir");
    let base = tmp.path();
    let old_bin = base.join("envd-old");
    let new_bin = base.join("envd-new");
    eprintln!("building envd OLD ({OLD_ID}) and NEW ({NEW_ID})...");
    build_envd(OLD_ID, OLD_BIN_ENV, &old_bin);
    build_envd(NEW_ID, NEW_BIN_ENV, &new_bin);

    let bundle = base.join("bundle");
    let rootfs = bundle.join("rootfs");
    std::fs::create_dir_all(&rootfs).expect("mkdir bundle/rootfs");
    build_rootfs(&rootfs, &old_bin);
    eprintln!("rootfs assembled at {}", rootfs.display());

    // A network namespace with loopback up — the containerd/CNI-equivalent the
    // gVisor netstack mirrors. Unique name so concurrent runs don't collide.
    let netns = format!("gv470e2e{}", std::process::id());
    let netns_path = format!("/var/run/netns/{netns}");
    let _ = run(&prefix, &["ip", "netns", "delete", &netns]); // best-effort pre-clean
                                                              // `ip netns add` can exit non-zero with a benign "setns: Operation not
                                                              // permitted" when it tries to switch into the freshly created namespace at
                                                              // the end (blocked in some sandboxed/dev environments) — yet the namespace
                                                              // file is created and fully usable. So we ignore its exit status and gate on
                                                              // the loopback bring-up below, which is the operation we actually need.
    let _ = run(&prefix, &["ip", "netns", "add", &netns]);
    let (ok, _, err) = run(
        &prefix,
        &[
            "ip", "netns", "exec", &netns, "ip", "link", "set", "lo", "up",
        ],
    );
    assert!(
        ok,
        "bring loopback up in netns failed (netns not usable): {err}"
    );

    eprintln!("netns {netns} ready");
    write_config(&bundle, &rootfs, &netns_path);
    eprintln!("config.json written");

    let runsc_root = base.join("runsc-root").to_string_lossy().into_owned();
    std::fs::create_dir_all(&runsc_root).expect("mkdir runsc-root");
    let img = base.join("img");
    std::fs::create_dir_all(&img).expect("mkdir img");
    let cid = format!("gv470e2e-{}", std::process::id());
    let globals = runsc_globals(&runsc_root);
    let gstr: Vec<&str> = globals.iter().map(String::as_str).collect();

    // Ensure teardown even if an assertion below panics.
    let _guard = Teardown {
        prefix: prefix.clone(),
        runsc_root: runsc_root.clone(),
        cid: cid.clone(),
        netns: netns.clone(),
    };

    let bundle_s = bundle.to_string_lossy().into_owned();
    let img_s = img.to_string_lossy().into_owned();

    // 1. Cold-start the sandbox (detached); envd self-relocates onto
    //    /var/tmp/envd/run/envd and re-execs from there.
    let mut args = gstr.clone();
    args.extend(["run", "--bundle", &bundle_s, "--detach", &cid]);
    eprintln!("cold-starting sandbox {cid}...");
    let ok = run_detached(&prefix, 45, &args);
    assert!(ok, "runsc run failed");
    eprintln!("runsc run returned; waiting for serve...");

    // 2. It must serve and report the OLD build id.
    let cold = wait_for_serve(&prefix, &globals, &cid, Duration::from_secs(30));
    assert_eq!(
        cold, OLD_ID,
        "cold-started sandbox should serve OLD build id"
    );
    eprintln!("cold-start serves: {cold}");

    // 2b. Prove the serving process actually *relocated* — serving the OLD id
    //     alone does not, since an envd that failed to relocate and served in
    //     place from the seed would report the identical id (and, under
    //     `root:memory`, would even survive the restore swap). Assert PID 1's
    //     executable resolves under the relocate dir, so a regression where
    //     relocation silently no-ops is caught here rather than slipping
    //     through the whole round-trip.
    let mut args = gstr.clone();
    args.extend(["exec", &cid, "/bin/busybox", "readlink", "/proc/1/exe"]);
    let (ok, exe, err) = run(&prefix, &args);
    let exe = exe.trim();
    assert!(ok, "reading /proc/1/exe in the sandbox failed: {err}");
    assert!(
        exe.starts_with(ENVD_RUN_DIR),
        "envd PID 1 should execute from the relocate dir {ENVD_RUN_DIR}, but /proc/1/exe = {exe:?} \
         (relocation did not happen)"
    );
    eprintln!("relocation confirmed: PID 1 exe = {exe}");

    // 3. Checkpoint (stops the container). Bound generously — it writes a
    //    ~200 MiB pages.img.
    let mut args = gstr.clone();
    args.extend([
        "checkpoint",
        "--image-path",
        &img_s,
        "--leave-running=false",
        &cid,
    ]);
    let (ok, _, err) = run_to(&prefix, 90, &args);
    assert!(ok, "runsc checkpoint failed: {err}");
    assert!(
        img.join("pages.img").exists(),
        "checkpoint must produce pages.img"
    );

    // 4. Replace the on-disk host-staged seed binary OLD -> NEW. This is the
    //    standalone analogue of swapping /var/lib/sandbawks/envd/envd on the
    //    node between checkpoint and restore.
    install(&new_bin, &rootfs.join("usr/local/bin/envd"));

    // 5. Delete the checkpointed container, then restore from the image. `runsc
    //    restore` can occasionally stall on this host; the `run_detached` cap
    //    turns a stall into a fast non-zero exit, and we retry the
    //    delete+restore once before giving up so a single transient gVisor flake
    //    doesn't fail the acceptance gate.
    let mut restored = String::new();
    for attempt in 1..=2 {
        let mut del = gstr.clone();
        del.extend(["delete", "-force", &cid]);
        let _ = run(&prefix, &del); // best-effort; first iteration removes the checkpointed container

        let mut args = gstr.clone();
        args.extend([
            "restore",
            "--bundle",
            &bundle_s,
            "--image-path",
            &img_s,
            "--detach",
            &cid,
        ]);
        eprintln!("restore attempt {attempt}...");
        let ok = run_detached(&prefix, 90, &args);
        if !ok {
            eprintln!("restore attempt {attempt} failed/timed out");
            continue;
        }

        // 6. The restored sandbox must STILL serve the OLD build id — proving the
        //    code pages were restored from pages.img, not re-faulted from the
        //    NEW on-disk binary.
        restored = wait_for_serve(&prefix, &globals, &cid, Duration::from_secs(30));
        if !restored.is_empty() {
            break;
        }
        eprintln!("restore attempt {attempt} restored but did not serve; retrying");
    }
    assert_eq!(
        restored, OLD_ID,
        "restored sandbox must report the OLD (captured) build id, not the swapped NEW host binary"
    );
    eprintln!("post-restore serves: {restored} (captured pages survived the host-binary swap)");
}
