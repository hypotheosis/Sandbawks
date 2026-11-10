# envd static-link guard — extract `TARGET` build arg (PR #274 review)

- **Date:** 2026-06-02T18:00:00Z
- **Issue:** #267
- **PR:** #274
- **Trigger:** Review comment on `crates/envd/Dockerfile` about the
  hardcoded `x86_64-unknown-linux-musl` path coupling the new static-link
  guard to the single target the Dockerfile builds.

## Change

Introduced a single `ARG TARGET=x86_64-unknown-linux-musl` as the source
of truth for the Rust target triple in the **builder** and final stages of
`crates/envd/Dockerfile`. `${TARGET}` now drives:

- `cargo build --release --target "${TARGET}"` in the builder stage,
- the static-link guard's binary path (`/src/target/${TARGET}/release/envd`),
- the final-stage `COPY` of the binary out of the builder.

These three previously each spelled out the triple literally and could
drift apart if the target ever changed; they are now one knob.

## Why only the builder + final stages

The `chef` / `planner` / `cacher` stages are deliberately byte-identical to
the controller / api Dockerfiles so BuildKit shares the cooker layer hash
across all three workspace image builds (the same property that put the
`file` install in the builder stage rather than `chef`). Parameterizing
those stages — or the `CC_*` / `CARGO_TARGET_*` env whose names structurally
embed the triple — would break that sharing for no benefit. So they remain
musl-pinned: `rustup target add`, the linker env, and
`cargo chef cook --target` still name `x86_64-unknown-linux-musl`
literally.

The consequence, documented inline: overriding `TARGET` only makes sense
for another musl triple the chef stage already provisions (e.g. an
`aarch64-unknown-linux-musl` cross leg would also add that target +
linker upstream). A non-musl override fails loudly at build time rather
than silently shipping a dynamic binary.

## Validation

- `just fmt` / `cargo clippy --workspace --all-targets -- -D warnings` /
  `just test` — clean (no Rust touched).
- `just envd-image-build` — full image build succeeds with the default
  `TARGET`; guard still prints `OK: envd is statically linked` and the
  final `COPY` resolves the binary.
