// SDK methods explicitly skipped until the backing feature lands.
// Mirrors `tests/e2b-sdk-compat/tests/test_skipped.py`. Each test is
// `it.skip` with a reason pointing at the plan / epic that will
// deliver the missing functionality. Keeping skipped tests in the
// suite (rather than deleting them) means the test matrix output
// advertises what's *not* yet wired up — reviewers of a later plan
// can grep this file for their plan number.

import { describe, it } from "vitest";

describe("skipped_until_plan", () => {
    // Sandbox.createSnapshot requires the warm-pool / snapshot CRD
    // path. TODO(plan-07): exercise once `SandboxWarmPool` lands and
    // the API surfaces an explicit snapshot management endpoint.
    it.skip("createSnapshot pending plan-07 (warm-pool + snapshot CRD)", () => {});

    it.skip("deleteSnapshot pending plan-07 (warm-pool + snapshot CRD)", () => {});

    it.skip("listSnapshots pending plan-07 (warm-pool + snapshot CRD)", () => {});

    // Template.build requires the template build pipeline.
    // TODO(future-plan): out of scope for v0.1 MVP; tracked alongside
    // template registry work post-MVP.
    it.skip("Template.build pending future plan (template build pipeline)", () => {});

    // Sandbox.getMetrics reads the Prometheus surface in spec §11.
    // TODO: enable once the observability plan lands the per-sandbox
    // metrics endpoint the SDK probes.
    it.skip("getMetrics pending observability plan (spec §11)", () => {});

    // Sandbox.create({ volumeMounts: ... }) requires the Volume CRD.
    // TODO(future-plan): Volume / persistent-storage support is post-MVP.
    it.skip("volumeMounts pending future plan (Volume / persistent storage)", () => {});

    // files.watchDir — SDK contract (callback vs async iterable) is
    // unverified; mirrors the deferral in the Python harness.
    // TODO(plan-06-t5+): rewrite against the live JS SDK contract
    // once the CI job exercises a real gvisor kind cluster.
    it.skip("files.watchDir pending SDK contract verification", () => {});
});
