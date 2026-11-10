package wrapper

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// stagedMarkerFilename is the completion marker the node DaemonSet drops, last,
// inside the staged restore image directory once every checkpoint part is on
// disk (epic #484, issue #487). It MUST match the Rust
// `sandbawks_types::STAGED_MARKER_FILENAME` constant the node writes — the shim
// and the node agree on this name by convention, not by a shared build artifact.
const stagedMarkerFilename = ".staged"

// stageGateTimeoutEnv overrides [defaultStageGateTimeout]. It is read from the
// shim process's own environment (the shim runs as the containerd runtime
// handler, not as a Pod), so a cluster tunes it via the containerd/shim service
// environment rather than Helm. An unset, blank, zero, or unparseable value
// falls back to the default.
const stageGateTimeoutEnv = "SANDBAWKS_STAGE_GATE_TIMEOUT_SECS"

// defaultStageGateTimeout bounds how long [waitForStagedImage] blocks the
// restore container's Start waiting for the node to finish staging. Sized for a
// GiB-scale bundle staging onto a cold node; the wait returns the instant the
// marker appears, so a generous ceiling costs nothing on the happy path.
const defaultStageGateTimeout = 300 * time.Second

// markerPollInterval is how often [waitForStagedImage] re-stats the marker. A
// quarter second keeps post-staging restore latency low without busy-spinning.
const markerPollInterval = 250 * time.Millisecond

// stageGateTimeout resolves the restore staging-gate timeout from
// [stageGateTimeoutEnv], falling back to [defaultStageGateTimeout] on an unset,
// blank, zero, or unparseable value — a misconfigured env should degrade to a
// sane bound, never to a zero-length (instant-fail) wait.
func stageGateTimeout() time.Duration {
	raw := strings.TrimSpace(os.Getenv(stageGateTimeoutEnv))
	if raw == "" {
		return defaultStageGateTimeout
	}
	secs, err := strconv.ParseUint(raw, 10, 64)
	if err != nil || secs == 0 {
		return defaultStageGateTimeout
	}
	return time.Duration(secs) * time.Second
}

// waitForStagedImage blocks until the node has finished staging the checkpoint
// at imagePath — signalled by the [stagedMarkerFilename] marker appearing inside
// it — or until ctx is cancelled or timeout elapses.
//
// This is the scheduler-placed-restore gate (epic #484). The restore Pod is
// created *before* its snapshot is staged onto the node the scheduler picked, so
// the restore container's Start must wait for staging to complete. The gate
// lives here, in the shim, rather than in a Pod init container: a gVisor memory
// checkpoint is restored as a *whole sandbox*, and runsc reconstitutes exactly
// the checkpoint's container set — so an extra init container would be rejected
// by restore-spec validation ("checkpoint image does not contain spec for
// container"). Gating the root container's Restore dispatch here keeps the wait
// outside the restored sandbox's container set entirely.
//
// The marker is checked *before* the first sleep, so an already-staged image
// returns immediately. A non-NotExist stat error (e.g. a permission problem on
// the staging mount) is returned rather than retried — it will not self-heal.
func waitForStagedImage(ctx context.Context, imagePath string, timeout, poll time.Duration) error {
	if poll <= 0 {
		poll = markerPollInterval
	}
	marker := filepath.Join(imagePath, stagedMarkerFilename)

	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(poll)
	defer ticker.Stop()

	for {
		switch _, err := os.Stat(marker); {
		case err == nil:
			return nil
		case !os.IsNotExist(err):
			return fmt.Errorf("stat staged marker %q: %w", marker, err)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			return fmt.Errorf(
				"staged restore image %q not ready: marker %q did not appear within %s",
				imagePath, marker, timeout,
			)
		case <-ticker.C:
		}
	}
}
