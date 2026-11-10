package wrapper

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// restoreImagePathAnnotation is the OCI annotation a sandbawks-managed
// container carries when its Start must drive a gVisor memory Restore instead
// of a plain Start. Its value is the absolute path to the staged checkpoint
// image directory (the `*.img` files) the sandbox's memory + rootfs are
// restored from (#232 — memory-only restore; there is no longer a separate
// filesystem-snapshot restore step).
//
// The "io.sandbawks/" prefix follows the OCI reverse-DNS annotation convention,
// namespacing the key so it cannot collide with runtime or runc annotations.
const restoreImagePathAnnotation = "io.sandbawks/restore-image-path"

// criContainerTypeAnnotation is the containerd CRI annotation that tags an OCI
// bundle as either the pod sandbox (the "pause" infra container, which creates
// the runsc sandbox) or an application sub-container (which joins it).
// containerd stamps it on both, and `pod_annotations` copies the
// restore-image-path annotation onto BOTH specs — so the shim must decide which
// one drives the restore.
//
// A gVisor memory checkpoint is a *whole-sandbox* image: a single `runsc
// restore` on the container that owns the sandbox brings back every container's
// memory + rootfs at once. That restore happens on the root container (the pod
// sandbox, or a standalone container); the application sub-container is
// reconstituted by that same restore and then attaches via a plain Start.
// Dispatching Restore a second time on the sub-container would try to restore
// the already-restored sandbox again, so we restore on the root container and
// pass sub-containers through. (This is the same root-only gate the prior
// fs-restore design needed — kept because memory restore is whole-sandbox too.)
const criContainerTypeAnnotation = "io.kubernetes.cri.container-type"

// criContainerTypeSubContainer is the [criContainerTypeAnnotation] value
// marking an application sub-container that joins an existing sandbox — the one
// that must NOT drive its own restore (the root container's whole-sandbox
// restore already brought it back). Any other value (the "sandbox" pod
// container, or absent as in a non-CRI/test bundle, i.e. a standalone root
// container) is restorable. The guard is conservative: it suppresses restore
// *only* for an explicit sub-container.
const criContainerTypeSubContainer = "container"

// ociConfig is the minimal slice of an OCI runtime config.json the decorator
// needs: the annotations map. Decoding into this narrow struct rather than the
// full runtime-spec Spec keeps the dependency surface small and ensures an
// unrelated upstream schema change cannot break annotation parsing.
type ociConfig struct {
	Annotations map[string]string `json:"annotations"`
}

// parseRestoreAnnotation reads <bundleDir>/config.json and returns the restore
// image path declared by the io.sandbawks/restore-image-path annotation.
//
// Return contract:
//   - ("", nil)   — no restore requested: config.json is absent, or the
//     annotation is missing/empty. The caller forwards a plain Create.
//   - (path, nil) — restore requested: the annotation is present and its value
//     is an absolute path that exists on disk.
//   - ("", error) — the annotation is present but its value is unusable, or
//     config.json exists yet cannot be read or parsed.
//
// A missing config.json is deliberately not an error: the pass-through path
// must stay viable for callers (and tests) that never populate a bundle. A
// config.json that exists but is unreadable or malformed *is* an error, since
// that signals a genuinely broken bundle rather than "nothing to restore".
//
// The path is validated here, before any dispatch, as a defense against a
// crafted annotation escaping the staging root: it must be absolute (a relative
// path would resolve against the shim's unpredictable working directory) and
// must already exist (a memory Restore has no business pointing at a snapshot
// that is not staged). An invalid request therefore never reaches gVisor.
func parseRestoreAnnotation(bundleDir string) (string, error) {
	raw, err := os.ReadFile(filepath.Join(bundleDir, "config.json"))
	if err != nil {
		if os.IsNotExist(err) {
			return "", nil
		}
		return "", fmt.Errorf("read OCI config: %w", err)
	}

	var cfg ociConfig
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return "", fmt.Errorf("parse OCI config: %w", err)
	}

	// An application sub-container joining an existing sandbox never drives
	// its own restore — see criContainerTypeAnnotation. The whole sandbox
	// (including this container) is restored when the root/pod-sandbox
	// container's Start dispatches the memory Restore, and this sub-container
	// then attaches via a plain Start.
	if cfg.Annotations[criContainerTypeAnnotation] == criContainerTypeSubContainer {
		return "", nil
	}

	path := cfg.Annotations[restoreImagePathAnnotation]
	if path == "" {
		return "", nil
	}

	if !filepath.IsAbs(path) {
		return "", fmt.Errorf(
			"restore image path %q from annotation %s must be absolute",
			path, restoreImagePathAnnotation,
		)
	}
	// Deliberately NO existence check here. Under scheduler-placed restore
	// (epic #484) the restore Pod — and therefore this Create — is created
	// *before* its snapshot is staged onto the node, so the image dir routinely
	// does not exist yet at annotation-parse time. Readiness is instead enforced
	// at Start, where [Service.Start] blocks on the node's `.staged` marker via
	// waitForStagedImage before dispatching the memory Restore; gVisor surfaces
	// its own open error if the image is incomplete past that gate. The
	// absolute-path check above stays: it is the cheap, time-independent half of
	// the "crafted annotation can't escape the staging root" defense, and unlike
	// existence it can be decided correctly before staging.

	return path, nil
}
