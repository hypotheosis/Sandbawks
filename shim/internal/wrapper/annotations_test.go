package wrapper

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	task "github.com/containerd/containerd/api/runtime/task/v2"
)

// writeBundle creates a bundle directory containing a config.json whose
// annotations map is exactly annos, and returns the bundle path. A nil annos
// still writes a valid config.json that simply carries no annotations.
func writeBundle(t *testing.T, annos map[string]string) string {
	t.Helper()
	dir := t.TempDir()
	raw, err := json.Marshal(ociConfig{Annotations: annos})
	if err != nil {
		t.Fatalf("marshal config.json: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "config.json"), raw, 0o600); err != nil {
		t.Fatalf("write config.json: %v", err)
	}
	return dir
}

// stagedImage creates a restore-image *directory* containing the [.staged]
// completion marker (plus a placeholder image file) and returns its absolute
// path, standing in for a snapshot the node has finished staging. The restore
// image path is a directory: the shim's Start gate waits for the marker inside
// it (waitForStagedImage) before dispatching Restore, so a test that drives
// Start through the restore path must stage the marker or the gate blocks.
func stagedImage(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "pages.img"), []byte("snapshot"), 0o600); err != nil {
		t.Fatalf("write snapshot image: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, stagedMarkerFilename), nil, 0o600); err != nil {
		t.Fatalf("write staged marker: %v", err)
	}
	return dir
}

// TestCreateNoAnnotationPassesThrough is smoke case (1): a bundle without the
// restore annotation forwards to plain Create and records no restore intent.
func TestCreateNoAnnotationPassesThrough(t *testing.T) {
	fake := &recordingService{}
	s := &Service{next: fake}
	bundle := writeBundle(t, nil)

	if _, err := s.Create(context.Background(), &task.CreateTaskRequest{ID: "c1", Bundle: bundle}); err != nil {
		t.Fatalf("Create returned error: %v", err)
	}
	if len(fake.calls) != 1 || fake.calls[0] != "Create" {
		t.Fatalf("forwarded as %v, want exactly [Create]", fake.calls)
	}
	if _, ok := s.restoring.Load("c1"); ok {
		t.Fatal("restore intent stored for a non-restore container")
	}
}

// TestCreateEmptyAnnotationPassesThrough covers the contract corner where the
// annotation key is present but its value is empty: parseRestoreAnnotation
// returns ("", nil), so Create must pass through to plain Create and store no
// restore intent.
func TestCreateEmptyAnnotationPassesThrough(t *testing.T) {
	fake := &recordingService{}
	s := &Service{next: fake}
	bundle := writeBundle(t, map[string]string{restoreImagePathAnnotation: ""})

	if _, err := s.Create(context.Background(), &task.CreateTaskRequest{ID: "c5", Bundle: bundle}); err != nil {
		t.Fatalf("Create returned error: %v", err)
	}
	if len(fake.calls) != 1 || fake.calls[0] != "Create" {
		t.Fatalf("forwarded as %v, want exactly [Create]", fake.calls)
	}
	if _, ok := s.restoring.Load("c5"); ok {
		t.Fatal("restore intent stored for an empty annotation value")
	}
}

// TestCreateMalformedConfigErrors covers the contract corner where config.json
// exists but is not valid JSON: parseRestoreAnnotation returns ("", error), so
// Create must surface the error before any dispatch and store no intent.
func TestCreateMalformedConfigErrors(t *testing.T) {
	fake := &recordingService{}
	s := &Service{next: fake}
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "config.json"), []byte("{not valid json"), 0o600); err != nil {
		t.Fatalf("write malformed config.json: %v", err)
	}

	if _, err := s.Create(context.Background(), &task.CreateTaskRequest{ID: "c6", Bundle: dir}); err == nil {
		t.Fatal("Create returned nil error for a malformed config.json")
	}
	if len(fake.calls) != 0 {
		t.Fatalf("dispatched %v despite a malformed config.json; want no forward", fake.calls)
	}
	if _, ok := s.restoring.Load("c6"); ok {
		t.Fatal("restore intent stored despite parse failure")
	}
}

// TestCreateWithAnnotationRecordsRestoreIntent is smoke case (2): a bundle whose
// annotation points at an existing absolute path forwards to a plain Create
// (#232 — no CreateWithFSRestore) and records the staged image path as restore
// intent under the container ID so Start can drive the memory Restore.
func TestCreateWithAnnotationRecordsRestoreIntent(t *testing.T) {
	fake := &recordingService{}
	s := &Service{next: fake}
	image := stagedImage(t)
	bundle := writeBundle(t, map[string]string{restoreImagePathAnnotation: image})

	if _, err := s.Create(context.Background(), &task.CreateTaskRequest{ID: "c2", Bundle: bundle}); err != nil {
		t.Fatalf("Create returned error: %v", err)
	}
	if len(fake.calls) != 1 || fake.calls[0] != "Create" {
		t.Fatalf("forwarded as %v, want exactly [Create] (memory-only restore: no CreateWithFSRestore)", fake.calls)
	}
	got, ok := s.restoring.Load("c2")
	if !ok {
		t.Fatal("restore intent not stored for an annotated container")
	}
	if got != image {
		t.Fatalf("stored restore intent = %q, want %q", got, image)
	}
}

// TestCreateRootSandboxWithAnnotationRecordsRestoreIntent covers the pod-sandbox
// (root container) case: the whole-sandbox memory checkpoint is restored on the
// container that owns the sandbox. A bundle tagged container-type=sandbox with a
// valid image therefore forwards to a plain Create and records the restore
// intent so the root container's Start drives the memory Restore.
func TestCreateRootSandboxWithAnnotationRecordsRestoreIntent(t *testing.T) {
	fake := &recordingService{}
	s := &Service{next: fake}
	image := stagedImage(t)
	bundle := writeBundle(t, map[string]string{
		criContainerTypeAnnotation: "sandbox",
		restoreImagePathAnnotation: image,
	})

	if _, err := s.Create(context.Background(), &task.CreateTaskRequest{ID: "sb1", Bundle: bundle}); err != nil {
		t.Fatalf("Create returned error: %v", err)
	}
	if len(fake.calls) != 1 || fake.calls[0] != "Create" {
		t.Fatalf("forwarded as %v, want exactly [Create] (the root sandbox records the restore intent)", fake.calls)
	}
	if _, ok := s.restoring.Load("sb1"); !ok {
		t.Fatal("restore intent not stored for the root sandbox container")
	}
}

// TestCreateSubContainerWithAnnotationPassesThrough guards the sub-container
// case: pod_annotations copies the restore-image-path annotation onto the
// application container's spec too, but a sub-container must not drive its own
// restore — the root container's whole-sandbox memory Restore already brings it
// back, and a second Restore would re-restore the sandbox. So a bundle tagged
// container-type=container must forward to a plain Create and record no restore
// intent, even with a valid image path.
func TestCreateSubContainerWithAnnotationPassesThrough(t *testing.T) {
	fake := &recordingService{}
	s := &Service{next: fake}
	image := stagedImage(t)
	bundle := writeBundle(t, map[string]string{
		criContainerTypeAnnotation: criContainerTypeSubContainer,
		restoreImagePathAnnotation: image,
	})

	if _, err := s.Create(context.Background(), &task.CreateTaskRequest{ID: "ct1", Bundle: bundle}); err != nil {
		t.Fatalf("Create returned error: %v", err)
	}
	if len(fake.calls) != 1 || fake.calls[0] != "Create" {
		t.Fatalf("forwarded as %v, want exactly [Create] (a sub-container must not be restored)", fake.calls)
	}
	if _, ok := s.restoring.Load("ct1"); ok {
		t.Fatal("restore intent stored for an application sub-container")
	}
}

// TestCreateWithUnstagedImageRecordsIntent covers scheduler-placed restore (epic
// #484): the restore Pod's Create runs *before* the snapshot is staged, so an
// annotation naming an absolute path that does not exist yet must NOT be
// rejected — Create forwards plainly and records the intent. Readiness is
// enforced later, at Start, by the `.staged` marker gate (waitForStagedImage),
// not by an existence check here. (Contrast TestCreateWithRelativeImageErrors:
// the absolute-path defense is time-independent and still fires.)
func TestCreateWithUnstagedImageRecordsIntent(t *testing.T) {
	fake := &recordingService{}
	s := &Service{next: fake}
	unstaged := filepath.Join(t.TempDir(), "not-staged-yet", "restore")
	bundle := writeBundle(t, map[string]string{restoreImagePathAnnotation: unstaged})

	if _, err := s.Create(context.Background(), &task.CreateTaskRequest{ID: "c3", Bundle: bundle}); err != nil {
		t.Fatalf("Create rejected an as-yet-unstaged restore image: %v", err)
	}
	if len(fake.calls) != 1 || fake.calls[0] != "Create" {
		t.Fatalf("forwarded as %v, want exactly [Create]", fake.calls)
	}
	got, ok := s.restoring.Load("c3")
	if !ok {
		t.Fatal("restore intent not stored for an as-yet-unstaged restore image")
	}
	if got != unstaged {
		t.Fatalf("stored restore intent = %q, want %q", got, unstaged)
	}
}

// TestCreateWithRelativeImageErrors guards the absolute-path defense: a relative
// annotation value is rejected before dispatch even if a same-named file exists
// in the working directory.
func TestCreateWithRelativeImageErrors(t *testing.T) {
	fake := &recordingService{}
	s := &Service{next: fake}
	bundle := writeBundle(t, map[string]string{restoreImagePathAnnotation: "relative/snapshot.img"})

	if _, err := s.Create(context.Background(), &task.CreateTaskRequest{ID: "c4", Bundle: bundle}); err == nil {
		t.Fatal("Create accepted a relative restore image path")
	}
	if len(fake.calls) != 0 {
		t.Fatalf("dispatched %v for a relative path; want no forward", fake.calls)
	}
}

// TestDeleteClearsContainerRestoreState verifies that an exec-process delete
// preserves the container's restore intent while a container-level delete clears it.
func TestDeleteClearsContainerRestoreState(t *testing.T) {
	fake := &recordingService{}
	s := &Service{next: fake}
	s.restoring.Store("c1", "/snap.img")

	if _, err := s.Delete(context.Background(), &task.DeleteRequest{ID: "c1", ExecID: "exec0"}); err != nil {
		t.Fatalf("Delete(exec) returned error: %v", err)
	}
	if _, ok := s.restoring.Load("c1"); !ok {
		t.Fatal("exec-process delete dropped the container restore intent")
	}

	if _, err := s.Delete(context.Background(), &task.DeleteRequest{ID: "c1"}); err != nil {
		t.Fatalf("Delete returned error: %v", err)
	}
	if _, ok := s.restoring.Load("c1"); ok {
		t.Fatal("container delete did not clear the restore intent")
	}
}

// TestStartNoIntentPassesThrough is Task 4 smoke case (1): a container with no
// recorded restore intent forwards to plain Start, never Restore.
func TestStartNoIntentPassesThrough(t *testing.T) {
	fake := &recordingService{}
	s := &Service{next: fake}

	if _, err := s.Start(context.Background(), &task.StartRequest{ID: "c1"}); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	if len(fake.calls) != 1 || fake.calls[0] != "Start" {
		t.Fatalf("forwarded as %v, want exactly [Start]", fake.calls)
	}
}

// TestStartWithIntentDispatchesRestore is Task 4 smoke case (2): a container
// whose Create recorded a restore intent forwards Start to Restore with the
// remembered image path and the expected flags, and consumes the intent so a
// later restart boots normally.
func TestStartWithIntentDispatchesRestore(t *testing.T) {
	fake := &recordingService{}
	s := &Service{next: fake}
	// A fully-staged image (the `.staged` marker is present), so the Start gate
	// clears immediately and dispatches Restore.
	image := stagedImage(t)
	s.restoring.Store("c2", image)

	if _, err := s.Start(context.Background(), &task.StartRequest{ID: "c2"}); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	if len(fake.calls) != 1 || fake.calls[0] != "Restore" {
		t.Fatalf("forwarded as %v, want exactly [Restore]", fake.calls)
	}
	if len(fake.restoreReqs) != 1 {
		t.Fatalf("recorded %d Restore requests, want exactly 1", len(fake.restoreReqs))
	}
	conf := fake.restoreReqs[0].Conf
	if conf.ImagePath != image {
		t.Fatalf("dispatched ImagePath = %q, want %q", conf.ImagePath, image)
	}
	if conf.Direct {
		t.Fatal("dispatched Direct = true, want false")
	}
	if !conf.Background {
		t.Fatal("dispatched Background = false, want true (Plan 08 B1: lazy/page-fault restore)")
	}
	if _, ok := s.restoring.Load("c2"); ok {
		t.Fatal("restore intent not consumed after dispatch")
	}
}

// TestStartExecProcessPassesThrough guards the ExecID seam: a non-empty ExecID
// names an exec process inside an already-running container, never a restore,
// so Start must pass through and leave any container-level intent untouched.
func TestStartExecProcessPassesThrough(t *testing.T) {
	fake := &recordingService{}
	s := &Service{next: fake}
	s.restoring.Store("c2", "/snap/c2.img")

	if _, err := s.Start(context.Background(), &task.StartRequest{ID: "c2", ExecID: "exec0"}); err != nil {
		t.Fatalf("Start(exec) returned error: %v", err)
	}
	if len(fake.calls) != 1 || fake.calls[0] != "Start" {
		t.Fatalf("forwarded as %v, want exactly [Start]", fake.calls)
	}
	if _, ok := s.restoring.Load("c2"); !ok {
		t.Fatal("exec-process start consumed the container restore intent")
	}
}

// TestStartTwoContainersNoCrossContamination is Task 4 smoke case (3): two
// containers with distinct recorded image paths each restore from their own
// path, proving the per-CID map never crosses one container's intent into
// another's Restore.
func TestStartTwoContainersNoCrossContamination(t *testing.T) {
	fake := &recordingService{}
	s := &Service{next: fake}
	// Both staged (markers present) so each Start gate clears immediately.
	img1 := stagedImage(t)
	img2 := stagedImage(t)
	s.restoring.Store("c1", img1)
	s.restoring.Store("c2", img2)

	if _, err := s.Start(context.Background(), &task.StartRequest{ID: "c1"}); err != nil {
		t.Fatalf("Start(c1) returned error: %v", err)
	}
	if _, err := s.Start(context.Background(), &task.StartRequest{ID: "c2"}); err != nil {
		t.Fatalf("Start(c2) returned error: %v", err)
	}

	if want := []string{"Restore", "Restore"}; len(fake.calls) != 2 ||
		fake.calls[0] != want[0] || fake.calls[1] != want[1] {
		t.Fatalf("forwarded as %v, want %v", fake.calls, want)
	}
	// Pin each dispatch to its own container's image path. Asserting against the
	// indexed request (rather than a shared "last request") is what makes this a
	// real cross-contamination guard: if Start ever leaked one container's intent
	// into another's Restore, exactly one of these would catch it.
	if len(fake.restoreReqs) != 2 {
		t.Fatalf("recorded %d Restore requests, want exactly 2", len(fake.restoreReqs))
	}
	if got := fake.restoreReqs[0].Conf.ImagePath; got != img1 {
		t.Fatalf("c1 restored from %q, want %q", got, img1)
	}
	if got := fake.restoreReqs[1].Conf.ImagePath; got != img2 {
		t.Fatalf("c2 restored from %q, want %q", got, img2)
	}
	s.restoring.Range(func(key, _ any) bool {
		t.Fatalf("restore intent %v survived its container's Start", key)
		return false
	})
}

// TestShutdownClearsAllRestoreState verifies Shutdown drops every recorded
// restore intent.
func TestShutdownClearsAllRestoreState(t *testing.T) {
	fake := &recordingService{}
	s := &Service{next: fake}
	s.restoring.Store("c1", "/a.img")
	s.restoring.Store("c2", "/b.img")

	if _, err := s.Shutdown(context.Background(), &task.ShutdownRequest{}); err != nil {
		t.Fatalf("Shutdown returned error: %v", err)
	}
	s.restoring.Range(func(key, _ any) bool {
		t.Fatalf("restore intent %v survived Shutdown", key)
		return false
	})
}
