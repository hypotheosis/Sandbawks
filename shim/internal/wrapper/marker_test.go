package wrapper

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	task "github.com/containerd/containerd/api/runtime/task/v2"
)

// TestWaitForStagedImageReturnsWhenMarkerPresent: an already-staged image (the
// marker exists before the call) returns nil immediately, without waiting a
// poll interval.
func TestWaitForStagedImageReturnsWhenMarkerPresent(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, stagedMarkerFilename), nil, 0o600); err != nil {
		t.Fatalf("write marker: %v", err)
	}

	if err := waitForStagedImage(context.Background(), dir, time.Second, 10*time.Millisecond); err != nil {
		t.Fatalf("waitForStagedImage returned error for a staged image: %v", err)
	}
}

// TestWaitForStagedImageReturnsWhenMarkerAppears: the marker is dropped after the
// call starts (modelling the node finishing staging while the gate polls); the
// wait must observe it and return nil before the timeout.
func TestWaitForStagedImageReturnsWhenMarkerAppears(t *testing.T) {
	dir := t.TempDir()
	marker := filepath.Join(dir, stagedMarkerFilename)

	done := make(chan error, 1)
	go func() {
		done <- waitForStagedImage(context.Background(), dir, 2*time.Second, 5*time.Millisecond)
	}()

	// Drop the marker shortly after the poll loop has begun.
	time.Sleep(20 * time.Millisecond)
	if err := os.WriteFile(marker, nil, 0o600); err != nil {
		t.Fatalf("write marker: %v", err)
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("waitForStagedImage returned error after marker appeared: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("waitForStagedImage did not return after the marker appeared")
	}
}

// TestWaitForStagedImageTimesOut: a marker that never appears makes the wait
// return a timeout error (the gate is bounded, not indefinite).
func TestWaitForStagedImageTimesOut(t *testing.T) {
	dir := t.TempDir() // no marker ever written

	err := waitForStagedImage(context.Background(), dir, 30*time.Millisecond, 5*time.Millisecond)
	if err == nil {
		t.Fatal("waitForStagedImage returned nil for an image that was never staged")
	}
}

// TestWaitForStagedImageHonorsContextCancellation: a cancelled context unblocks
// the wait promptly with the context error, so a shutdown/Start cancellation is
// not held hostage by the staging timeout.
func TestWaitForStagedImageHonorsContextCancellation(t *testing.T) {
	dir := t.TempDir() // no marker
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := waitForStagedImage(ctx, dir, time.Minute, 5*time.Millisecond); err != context.Canceled {
		t.Fatalf("waitForStagedImage returned %v, want context.Canceled", err)
	}
}

// TestStageGateTimeoutParsesEnv: the timeout is read from the environment, with a
// fallback to the default on unset / blank / zero / unparseable values so a
// misconfiguration never collapses to an instant-fail (zero-length) wait.
func TestStageGateTimeoutParsesEnv(t *testing.T) {
	cases := []struct {
		name string
		set  bool
		val  string
		want time.Duration
	}{
		{name: "unset", set: false, want: defaultStageGateTimeout},
		{name: "valid", set: true, val: "45", want: 45 * time.Second},
		{name: "whitespace", set: true, val: "  90 ", want: 90 * time.Second},
		{name: "blank", set: true, val: "   ", want: defaultStageGateTimeout},
		{name: "zero", set: true, val: "0", want: defaultStageGateTimeout},
		{name: "garbage", set: true, val: "soon", want: defaultStageGateTimeout},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.set {
				t.Setenv(stageGateTimeoutEnv, tc.val)
			} else {
				os.Unsetenv(stageGateTimeoutEnv)
			}
			if got := stageGateTimeout(); got != tc.want {
				t.Fatalf("stageGateTimeout() = %s, want %s", got, tc.want)
			}
		})
	}
}

// TestStartBlocksUntilMarkerThenRestores is the end-to-end gate proof at the
// Start seam: a restore intent whose image is not yet staged must NOT dispatch
// Restore until the node drops the `.staged` marker — exactly the
// create-then-stage ordering the shim gate exists to enforce.
func TestStartBlocksUntilMarkerThenRestores(t *testing.T) {
	fake := &recordingService{}
	s := &Service{next: fake}
	dir := t.TempDir() // unstaged: no marker yet
	s.restoring.Store("c1", dir)

	done := make(chan error, 1)
	go func() {
		_, err := s.Start(context.Background(), &task.StartRequest{ID: "c1"})
		done <- err
	}()

	// Give Start a moment to enter the gate, then confirm it has NOT yet
	// dispatched Restore (the marker is absent).
	time.Sleep(30 * time.Millisecond)
	if len(fake.calls) != 0 {
		t.Fatalf("Restore dispatched before the image was staged: %v", fake.calls)
	}

	// Stage the image; the gate must now clear and Restore must dispatch.
	if err := os.WriteFile(filepath.Join(dir, stagedMarkerFilename), nil, 0o600); err != nil {
		t.Fatalf("write marker: %v", err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Start returned error after staging completed: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Start did not dispatch Restore after the marker appeared")
	}
	if len(fake.calls) != 1 || fake.calls[0] != "Restore" {
		t.Fatalf("forwarded as %v, want exactly [Restore]", fake.calls)
	}
}
