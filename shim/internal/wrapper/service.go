// Package wrapper contains the sandbawks decorator over gVisor's runsc task
// service. It sits between containerd and the upstream runsc shim service and
// intercepts restore-annotated Start requests, redirecting them to gVisor's
// memory Restore extension RPC. Every other task RPC forwards straight to the
// wrapped service.
//
// # Memory-only restore (#232)
//
// An earlier design routed a restore-annotated Create to gVisor's
// CreateWithFSRestore extension (passing runsc a --fs-restore-image-path
// flag) and then a memory Restore on Start. The #230/#231/#232 experiments
// proved that path is both redundant and broken under sandbawks' default
// --overlay2=root:self: the workload's rootfs writes already live in the
// sandbox-internal overlay-tmpfs, which the memory checkpoint captures in
// pages.img, so a plain Create plus a memory Restore reconstructs the
// filesystem on its own. CreateWithFSRestore additionally tried to restore
// the same overlay-tmpfs a second time from the fs-checkpoint, failing every
// restore with "containerManager.Restore: EOF". Create therefore now always
// dispatches a plain create; only the staged image path is remembered so
// Start can drive the memory Restore. The CreateWithFSRestore method below is
// kept solely to satisfy the extension.TaskServiceExt interface — nothing in
// this decorator dispatches to it.
//
// # How the decorator is installed
//
// gVisor's runsc service consults a package-level hook,
// extension.NewExtension, when it builds the service for a container. If that
// hook is non-nil it is called with the freshly constructed runsc service as
// `next`, and whatever TaskServiceExt it returns is used for all subsequent
// RPCs. main wires extension.NewExtension = wrapper.NewService, so every
// container's RPCs flow through a *Service that holds the runsc service as its
// `next`. This is gVisor's intended extension seam — we are not replacing the
// runsc service, we are layering in front of it.
//
// # Do not embed the interface
//
// Service forwards every RPC explicitly rather than embedding
// extension.TaskServiceExt. That is load-bearing: the
// `var _ extension.TaskServiceExt = (*Service)(nil)` assertion only catches a
// newly-added upstream interface method (the build fails until it is
// forwarded) *because* nothing is embedded. Embedding the interface to "save
// boilerplate" would auto-satisfy any new method by silently forwarding it,
// defeating that guarantee and the single, obvious interception point that
// T3/T4 rely on for Create/Start. Keep the forwarding explicit.
package wrapper

import (
	"context"
	"sync"

	task "github.com/containerd/containerd/api/runtime/task/v2"
	types "github.com/containerd/containerd/v2/pkg/protobuf/types"

	"gvisor.dev/gvisor/pkg/shim/v1/extension"
)

// Service decorates a gVisor runsc task service (extension.TaskServiceExt).
//
// In Task 2 every method forwards verbatim to the wrapped service held in
// `next`. The explicit per-method forwarding (rather than struct embedding) is
// deliberate: it gives later tasks a single, obvious place to intercept Create
// and Start, and it lets the table test assert that no RPC silently bypasses
// the decorator.
type Service struct {
	// next is the wrapped service every RPC forwards to — in production this
	// is gVisor's runsc service; in tests it is a recording fake.
	next extension.TaskServiceExt

	// restoring records, per container ID, the staged restore image path of
	// every container whose Create carried a restore annotation. It is the
	// bridge between Create (which records the path) and Start (which consults
	// it to route a restored container's Start to gVisor's memory Restore RPC).
	// Entries are cleared on the container's Delete and on Shutdown so a
	// long-lived Service never accumulates stale intent. sync.Map (rather than a
	// map+mutex) fits the access pattern: write-once on Create, read on Start,
	// delete-once on teardown, all from concurrent RPC goroutines.
	restoring sync.Map
}

// Compile-time assertion that *Service satisfies the full extension interface
// (task.TaskService plus the two extension RPCs). If gVisor adds a method to
// TaskServiceExt, this line fails to build until the new method is forwarded.
var _ extension.TaskServiceExt = (*Service)(nil)

// NewService constructs the decorator. Its signature matches the
// extension.NewExtension hook (func(ctx, next, *task.CreateTaskRequest)
// (extension.TaskServiceExt, error)), so it can be assigned to that hook
// directly in main.
//
// `req` is the Create request that triggered service construction. Task 2
// ignores it (no annotation parsing yet); Task 3 will inspect its annotations
// to decide whether the container should be restored. Returning the decorator
// with a nil error means "I handle this task"; gVisor's hook contract also
// permits returning nil, nil to opt out, which this pass-through never does.
func NewService(_ context.Context, next extension.TaskServiceExt, _ *task.CreateTaskRequest) (extension.TaskServiceExt, error) {
	return &Service{next: next}, nil
}

// --- Standard containerd task.TaskService RPCs (forwarded verbatim) ---

// State returns the runtime state of a process. Forwards to the wrapped service.
func (s *Service) State(ctx context.Context, r *task.StateRequest) (*task.StateResponse, error) {
	return s.next.State(ctx, r)
}

// Create creates a new container. It always dispatches a plain create — the
// sandbox is built normally — and, if the container's OCI bundle carries the
// restore-image-path annotation, additionally records the staged image path so
// Start can dispatch a memory Restore for it. See the package docs for why the
// old CreateWithFSRestore dispatch was removed (#232): under
// --overlay2=root:self the memory checkpoint already carries the rootfs, so a
// plain create plus a memory Restore is sufficient, and the fs-restore graft
// double-restored the overlay-tmpfs and failed every restore.
//
// A malformed bundle or an invalid annotation (relative or non-existent image
// path) is rejected here, before any dispatch, so a bad restore request never
// reaches gVisor. The intent is recorded only after Create succeeds, so a
// failed create leaves no stale entry behind.
func (s *Service) Create(ctx context.Context, r *task.CreateTaskRequest) (*task.CreateTaskResponse, error) {
	imagePath, err := parseRestoreAnnotation(r.Bundle)
	if err != nil {
		return nil, err
	}

	resp, err := s.next.Create(ctx, r)
	if err != nil {
		return nil, err
	}
	if imagePath != "" {
		// Restore-annotated container: remember the staged image path so
		// Start dispatches a memory Restore against it.
		s.restoring.Store(r.ID, imagePath)
	}
	return resp, nil
}

// Start starts a previously created process. It is the mirror of Create's
// interception: if Create recorded a restore intent for this container ID (set
// when Create saw the restore annotation), the intent is consumed and the
// request is redirected to gVisor's memory Restore extension with the
// remembered image path; otherwise it passes straight through to the wrapped
// service's Start.
//
// Only a container-level Start (empty ExecID) can carry restore intent. A
// non-empty ExecID names an exec process inside an already-running container,
// never a fresh restore, so it always passes through — the same ExecID guard
// Delete uses, and the reason the restoring map is keyed by container ID alone.
//
// The intent is consumed with LoadAndDelete (an atomic read-and-clear) so a
// container that is started, stopped, then started again is restored exactly
// once: the second Start finds no intent and boots normally. Clearing only on a
// successful Restore would instead re-restore on every restart, which is wrong.
//
// The clear happens before Restore dispatches, mirroring Delete. This is a
// deliberate choice on the dispatch-error axis: if Restore fails, the intent is
// already gone, so a later Start on the same CID degrades to a plain boot rather
// than silently retrying the restore. A failed restore therefore surfaces as the
// Restore error to this caller; recovering it is the orchestrator's job (delete
// and recreate the container), not an implicit retry hidden behind a stale intent.
func (s *Service) Start(ctx context.Context, r *task.StartRequest) (*task.StartResponse, error) {
	if r.ExecID != "" {
		return s.next.Start(ctx, r)
	}
	intent, ok := s.restoring.LoadAndDelete(r.ID)
	if !ok {
		// Ordinary container: no restore intent recorded, plain pass-through.
		return s.next.Start(ctx, r)
	}
	// Invariant: only Create writes this map, and always with a string image
	// path, so the assertion cannot fail. A panic here would mean the map's
	// write contract was violated elsewhere — a bug to surface loudly, not mask.
	imagePath := intent.(string)

	// Scheduler-placed restore (epic #484): the restore Pod is created *before*
	// its snapshot is staged onto the node the scheduler picked, so block here
	// until the node's `Stage` job has written the `.staged` marker into
	// imagePath. This gate must live in the shim, not a Pod init container: a
	// gVisor memory checkpoint restores as a whole sandbox and runsc rebuilds
	// exactly the checkpoint's container set, so any extra init container would
	// fail restore-spec validation ("checkpoint image does not contain spec for
	// container"). Gating the root container's Restore here keeps the wait
	// outside the restored sandbox. A timeout returns the error to the caller;
	// the orchestrator recovers it (the controller's bounded `Resuming` loop
	// recreates the restore Pod), exactly as it would any other failed Start.
	if err := waitForStagedImage(ctx, imagePath, stageGateTimeout(), markerPollInterval); err != nil {
		return nil, err
	}

	return s.next.Restore(ctx, &extension.RestoreRequest{
		Start: r,
		Conf: extension.RestoreConfig{
			ImagePath: imagePath,
			Direct:    false,
			// Plan 08 B1: lazy / page-fault restore is now the default. With
			// Background=true, Restore returns from Start before every page is
			// faulted in; gVisor reads the remaining pages from ImagePath on
			// demand, for as long as the container runs. This is the spec §6 /
			// §13 path that drives resume p95 toward the 2 s target.
			//
			// Because pages are read after Start returns, the staged
			// --image-path bundle MUST outlive container start — it can only
			// be GC'd once the owning Sandbox leaves Running/Resuming. That
			// retention is enforced node-side by the staging-dir sweep keyed
			// to sandbox phase (Plan 08 B2,
			// sandbawks_node::gvisor::staging_bundle_needed).
			//
			// Only the resume Pod's *root* sandbox container reaches this
			// dispatch: application sub-containers are gated out at Create
			// (the io.kubernetes.cri.container-type=container annotation, see
			// annotations.go / TestCreateSubContainerWithAnnotationPassesThrough),
			// so they never record a restore intent and Start passes them
			// through. The bundle therefore must outlive the *root* sandbox
			// container — exactly the lifetime the node tracks via Sandbox
			// phase, which is what makes the Go-side invariant and the
			// Rust-side retention predicate line up.
			Background: true,
		},
	})
}

// Delete deletes a process. A container-level Delete (empty ExecID) also clears
// any restore intent recorded for that container ID; deleting an exec process
// inside the container (non-empty ExecID) leaves the container's intent intact.
// Forwards to the wrapped service either way.
//
// The intent is cleared *before* forwarding, the mirror image of Create, which
// records intent only *after* a successful dispatch. The asymmetry is
// deliberate: a Delete the runtime then rejects should still drop the restore
// intent, since a container being torn down must never leave intent that a
// later Start could act on — whereas a failed create must not leave intent for
// a container that was never created.
func (s *Service) Delete(ctx context.Context, r *task.DeleteRequest) (*task.DeleteResponse, error) {
	if r.ExecID == "" {
		s.restoring.Delete(r.ID)
	}
	return s.next.Delete(ctx, r)
}

// Pids lists the process IDs inside a container. Forwards to the wrapped service.
func (s *Service) Pids(ctx context.Context, r *task.PidsRequest) (*task.PidsResponse, error) {
	return s.next.Pids(ctx, r)
}

// Pause suspends a container. Forwards to the wrapped service.
func (s *Service) Pause(ctx context.Context, r *task.PauseRequest) (*types.Empty, error) {
	return s.next.Pause(ctx, r)
}

// Resume resumes a suspended container. Forwards to the wrapped service.
func (s *Service) Resume(ctx context.Context, r *task.ResumeRequest) (*types.Empty, error) {
	return s.next.Resume(ctx, r)
}

// Checkpoint checkpoints a container. Upstream runsc returns ErrNotImplemented
// here; sandbawks pause uses a direct `runsc checkpoint` shell-out instead.
// Forwards to the wrapped service regardless.
func (s *Service) Checkpoint(ctx context.Context, r *task.CheckpointTaskRequest) (*types.Empty, error) {
	return s.next.Checkpoint(ctx, r)
}

// Kill signals a process. Forwards to the wrapped service.
func (s *Service) Kill(ctx context.Context, r *task.KillRequest) (*types.Empty, error) {
	return s.next.Kill(ctx, r)
}

// Exec starts an additional process inside a container. Forwards to the wrapped service.
func (s *Service) Exec(ctx context.Context, r *task.ExecProcessRequest) (*types.Empty, error) {
	return s.next.Exec(ctx, r)
}

// ResizePty resizes a process's pseudo-terminal. Forwards to the wrapped service.
func (s *Service) ResizePty(ctx context.Context, r *task.ResizePtyRequest) (*types.Empty, error) {
	return s.next.ResizePty(ctx, r)
}

// CloseIO closes a process's I/O streams. Forwards to the wrapped service.
func (s *Service) CloseIO(ctx context.Context, r *task.CloseIORequest) (*types.Empty, error) {
	return s.next.CloseIO(ctx, r)
}

// Update updates a running container's resources. Forwards to the wrapped service.
func (s *Service) Update(ctx context.Context, r *task.UpdateTaskRequest) (*types.Empty, error) {
	return s.next.Update(ctx, r)
}

// Wait blocks until a process exits. Forwards to the wrapped service.
func (s *Service) Wait(ctx context.Context, r *task.WaitRequest) (*task.WaitResponse, error) {
	return s.next.Wait(ctx, r)
}

// Stats returns container resource statistics. Forwards to the wrapped service.
func (s *Service) Stats(ctx context.Context, r *task.StatsRequest) (*task.StatsResponse, error) {
	return s.next.Stats(ctx, r)
}

// Connect returns connection information for a container. Forwards to the wrapped service.
func (s *Service) Connect(ctx context.Context, r *task.ConnectRequest) (*task.ConnectResponse, error) {
	return s.next.Connect(ctx, r)
}

// Shutdown stops the shim. It drops every recorded restore intent first, then
// forwards to the wrapped service.
//
// This clear is purely defensive. A Service is conventionally discarded after
// Shutdown, so the cleared state is unobservable today; the loop exists only to
// keep the invariant ("no stale intent outlives Shutdown") true if a future
// change ever reuses a Service across Shutdown calls. It is cheap and cannot
// hurt, so it stays rather than relying on every future caller to discard.
func (s *Service) Shutdown(ctx context.Context, r *task.ShutdownRequest) (*types.Empty, error) {
	s.restoring.Range(func(key, _ any) bool {
		s.restoring.Delete(key)
		return true
	})
	return s.next.Shutdown(ctx, r)
}

// --- gVisor extension RPCs (forwarded verbatim) ---

// CreateWithFSRestore creates a container and restores its filesystem from a
// snapshot. It exists only to satisfy the extension.TaskServiceExt interface
// (so the compile-time assertion above stays meaningful) and forwards verbatim;
// the sandbawks decorator no longer dispatches to it — Create always does a
// plain create and Start drives a memory-only Restore (#232).
func (s *Service) CreateWithFSRestore(ctx context.Context, r *extension.CreateWithFSRestoreRequest) (*task.CreateTaskResponse, error) {
	return s.next.CreateWithFSRestore(ctx, r)
}

// Restore restores a checkpointed container from its memory image. Forwards to
// the wrapped service; Start dispatches here when a restore annotation was
// recorded at Create time.
func (s *Service) Restore(ctx context.Context, r *extension.RestoreRequest) (*task.StartResponse, error) {
	return s.next.Restore(ctx, r)
}
