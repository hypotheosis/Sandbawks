package wrapper

import (
	"context"
	"testing"

	task "github.com/containerd/containerd/api/runtime/task/v2"
	types "github.com/containerd/containerd/v2/pkg/protobuf/types"

	"gvisor.dev/gvisor/pkg/shim/v1/extension"
)

// recordingService is a fake extension.TaskServiceExt that appends the name of
// every invoked method to calls. It lets the table test below assert that each
// Service method forwards to exactly the matching method on the wrapped
// service. Every method returns zero-value responses with a nil error; the
// table test only inspects which methods fired, never the payload.
//
// calls is a slice rather than a single "last call" string on purpose: as of
// Task 2 each decorator method forwards to exactly one upstream method, so the
// test asserts len(calls) == 1. Task 3/4 add methods that may forward to a
// different RPC (or, on a dispatch miss, none), and a single overwritten field
// would hide a stray second forward; recording the full sequence lets those
// tasks assert call order without touching this harness.
type recordingService struct {
	calls []string

	// restoreReqs records every Restore request in call order, mirroring calls,
	// so the Task 4 dispatch tests can assert on the RestoreConfig the decorator
	// built (image path plus the Direct and Background flags) per dispatch. A
	// slice rather than a single "last request" field is deliberate: the
	// cross-contamination test fires two Restores and must pin each to its own
	// container's image path, so that reordering or batching the calls cannot let
	// one container's request silently satisfy another's assertion.
	restoreReqs []*extension.RestoreRequest
}

// Compile-time assertion that the fake implements the full interface, so it
// stays a drop-in for the real runsc service if gVisor extends TaskServiceExt.
var _ extension.TaskServiceExt = (*recordingService)(nil)

func (r *recordingService) State(context.Context, *task.StateRequest) (*task.StateResponse, error) {
	r.calls = append(r.calls, "State")
	return nil, nil
}

func (r *recordingService) Create(context.Context, *task.CreateTaskRequest) (*task.CreateTaskResponse, error) {
	r.calls = append(r.calls, "Create")
	return nil, nil
}

func (r *recordingService) Start(context.Context, *task.StartRequest) (*task.StartResponse, error) {
	r.calls = append(r.calls, "Start")
	return nil, nil
}

func (r *recordingService) Delete(context.Context, *task.DeleteRequest) (*task.DeleteResponse, error) {
	r.calls = append(r.calls, "Delete")
	return nil, nil
}

func (r *recordingService) Pids(context.Context, *task.PidsRequest) (*task.PidsResponse, error) {
	r.calls = append(r.calls, "Pids")
	return nil, nil
}

func (r *recordingService) Pause(context.Context, *task.PauseRequest) (*types.Empty, error) {
	r.calls = append(r.calls, "Pause")
	return nil, nil
}

func (r *recordingService) Resume(context.Context, *task.ResumeRequest) (*types.Empty, error) {
	r.calls = append(r.calls, "Resume")
	return nil, nil
}

func (r *recordingService) Checkpoint(context.Context, *task.CheckpointTaskRequest) (*types.Empty, error) {
	r.calls = append(r.calls, "Checkpoint")
	return nil, nil
}

func (r *recordingService) Kill(context.Context, *task.KillRequest) (*types.Empty, error) {
	r.calls = append(r.calls, "Kill")
	return nil, nil
}

func (r *recordingService) Exec(context.Context, *task.ExecProcessRequest) (*types.Empty, error) {
	r.calls = append(r.calls, "Exec")
	return nil, nil
}

func (r *recordingService) ResizePty(context.Context, *task.ResizePtyRequest) (*types.Empty, error) {
	r.calls = append(r.calls, "ResizePty")
	return nil, nil
}

func (r *recordingService) CloseIO(context.Context, *task.CloseIORequest) (*types.Empty, error) {
	r.calls = append(r.calls, "CloseIO")
	return nil, nil
}

func (r *recordingService) Update(context.Context, *task.UpdateTaskRequest) (*types.Empty, error) {
	r.calls = append(r.calls, "Update")
	return nil, nil
}

func (r *recordingService) Wait(context.Context, *task.WaitRequest) (*task.WaitResponse, error) {
	r.calls = append(r.calls, "Wait")
	return nil, nil
}

func (r *recordingService) Stats(context.Context, *task.StatsRequest) (*task.StatsResponse, error) {
	r.calls = append(r.calls, "Stats")
	return nil, nil
}

func (r *recordingService) Connect(context.Context, *task.ConnectRequest) (*task.ConnectResponse, error) {
	r.calls = append(r.calls, "Connect")
	return nil, nil
}

func (r *recordingService) Shutdown(context.Context, *task.ShutdownRequest) (*types.Empty, error) {
	r.calls = append(r.calls, "Shutdown")
	return nil, nil
}

func (r *recordingService) CreateWithFSRestore(_ context.Context, _ *extension.CreateWithFSRestoreRequest) (*task.CreateTaskResponse, error) {
	r.calls = append(r.calls, "CreateWithFSRestore")
	return nil, nil
}

func (r *recordingService) Restore(_ context.Context, req *extension.RestoreRequest) (*task.StartResponse, error) {
	r.calls = append(r.calls, "Restore")
	r.restoreReqs = append(r.restoreReqs, req)
	return nil, nil
}

// TestServiceForwardsEveryRPC verifies that each of the 17 standard task RPCs
// plus the 2 gVisor extension RPCs forwards through the Service decorator to
// the wrapped service exactly once. A missing or mis-wired forward (e.g. a
// future Task 3/4 edit that drops a pass-through or forwards twice) makes the
// matching row fail.
func TestServiceForwardsEveryRPC(t *testing.T) {
	ctx := context.Background()

	// Each row invokes one Service method; the want field is the single
	// method name the wrapped service must observe.
	cases := []struct {
		want   string
		invoke func(s *Service) error
	}{
		{"State", func(s *Service) error { _, err := s.State(ctx, &task.StateRequest{}); return err }},
		{"Create", func(s *Service) error { _, err := s.Create(ctx, &task.CreateTaskRequest{}); return err }},
		{"Start", func(s *Service) error { _, err := s.Start(ctx, &task.StartRequest{}); return err }},
		{"Delete", func(s *Service) error { _, err := s.Delete(ctx, &task.DeleteRequest{}); return err }},
		{"Pids", func(s *Service) error { _, err := s.Pids(ctx, &task.PidsRequest{}); return err }},
		{"Pause", func(s *Service) error { _, err := s.Pause(ctx, &task.PauseRequest{}); return err }},
		{"Resume", func(s *Service) error { _, err := s.Resume(ctx, &task.ResumeRequest{}); return err }},
		{"Checkpoint", func(s *Service) error { _, err := s.Checkpoint(ctx, &task.CheckpointTaskRequest{}); return err }},
		{"Kill", func(s *Service) error { _, err := s.Kill(ctx, &task.KillRequest{}); return err }},
		{"Exec", func(s *Service) error { _, err := s.Exec(ctx, &task.ExecProcessRequest{}); return err }},
		{"ResizePty", func(s *Service) error { _, err := s.ResizePty(ctx, &task.ResizePtyRequest{}); return err }},
		{"CloseIO", func(s *Service) error { _, err := s.CloseIO(ctx, &task.CloseIORequest{}); return err }},
		{"Update", func(s *Service) error { _, err := s.Update(ctx, &task.UpdateTaskRequest{}); return err }},
		{"Wait", func(s *Service) error { _, err := s.Wait(ctx, &task.WaitRequest{}); return err }},
		{"Stats", func(s *Service) error { _, err := s.Stats(ctx, &task.StatsRequest{}); return err }},
		{"Connect", func(s *Service) error { _, err := s.Connect(ctx, &task.ConnectRequest{}); return err }},
		{"Shutdown", func(s *Service) error { _, err := s.Shutdown(ctx, &task.ShutdownRequest{}); return err }},
		{"CreateWithFSRestore", func(s *Service) error {
			_, err := s.CreateWithFSRestore(ctx, &extension.CreateWithFSRestoreRequest{})
			return err
		}},
		{"Restore", func(s *Service) error {
			_, err := s.Restore(ctx, &extension.RestoreRequest{})
			return err
		}},
	}

	for _, tc := range cases {
		t.Run(tc.want, func(t *testing.T) {
			fake := &recordingService{}
			s := &Service{next: fake}
			if err := tc.invoke(s); err != nil {
				t.Fatalf("%s forwarded but returned error: %v", tc.want, err)
			}
			if len(fake.calls) != 1 || fake.calls[0] != tc.want {
				t.Fatalf("Service.%s forwarded as %v, want exactly [%q]", tc.want, fake.calls, tc.want)
			}
		})
	}
}

// TestNewServiceWrapsNext verifies the constructor returns a *Service that
// holds the supplied service as its forwarding target and never opts out
// (nil, nil) of handling a task.
func TestNewServiceWrapsNext(t *testing.T) {
	fake := &recordingService{}
	ext, err := NewService(context.Background(), fake, &task.CreateTaskRequest{})
	if err != nil {
		t.Fatalf("NewService returned error: %v", err)
	}
	s, ok := ext.(*Service)
	if !ok {
		t.Fatalf("NewService returned %T, want *Service", ext)
	}
	if s.next != fake {
		t.Fatal("NewService did not wrap the supplied service")
	}
}
