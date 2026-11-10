// Command containerd-shim-sandbawks-v1 is the sandbawks custom containerd
// runtime shim. It hands container lifecycle off to gVisor's runsc shim
// harness (pkg/shim/v1) and installs the sandbawks decorator
// (internal/wrapper) in front of the runsc task service via gVisor's
// extension.NewExtension hook.
//
// The decorator forwards every RPC straight to runsc, intercepting only
// restore-annotated Create (to record the staged image path) and Start (to
// dispatch a memory Restore against it) — see internal/wrapper. main itself
// only installs the hook and is independent of that dispatch logic.
package main

import (
	"context"

	containerdshim "github.com/containerd/containerd/v2/pkg/shim"
	shimv1 "gvisor.dev/gvisor/pkg/shim/v1"
	"gvisor.dev/gvisor/pkg/shim/v1/extension"

	// Registers gVisor's runsc task-service plugin with containerd as a side
	// effect of import; without it the harness has no service to construct.
	_ "gvisor.dev/gvisor/pkg/shim/v1/plugin"

	"github.com/hypotheosis/sandbawks/shim/internal/wrapper"
)

func main() {
	// Install the sandbawks decorator. gVisor's runsc service calls this hook
	// (when non-nil) with the runsc service as `next` and wraps itself in the
	// returned TaskServiceExt, so every container's RPCs flow through
	// wrapper.Service.
	//
	// This is a one-shot assignment to gVisor's package-level hook, done once
	// at process start before the harness runs and never mutated again. It is
	// deliberately confined to main: importing this package does not touch the
	// hook, so no test or library caller mutates gVisor's global state. A
	// future task that moves this wiring into a library helper must preserve
	// that "assign once, in main, before Run" contract.
	extension.NewExtension = wrapper.NewService

	// Run the standard containerd shim-v2 harness with gVisor's runsc manager.
	//
	// The runtime type "io.containerd.sandbawks.v1" follows containerd's
	// binary-resolution convention (io.containerd.<name>.<version> ->
	// containerd-shim-<name>-<version>), so it maps to this binary,
	// containerd-shim-sandbawks-v1. The "sandbawks-runsc" RuntimeClass and the
	// containerd runtime-handler registration that point kubelet at this
	// binary are wired in Plan 07 T5/T6.
	containerdshim.Run(context.Background(), shimv1.NewShimManager("io.containerd.sandbawks.v1"))
}
