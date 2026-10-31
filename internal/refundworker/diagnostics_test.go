package refundworker

import (
	"context"
	"errors"
	"llm-refund/internal/store"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func TestDiagnosticQueueDoesNotBlockOnSlowDatabase(t *testing.T) {
	blocked, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	calls := 0
	emit, closeQueue := diagnosticQueue("fixture", func(c store.Checkpoint) error {
		if calls == 0 {
			close(blocked)
			<-release
		}
		calls++
		if calls == 9 {
			close(done)
		}
		return nil
	})
	for i := 0; i < 16; i++ {
		emit(store.RefundDiagnostic{Code: "protocol_trace"})
	}
	<-blocked
	started := time.Now()
	for i := 0; i < 10000; i++ {
		emit(store.RefundDiagnostic{Code: "protocol_trace", Evidence: &store.RefundEvidence{Stage: "workspace_observation", Workspaces: &store.RefundWorkspaceObservation{
			Source: "auth_workspaces", Phase: "before_team_select", Result: "observed", Items: []store.RefundWorkspaceItem{{WorkspaceID: "team-1", TargetTeam: true, Fields: map[string]string{"workspace.plan_type": "team"}}},
		}}})
	}
	closeQueue()
	if time.Since(started) > time.Second {
		t.Fatal("slow diagnostics blocked producer")
	}
	close(release)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("did not drain bounded queue")
	}
}

func TestProtocolEvidenceDoesNotAwaitDatabaseAndCapturesExit(t *testing.T) {
	python, err := exec.LookPath("python")
	if err != nil {
		t.Skip("python unavailable")
	}
	script := filepath.Join(t.TempDir(), "probe.py")
	source := `import sys,json
json.loads(sys.stdin.readline())
for i in range(32):print(json.dumps({'type':'diagnostic','code':'protocol_trace','evidence':{'stage':'csrf','event':'response','reason':'csrf_or_risk_blocked','upstream_code':'private-password','source':'/private/token.py'},'http_status':403}),flush=True)
print(json.dumps({'type':'checkpoint','method':'login'}),flush=True)
assert json.loads(sys.stdin.readline())['ok']
sys.exit(9)
`
	if err = os.WriteFile(script, []byte(source), 0600); err != nil {
		t.Fatal(err)
	}
	release, entered := make(chan struct{}), make(chan struct{})
	defer close(release)
	first := true
	callback := func(c store.Checkpoint) error {
		if c.Method == "diagnostic_batch" {
			if first {
				first = false
				close(entered)
			}
			<-release
			return nil
		}
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	err = (ProtocolRunner{Python: python, Script: script}).Run(ctx, &store.RefundJob{ID: "fixture"}, callback)
	var pe *ProtocolError
	if !errors.As(err, &pe) || pe.Diagnostic.Evidence.Reason != "process_exit" || pe.Diagnostic.Evidence.ExitCode != 9 {
		t.Fatalf("missing process evidence: %v", err)
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("no async writer")
	}
}
