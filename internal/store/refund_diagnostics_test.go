package store_test

import (
	"context"
	"encoding/json"
	"llm-refund/internal/store"
	"strings"
	"testing"
	"time"
)

func TestPrivateDiagnosticsAppendAfterCompletionWithoutChangingState(t *testing.T) {
	s, _, u := executionFixture(t, 1)
	submitRequests(t, s, u, 1)
	ctx := context.Background()
	j, err := s.ClaimRefund(ctx)
	if err != nil || j == nil {
		t.Fatal(err)
	}
	prepareExport(t, s, j)
	for _, step := range []string{"merge_v2_accepted", "removing", "completed"} {
		if err = s.RefundCheckpoint(ctx, j, store.Checkpoint{Method: step, TeamID: j.Mother.TeamID, UserID: "child-user"}); err != nil {
			t.Fatal(err)
		}
	}
	before, _ := s.Refund(ctx, u.ID, j.ID)
	observed := time.Now().Add(-time.Second).UTC()
	d := store.RefundDiagnostic{Code: "login_failed", HTTPStatus: 403, NetworkCode: 7, ObservedAt: observed, Evidence: &store.RefundEvidence{Stage: "csrf", Side: "child", Event: "response", Reason: "csrf_or_risk_blocked", UpstreamCode: "private-token", Source: "/private/file.py", Exception: "private-password", Sequence: 1, Challenge: true}}
	w := store.RefundDiagnostic{ObservedAt: observed, Evidence: &store.RefundEvidence{Stage: "workspace_observation", Side: "child", Event: "response", Workspaces: &store.RefundWorkspaceObservation{
		Source: "auth_workspaces", Phase: "before_team_select", Result: "observed", Items: []store.RefundWorkspaceItem{{WorkspaceID: "our-team", TargetTeam: true, Fields: map[string]string{"workspace.plan_type": "unknown_pro_50x", "access_token": "private-token"}}},
	}}}
	if err = s.AppendRefundDiagnostics(ctx, j, []store.RefundDiagnostic{w, d}); err != nil {
		t.Fatal(err)
	}
	var raw []byte
	if err = s.Pool.QueryRow(ctx, "SELECT diagnostic FROM refund_execution_diagnostics WHERE refund_id=$1 ORDER BY id DESC LIMIT 1", j.ID).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{"private-", "/private", j.LeaseToken, j.Account.Password, j.Account.TOTPSecret} {
		if secret != "" && strings.Contains(string(raw), secret) {
			t.Fatal("secret persisted")
		}
	}
	var got map[string]json.RawMessage
	if err = json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	if len(got["context"]) == 0 || !strings.Contains(string(got["evidence"]), "csrf_or_risk_blocked") || len(got["observed_at"]) == 0 {
		t.Fatal("lost diagnostic context", string(raw))
	}
	if err = s.Pool.QueryRow(ctx, "SELECT diagnostic FROM refund_execution_diagnostics WHERE refund_id=$1 AND diagnostic->'evidence'->>'stage'='workspace_observation'", j.ID).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), "unknown_pro_50x") || !strings.Contains(string(raw), "before_team_select") || strings.Contains(string(raw), "private-token") {
		t.Fatal("workspace evidence was lost or leaked credentials", string(raw))
	}
	after, _ := s.Refund(ctx, u.ID, j.ID)
	if before.Status != after.Status || after.Status != "succeeded" || after.FeeStatus != "settled" || !before.UpdatedAt.Equal(after.UpdatedAt) {
		t.Fatal("telemetry changed business state")
	}
	var settlements int
	s.Pool.QueryRow(ctx, "SELECT count(*) FROM ledger WHERE refund_id=$1 AND kind='settlement'", j.ID).Scan(&settlements)
	if settlements != 1 {
		t.Fatal("telemetry changed settlement")
	}
}
