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
	m := store.RefundDiagnostic{Evidence: &store.RefundEvidence{Stage: "members_read", Side: "mother", Event: "response", Membership: &store.RefundMembershipEvidence{Scope: "full", Result: "absent", MatchedBy: "none", Pages: 2, Items: 108, Complete: true}}}
	if err = s.AppendRefundDiagnostics(ctx, j, []store.RefundDiagnostic{m, w, d}); err != nil {
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
	if err = s.Pool.QueryRow(ctx, "SELECT diagnostic FROM refund_execution_diagnostics WHERE refund_id=$1 AND diagnostic->'evidence'->>'stage'='members_read'", j.ID).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	var membership store.RefundDiagnostic
	if err = json.Unmarshal(raw, &membership); err != nil || membership.Evidence.Membership == nil || !membership.Evidence.Membership.Complete || membership.Evidence.Membership.Items != 108 {
		t.Fatal("membership evidence was lost", err)
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

func TestPersonalPlanObservationPersistsWithoutChangingOrderOrMoney(t *testing.T) {
	s, _, u := executionFixture(t, 1)
	submitRequests(t, s, u, 1)
	ctx := context.Background()
	j, err := s.ClaimRefund(ctx)
	if err != nil || j == nil {
		t.Fatal(err)
	}
	before, _ := s.Refund(ctx, u.ID, j.ID)
	d := store.RefundDiagnostic{ObservedAt: time.Now().UTC(), HTTPStatus: 200, Evidence: &store.RefundEvidence{
		Stage: "workspace_observation", Side: "child", Event: "response", Workspaces: &store.RefundWorkspaceObservation{
			Source: "personal_accounts_check", Phase: "before_team_select", Result: "observed",
			Items: []store.RefundWorkspaceItem{{WorkspaceID: "personal-fixture", Structure: "personal",
				Fields: map[string]string{"account.plan_type": "unknown_pro", "entitlement.subscription_plan": "chatgptpro_50x", "entitlement.rate_limit_multiplier": "50", "account.access_token": "private-token"}}},
		},
	}}
	if err := s.AppendRefundDiagnostics(ctx, j, []store.RefundDiagnostic{d}); err != nil {
		t.Fatal(err)
	}
	var raw []byte
	if err := s.Pool.QueryRow(ctx, "SELECT diagnostic FROM refund_execution_diagnostics WHERE refund_id=$1", j.ID).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), "chatgptpro_50x") || !strings.Contains(string(raw), "before_team_select") || strings.Contains(string(raw), "private-token") {
		t.Fatal("plan observation missing or unsafe")
	}
	after, _ := s.Refund(ctx, u.ID, j.ID)
	if before.Status != after.Status || before.FeeStatus != after.FeeStatus || !before.UpdatedAt.Equal(after.UpdatedAt) {
		t.Fatal("observation changed processing state")
	}
	var effects int
	s.Pool.QueryRow(ctx, "SELECT count(*) FROM ledger WHERE refund_id=$1 AND kind IN ('settlement','release')", j.ID).Scan(&effects)
	if effects != 0 {
		t.Fatal("observation affected balance")
	}
}
