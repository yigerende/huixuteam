package store_test

import (
	"encoding/json"
	"strings"
	"testing"

	"llm-refund/internal/store"
)

func TestWorkspaceEvidencePreservesUnknownPlanIDsAndRejectsPrivateFields(t *testing.T) {
	active := false
	d := store.RefundEvidence{Stage: "workspace_observation", Workspaces: &store.RefundWorkspaceObservation{
		Source: "auth_workspaces", Phase: "before_team_select", Result: "observed",
		Items: []store.RefundWorkspaceItem{{WorkspaceID: "workspace-1", Structure: "personal", HasActiveSubscription: &active,
			Fields: map[string]string{"workspace.subscription_plan": "unknown_pro_50x", "entitlement.plan_type": "pro", "workspace.access_token": "private-token", "account.plan_name": "https://private.example", "workspace.email": "private@example.com"}},
			{WorkspaceID: "private@example.com", Role: "private-role", Fields: map[string]string{"workspace.plan_type": strings.Repeat("x", 1000)}}},
	}}
	got := d.Sanitized()
	if got.Workspaces == nil || got.Workspaces.Items[0].Fields["workspace.subscription_plan"] != "unknown_pro_50x" || got.Workspaces.Items[0].HasActiveSubscription == nil || *got.Workspaces.Items[0].HasActiveSubscription {
		t.Fatal("lost plan evidence")
	}
	raw, _ := json.Marshal(got)
	if strings.Contains(string(raw), "private") || strings.Contains(string(raw), strings.Repeat("x", 100)) {
		t.Fatal("unsafe data survived", string(raw))
	}
	d.Stage = "export"
	if d.Sanitized().Workspaces != nil {
		t.Fatal("workspace evidence allowed outside observation stage")
	}
}

func TestWorkspaceEvidenceBoundsRowsAndRejectsInvalidPhase(t *testing.T) {
	d := store.RefundEvidence{Stage: "workspace_observation", Workspaces: &store.RefundWorkspaceObservation{
		Source: "auth_workspaces", Phase: "authenticated_before_callback", Result: "observed", Items: make([]store.RefundWorkspaceItem, 100),
	}}
	got := d.Sanitized().Workspaces
	if got == nil || len(got.Items) != 16 || !got.Truncated {
		t.Fatal("workspace rows unbounded")
	}
	d.Workspaces.Phase = "private-phase"
	if d.Sanitized().Workspaces != nil {
		t.Fatal("invalid phase persisted")
	}
}

func TestPersonalPlanEvidenceIsPrivateObservationBeforeTeamSelection(t *testing.T) {
	d := store.RefundEvidence{Stage: "workspace_observation", Workspaces: &store.RefundWorkspaceObservation{
		Source: "personal_accounts_check", Phase: "before_team_select", Result: "observed",
		Items: []store.RefundWorkspaceItem{
			{WorkspaceID: "personal", Structure: "personal", Fields: map[string]string{"account.plan_type": "future_pro_50x", "entitlement.rate_limit_multiplier": "50", "account.access_token": "private-token"}},
			{WorkspaceID: "business", Structure: "workspace", Fields: map[string]string{"account.plan_type": "private-business"}},
			{WorkspaceID: "our-team", Structure: "personal", TargetTeam: true},
		},
	}}
	got := d.Sanitized().Workspaces
	if got == nil || len(got.Items) != 1 || got.Items[0].Fields["account.plan_type"] != "future_pro_50x" || got.Items[0].Fields["entitlement.rate_limit_multiplier"] != "50" {
		t.Fatal("personal plan observation lost or mixed with team", got)
	}
	raw, _ := json.Marshal(got)
	if strings.Contains(string(raw), "private") {
		t.Fatal("private fields persisted")
	}
	d.Workspaces.Phase = "before_personal_select"
	if d.Sanitized().Workspaces != nil {
		t.Fatal("mislabelled personal plan accepted")
	}
}
