package store

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func planDiagnostic(at time.Time, value string) RefundDiagnostic {
	return RefundDiagnostic{ObservedAt: at, HTTPStatus: 200, Evidence: &RefundEvidence{
		Stage: "workspace_observation", Side: "child", Workspaces: &RefundWorkspaceObservation{
			Source: "personal_accounts_check", Phase: "before_team_select", Result: "observed",
			Items: []RefundWorkspaceItem{{Structure: "personal", WorkspaceID: "private-account-id", Fields: map[string]string{
				"account.plan_type": value, "entitlement.subscription_plan": "chatgptpro_50x", "entitlement.rate_limit_multiplier": "50",
				"access_token": "private-access-token", "account.plan_name": "https://private.example/secret",
			}}},
		},
	}}
}

func TestPersonalPlanViewOnlyProjectsVerifiedWhitelistedPlan(t *testing.T) {
	d := planDiagnostic(time.Now(), "pro")
	v := personalPlanView(d)
	if v == nil || v.Label != "个人套餐：pro / chatgptpro_50x · 倍率 50" || len(v.Fields) != 3 {
		t.Fatal("missing raw plan identifiers", v)
	}
	raw, _ := json.Marshal(v)
	if strings.Contains(string(raw), "private") || strings.Contains(string(raw), "account_id") || strings.Contains(string(raw), "access_token") {
		t.Fatal("non-display evidence leaked", string(raw))
	}
	d.Evidence.Workspaces.Items[0].Structure = "workspace"
	if v = personalPlanView(d); v == nil || v.Status == "observed" || len(v.Fields) != 0 {
		t.Fatal("Business reported as personal", v)
	}
	d = planDiagnostic(time.Now(), "pro")
	d.Evidence.Workspaces.Result = "missing"
	if v = personalPlanView(d); v.Label != "个人套餐未采集" || len(v.Fields) != 0 {
		t.Fatal("missing observation guessed as a plan", v)
	}
	d.Evidence.Workspaces.Source = "auth_workspaces"
	if personalPlanView(d) != nil {
		t.Fatal("login workspace metadata used as verified plan")
	}
}

func TestPersonalPlanLateLogsStayWithTheirLoginAttempt(t *testing.T) {
	base := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
	at := func(n int) *time.Time { v := base.Add(time.Duration(n) * time.Second); return &v }
	out := &RefundTimeline{GeneratedAt: *at(100), Items: []RefundTimelineItem{
		{Stage: "登", StartedAt: at(0), FinishedAt: at(20)},
		{Stage: "登", StartedAt: at(30), FinishedAt: at(60)},
		{Stage: "团", FinishedAt: at(60)},
	}}
	latest := map[int]time.Time{}
	attachTimelinePersonalPlan(out, planDiagnostic(*at(40), "pro"), latest)
	attachTimelinePersonalPlan(out, planDiagnostic(*at(10), "plus"), latest)
	attachTimelinePersonalPlan(out, planDiagnostic(*at(35), "free"), latest)
	attachTimelinePersonalPlan(out, planDiagnostic(*at(70), "wrong"), latest)
	if !strings.Contains(out.Items[0].PersonalPlan.Label, "plus") || !strings.Contains(out.Items[1].PersonalPlan.Label, "pro /") || out.Items[2].PersonalPlan != nil {
		t.Fatal("observation attached to wrong attempt", out.Items)
	}
}
