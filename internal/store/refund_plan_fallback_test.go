package store_test

import (
	"context"
	"encoding/json"
	"errors"
	"llm-refund/internal/store"
	"strings"
	"testing"
	"time"
)

func fallbackFixture(t *testing.T) (*store.Store, store.User, store.User, *store.RefundJob) {
	t.Helper()
	s, a, u := executionFixture(t, 1)
	enableRecovery(t, s, a)
	cfg, err := s.RefundSettings(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	cfg.Recovery.PlanFallback = &store.PlanFallbackSettings{Enabled: true, MaxCycles: 2, MaxAttempts: 3, RetrySeconds: 30}
	if err = s.SaveRefundSettings(context.Background(), a.ID, cfg); err != nil {
		t.Fatal(err)
	}
	submitRequests(t, s, u, 1)
	j, err := s.ClaimRefund(context.Background())
	if err != nil || j == nil {
		t.Fatal(err)
	}
	prepareExport(t, s, j)
	if err = s.FinishRefundAttempt(context.Background(), j, store.RefundDiagnostic{Code: "export_uncertain", NetworkCode: 56}); err != nil {
		t.Fatal(err)
	}
	return s, a, u, j
}

func claimFallback(t *testing.T, s *store.Store, id string) *store.RefundJob {
	t.Helper()
	recoveryDue(t, s, id)
	j, err := s.ClaimRefund(context.Background())
	if err != nil || j == nil || j.RecoveryMode != "fallback" || j.Step != "submitting" {
		t.Fatalf("missing fallback %+v %v", j, err)
	}
	return j
}

func TestPlanFallbackFreeSettlesOnceWithoutInventingAcceptance(t *testing.T) {
	s, _, u, old := fallbackFixture(t)
	ctx := context.Background()
	view, err := s.AdminRefundRecovery(ctx, old.ID)
	if err != nil || view.NextAt == nil || time.Until(*view.NextAt) < 25*time.Second {
		t.Fatal("missing persisted delay", view, err)
	}
	if j, err := s.ClaimRefund(ctx); j != nil || err != nil {
		t.Fatal("ignored interval", err)
	}
	j := claimFallback(t, s, old.ID)
	if j.Mother.ID != old.Mother.ID || j.ChildProxy != old.ChildProxy {
		t.Fatal("changed resource")
	}
	if err := s.RefundCheckpoint(ctx, j, store.Checkpoint{Method: "fallback_free", UserID: "child-user"}); !errors.Is(err, store.ErrState) {
		t.Fatal("free before removal", err)
	}
	for _, m := range []string{"fallback_removing", "fallback_removed", "fallback_login", "fallback_plan_started"} {
		checkpoint(t, s, j, m)
	}
	if err := s.RefundCheckpoint(ctx, j, store.Checkpoint{Method: "fallback_free", UserID: "different"}); !errors.Is(err, store.ErrState) {
		t.Fatal("wrong identity", err)
	}
	checkpoint(t, s, j, "fallback_free")
	if err := s.RefundCheckpoint(ctx, j, store.Checkpoint{Method: "fallback_free", UserID: "child-user"}); !errors.Is(err, store.ErrState) {
		t.Fatal("duplicate completion", err)
	}
	if err := s.FinishRefundAttempt(ctx, old, store.RefundDiagnostic{Code: "export_uncertain"}); err != nil {
		t.Fatal(err)
	}
	r, err := s.Refund(ctx, u.ID, j.ID)
	if err != nil || r.Status != "succeeded" || r.FeeStatus != "settled" {
		t.Fatal(r, err)
	}
	var accepted, released bool
	var basis string
	var settlements, freezes int
	s.Pool.QueryRow(ctx, "SELECT accepted_at IS NOT NULL,released,completion_basis FROM refund_execution WHERE refund_id=$1", j.ID).Scan(&accepted, &released, &basis)
	if accepted || !released || basis != "plan_free" {
		t.Fatal("wrong facts", accepted, released, basis)
	}
	s.Pool.QueryRow(ctx, "SELECT count(*) FILTER(WHERE kind='settlement'),count(*) FILTER(WHERE kind='freeze') FROM ledger WHERE refund_id=$1", j.ID).Scan(&settlements, &freezes)
	if settlements != 1 || freezes != 1 {
		t.Fatal("duplicate money", settlements, freezes)
	}
	timeline, err := s.AdminRefundTimeline(ctx, j.ID)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(timeline)
	if !strings.Contains(string(raw), "个人套餐为 Free") || strings.Contains(string(raw), "\"status\":\"已受理\"") {
		t.Fatal("incorrect outcome timeline", string(raw))
	}
	public, _ := json.Marshal(r)
	for _, secret := range []string{"fallback", "Free", "child-user", "team-fixture", "private-proxy", "export_"} {
		if strings.Contains(string(public), secret) || strings.Contains(string(raw), secret) && secret != "fallback" && secret != "Free" {
			t.Fatal("projection leak", secret)
		}
	}
}

func TestPlanFallbackPaidReinvitesSameRequestWithBoundedCycles(t *testing.T) {
	s, _, u, old := fallbackFixture(t)
	ctx := context.Background()
	for round := 0; round < 2; round++ {
		j := claimFallback(t, s, old.ID)
		for _, m := range []string{"fallback_removing", "fallback_removed", "fallback_login", "fallback_plan_started", "fallback_paid"} {
			checkpoint(t, s, j, m)
		}
		if err := s.FinishRefundAttempt(ctx, j, store.RefundDiagnostic{Code: "fallback_paid", Retryable: true}); err != nil {
			t.Fatal(err)
		}
		if next, err := s.ClaimRefund(ctx); next != nil || err != nil {
			t.Fatal("immediate re-invite", err)
		}
		recoveryDue(t, s, j.ID)
		next, err := s.ClaimRefund(ctx)
		if err != nil || next == nil || next.Step != "inviting" || next.ID != old.ID || next.Mother.ID != old.Mother.ID || next.ChildProxy != old.ChildProxy {
			t.Fatal("lost request context", next, err)
		}
		prepareExport(t, s, next)
		if err = s.FinishRefundAttempt(ctx, next, store.RefundDiagnostic{Code: "export_uncertain", HTTPStatus: 502}); err != nil {
			t.Fatal(err)
		}
	}
	r, _ := s.Refund(ctx, u.ID, old.ID)
	v, _ := s.AdminRefundRecovery(ctx, old.ID)
	if r.Status != "failed" || r.FeeStatus != "released" || v.NextAt != nil || v.Fallbacks != 2 {
		t.Fatal("cycle cap ignored", r, v)
	}
	if j, err := s.ClaimRefund(ctx); j != nil || err != nil {
		t.Fatal("cycle restarted", err)
	}
	var n int
	s.Pool.QueryRow(ctx, "SELECT count(*) FROM ledger WHERE refund_id=$1", old.ID).Scan(&n)
	if n != 2 {
		t.Fatal("expected one freeze and one release", n)
	}
}

func TestPlanFallbackFailureResumeAndDisablePreserveRemovalAndLimit(t *testing.T) {
	s, a, u, old := fallbackFixture(t)
	ctx := context.Background()
	j := claimFallback(t, s, old.ID)
	checkpoint(t, s, j, "fallback_removing")
	checkpoint(t, s, j, "fallback_removed")
	if err := s.FinishRefundAttempt(ctx, j, store.RefundDiagnostic{Code: "fallback_unavailable", HTTPStatus: 401}); err != nil {
		t.Fatal(err)
	}
	cfg, _ := s.RefundSettings(ctx)
	cfg.Recovery.PlanFallback.Enabled = false
	if err := s.SaveRefundSettings(ctx, a.ID, cfg); err != nil {
		t.Fatal(err)
	}
	recoveryDue(t, s, j.ID)
	if next, err := s.ClaimRefund(ctx); next != nil || err != nil {
		t.Fatal("disabled still runs", err)
	}
	cfg.Recovery.PlanFallback.Enabled = true
	if err := s.SaveRefundSettings(ctx, a.ID, cfg); err != nil {
		t.Fatal(err)
	}
	for attempt := 1; attempt < 3; attempt++ {
		next := claimFallback(t, s, j.ID)
		if next.FallbackPhase != "checking" {
			t.Fatal("lost removal", next.FallbackPhase)
		}
		if err := s.RefundCheckpoint(ctx, j, store.Checkpoint{Method: "fallback_free", UserID: "child-user"}); !errors.Is(err, store.ErrState) {
			t.Fatal("old lease completed", err)
		}
		if err := s.FinishRefundAttempt(ctx, next, store.RefundDiagnostic{Code: "fallback_unavailable"}); err != nil {
			t.Fatal(err)
		}
	}
	r, _ := s.Refund(ctx, u.ID, j.ID)
	v, _ := s.AdminRefundRecovery(ctx, j.ID)
	if r.Status != "failed" || r.FeeStatus != "released" || v.NextAt != nil {
		t.Fatal("query failure falsely completed/retried", r, v)
	}
}

func TestTLSNotSentUsesConfiguredRetryBeforeFallback(t *testing.T) {
	s, a, u := executionFixture(t, 1)
	enableRecovery(t, s, a)
	submitRequests(t, s, u, 1)
	ctx := context.Background()
	j, err := s.ClaimRefund(ctx)
	if err != nil || j == nil {
		t.Fatal(err)
	}
	prepareExport(t, s, j)
	checkpoint(t, s, j, "merge_v2_rejected")
	if err = s.FinishRefundAttempt(ctx, j, store.RefundDiagnostic{Code: "export_not_sent", NetworkCode: 35, Retryable: true, Evidence: &store.RefundEvidence{TransportState: "tls_before_request"}}); err != nil {
		t.Fatal(err)
	}
	if next, err := s.ClaimRefund(ctx); next != nil || err != nil {
		t.Fatal("retry delay bypassed", err)
	}
	recoveryDue(t, s, j.ID)
	next, err := s.ClaimRefund(ctx)
	if err != nil || next == nil || next.RecoveryMode != "retry" || next.Step != "login" {
		t.Fatal(next, err)
	}
}

func TestFallbackSettingsOmittedByOldClientRemainDisabled(t *testing.T) {
	s, a, _ := executionFixture(t, 1)
	ctx := context.Background()
	enableRecovery(t, s, a)
	cfg, err := s.RefundSettings(ctx)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Recovery.PlanFallback = nil
	cfg.Recovery.MaxRetries = 4
	if err = s.SaveRefundSettings(ctx, a.ID, cfg); err != nil {
		t.Fatal(err)
	}
	stored, err := s.RefundSettings(ctx)
	if err != nil || stored.Recovery.PlanFallback == nil || stored.Recovery.PlanFallback.Enabled || stored.Recovery.MaxRetries != 4 {
		t.Fatal(stored, err)
	}
}

func TestFallbackDisabledWhileWaitingForPaidReinvite(t *testing.T) {
	s, a, _, old := fallbackFixture(t)
	ctx := context.Background()
	j := claimFallback(t, s, old.ID)
	for _, m := range []string{"fallback_removing", "fallback_removed", "fallback_login", "fallback_plan_started", "fallback_paid"} {
		checkpoint(t, s, j, m)
	}
	if err := s.FinishRefundAttempt(ctx, j, store.RefundDiagnostic{Code: "fallback_paid", Retryable: true}); err != nil {
		t.Fatal(err)
	}
	cfg, _ := s.RefundSettings(ctx)
	cfg.Recovery.PlanFallback.Enabled = false
	if err := s.SaveRefundSettings(ctx, a.ID, cfg); err != nil {
		t.Fatal(err)
	}
	recoveryDue(t, s, j.ID)
	if next, err := s.ClaimRefund(ctx); next != nil || err != nil {
		t.Fatal("disabled reinvite claimed", next, err)
	}
	view, err := s.AdminRefundRecovery(ctx, j.ID)
	if err != nil || view.Pending || view.NextAt != nil {
		t.Fatal("disabled plan shown pending", view, err)
	}
	cfg.Recovery.PlanFallback.Enabled = true
	if err := s.SaveRefundSettings(ctx, a.ID, cfg); err != nil {
		t.Fatal(err)
	}
	if next, err := s.ClaimRefund(ctx); next == nil || err != nil || next.ID != j.ID {
		t.Fatal("enabled reinvite not resumed", next, err)
	}
}
