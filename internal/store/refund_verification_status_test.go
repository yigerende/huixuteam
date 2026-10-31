package store_test

import (
	"context"
	"errors"
	"llm-refund/internal/store"
	"reflect"
	"testing"
	"time"
)

func TestVerificationStatusConfigurationDefaultsValidationAndOldClients(t *testing.T) {
	s, a, _ := executionFixture(t, 1)
	ctx := context.Background()
	cfg, err := s.RefundSettings(ctx)
	if err != nil || !reflect.DeepEqual(cfg.Recovery.VerificationHTTPStatuses, []int{404}) {
		t.Fatal(cfg, err)
	}
	for _, codes := range [][]int{{200}, {600}, {404, 404}, {-1}} {
		cfg.Recovery.VerificationHTTPStatuses = codes
		if err = s.SaveRefundSettings(ctx, a.ID, cfg); err == nil {
			t.Fatal("invalid codes accepted", codes)
		}
	}
	for _, codes := range [][]int{{404, 409, 429}, {}} {
		cfg.Recovery.VerificationHTTPStatuses = codes
		if err = s.SaveRefundSettings(ctx, a.ID, cfg); err != nil {
			t.Fatal(err)
		}
		cfg.Recovery.VerificationHTTPStatuses = nil
		if err = s.SaveRefundSettings(ctx, a.ID, cfg); err != nil {
			t.Fatal(err)
		}
		stored, err := s.RefundSettings(ctx)
		if err != nil || !reflect.DeepEqual(stored.Recovery.VerificationHTTPStatuses, codes) {
			t.Fatal("old client overwrote config", stored, err)
		}
	}
}

func TestConfiguredResponseVerifiesWithoutResubmitAndReleasesOnlyCleanMother(t *testing.T) {
	s, a, u := executionFixture(t, 1)
	ctx := context.Background()
	enableRecovery(t, s, a)
	cfg, _ := s.RefundSettings(ctx)
	cfg.Recovery.PlanFallback = &store.PlanFallbackSettings{Enabled: true, MaxCycles: 2, MaxAttempts: 3, RetrySeconds: 47}
	cfg.Recovery.VerificationHTTPStatuses = []int{404, 409}
	cfg.MotherIntervalSeconds = 17
	if err := s.SaveRefundSettings(ctx, a.ID, cfg); err != nil {
		t.Fatal(err)
	}
	submitRequests(t, s, u, 2)
	j, err := s.ClaimRefund(ctx)
	if err != nil || j == nil {
		t.Fatal(err)
	}
	prepareExport(t, s, j)
	d := store.RefundDiagnostic{Code: "export_verification_required", HTTPStatus: 409}
	if err = s.RefundCheckpoint(ctx, j, store.Checkpoint{Method: "merge_v2_verify", Diagnostic: &d}); err != nil {
		t.Fatal(err)
	}
	if err = s.RefundCheckpoint(ctx, j, store.Checkpoint{Method: "merge_v2_rejected"}); !errors.Is(err, store.ErrState) {
		t.Fatal("checkpoint cleared", err)
	}
	if err = s.FinishRefundAttempt(ctx, j, d); err != nil {
		t.Fatal(err)
	}
	view, _ := s.AdminRefundRecovery(ctx, j.ID)
	if view.NextAt == nil || time.Until(*view.NextAt) < 42*time.Second {
		t.Fatal("delay missing", view)
	}
	if next, err := s.ClaimRefund(ctx); err != nil || next != nil {
		t.Fatal("claimed before removal/delay", next, err)
	}
	cfg.Recovery.VerificationHTTPStatuses = []int{}
	if err = s.SaveRefundSettings(ctx, a.ID, cfg); err != nil {
		t.Fatal(err)
	}
	for attempt := 0; attempt < 3; attempt++ {
		next := claimFallback(t, s, j.ID)
		if !next.VerificationOnly || next.Mother.ID != j.Mother.ID || next.ChildProxy != j.ChildProxy {
			t.Fatal("lost durable policy", next)
		}
		if attempt == 0 {
			checkpoint(t, s, next, "fallback_removing")
			checkpoint(t, s, next, "fallback_removed")
		}
		checkpoint(t, s, next, "fallback_login")
		checkpoint(t, s, next, "fallback_plan_started")
		if err = s.RefundCheckpoint(ctx, next, store.Checkpoint{Method: "fallback_paid", UserID: "child-user"}); !errors.Is(err, store.ErrState) {
			t.Fatal("allowed reinvite", err)
		}
		checkpoint(t, s, next, "fallback_paid_unconfirmed")
		if err = s.FinishRefundAttempt(ctx, next, store.RefundDiagnostic{Code: "fallback_paid_unconfirmed"}); err != nil {
			t.Fatal(err)
		}
	}
	r, _ := s.Refund(ctx, u.ID, j.ID)
	if r.Status != "failed" || r.FeeStatus != "released" {
		t.Fatal("unconfirmed outcome changed money", r)
	}
	view, _ = s.AdminRefundRecovery(ctx, j.ID)
	if view.NextAt != nil {
		t.Fatal("unbounded verification", view)
	}
	if next, err := s.ClaimRefund(ctx); err != nil || next != nil {
		t.Fatal("mother interval ignored", next, err)
	}
	if _, err = s.Pool.Exec(ctx, "UPDATE refund_execution SET mother_released_at=now()-interval '18 seconds' WHERE refund_id=$1", j.ID); err != nil {
		t.Fatal(err)
	}
	next, err := s.ClaimRefund(ctx)
	if err != nil || next == nil || next.ID == j.ID || next.Mother.ID != j.Mother.ID {
		t.Fatal("clean mother still blocked", next, err)
	}
	if err = s.ResumeRefund(ctx, a.ID, j.ID, "confirm_rejected", "fixture manual review"); err == nil {
		t.Fatal("resubmission allowed after mother release")
	}
	// Auto-ended services cannot be resumed or charged later.
	if err = s.ResumeRefund(ctx, a.ID, j.ID, "verify_only", "fixture manual verification"); err == nil {
		t.Fatal("auto-ended order resumed")
	}
}

func TestConfiguredResponseFreeSettlesOnceWithoutAcceptance(t *testing.T) {
	s, _, u, old := fallbackFixture(t)
	ctx := context.Background()
	// Simulate the persisted configured-status checkpoint before delayed recovery.
	if _, err := s.Pool.Exec(ctx, "UPDATE refund_execution SET verification_only=true WHERE refund_id=$1", old.ID); err != nil {
		t.Fatal(err)
	}
	j := claimFallback(t, s, old.ID)
	for _, method := range []string{"fallback_removing", "fallback_removed", "fallback_login", "fallback_plan_started", "fallback_free"} {
		checkpoint(t, s, j, method)
	}
	if err := s.RefundCheckpoint(ctx, j, store.Checkpoint{Method: "fallback_free", UserID: "child-user"}); !errors.Is(err, store.ErrState) {
		t.Fatal("duplicate settlement", err)
	}
	r, err := s.Refund(ctx, u.ID, j.ID)
	if err != nil || r.Status != "succeeded" || r.FeeStatus != "settled" {
		t.Fatal(r, err)
	}
	var accepted bool
	var count int
	if err = s.Pool.QueryRow(ctx, "SELECT accepted_at IS NOT NULL FROM refund_execution WHERE refund_id=$1", j.ID).Scan(&accepted); err != nil || accepted {
		t.Fatal("invented acceptance", accepted, err)
	}
	if err = s.Pool.QueryRow(ctx, "SELECT count(*) FROM ledger WHERE refund_id=$1 AND kind='settlement'", j.ID).Scan(&count); err != nil || count != 1 {
		t.Fatal("wrong settlement count", count, err)
	}
}
