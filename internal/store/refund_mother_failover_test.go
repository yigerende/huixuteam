package store_test

import (
	"context"
	"llm-refund/internal/store"
	"sync"
	"testing"
	"time"
)

func preflightFailed(t *testing.T, s *store.Store, j *store.RefundJob) {
	t.Helper()
	if err := s.FinishRefundAttempt(context.Background(), j, store.RefundDiagnostic{Code: "mother_preflight_auth", HTTPStatus: 401}); err != nil {
		t.Fatal(err)
	}
}

func TestMotherFailoverQueuesPreservesFeeProxyAndSerializes(t *testing.T) {
	s, a, u := executionFixture(t, 2)
	ctx := context.Background()
	submitRequests(t, s, u, 3)
	j, err := s.ClaimRefund(ctx)
	if err != nil || j == nil {
		t.Fatal(err)
	}
	other, err := s.ClaimRefund(ctx)
	if err != nil || other == nil {
		t.Fatal(err)
	}
	preflightFailed(t, s, j)
	r, _ := s.Refund(ctx, u.ID, j.ID)
	if r.Status != "queued" || r.FeeStatus != "frozen" {
		t.Fatal(r)
	}
	v, _ := s.AdminRefundRecovery(ctx, j.ID)
	if v == nil || !v.Pending || v.NextAt == nil || v.Switches != 1 {
		t.Fatal(v)
	}
	// Even after the wait expires, a busy replacement cannot be stolen.
	recoveryDue(t, s, j.ID)
	if next, e := s.ClaimRefund(ctx); e != nil || next != nil {
		t.Fatal("busy mother reused", e)
	}
	prepareExport(t, s, other)
	checkpoint(t, s, other, "merge_v2_accepted")
	checkpoint(t, s, other, "removing")
	checkpoint(t, s, other, "completed")
	cfg, _ := s.RefundSettings(ctx)
	cfg.MotherIntervalSeconds = 30
	if err = s.SaveRefundSettings(ctx, a.ID, cfg); err != nil {
		t.Fatal(err)
	}
	if next, e := s.ClaimRefund(ctx); e != nil || next != nil {
		t.Fatal("mother interval skipped", e)
	}
	s.Pool.Exec(ctx, "UPDATE refund_execution SET completed_at=now()-interval '31 seconds' WHERE refund_id=$1", other.ID)
	var jobs []*store.RefundJob
	var mu sync.Mutex
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			next, e := s.ClaimRefund(ctx)
			if e != nil {
				t.Error(e)
			}
			if next != nil {
				mu.Lock()
				jobs = append(jobs, next)
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if len(jobs) != 1 {
		t.Fatalf("claims=%d", len(jobs))
	}
	next := jobs[0]
	if next.ID != j.ID || next.Mother.ID != other.Mother.ID || next.ChildProxy != j.ChildProxy || next.Step != "assigned" {
		t.Fatal("wrong order/context")
	}
	// An old worker result cannot move or settle the replacement execution.
	preflightFailed(t, s, j)
	current, _ := s.RefundExecution(ctx, j.ID)
	if !current.Running || current.MotherID != next.Mother.ID {
		t.Fatal(current)
	}
	prepareExport(t, s, next)
	checkpoint(t, s, next, "merge_v2_accepted")
	checkpoint(t, s, next, "removing")
	checkpoint(t, s, next, "completed")
	var freezes, settlements int
	s.Pool.QueryRow(ctx, "SELECT count(*) FILTER(WHERE kind='freeze'),count(*) FILTER(WHERE kind='settlement') FROM ledger WHERE refund_id=$1", j.ID).Scan(&freezes, &settlements)
	if freezes != 1 || settlements != 1 {
		t.Fatal(freezes, settlements)
	}
}

func TestMotherFailoverDelayPauseAndCancellation(t *testing.T) {
	s, a, u := executionFixture(t, 2)
	ctx := context.Background()
	submitRequests(t, s, u, 1)
	j, _ := s.ClaimRefund(ctx)
	preflightFailed(t, s, j)
	if next, e := s.ClaimRefund(ctx); e != nil || next != nil {
		t.Fatal("wait skipped", e)
	}
	recoveryDue(t, s, j.ID)
	cfg, _ := s.RefundSettings(ctx)
	cfg.MotherFailover.Enabled = false
	s.SaveRefundSettings(ctx, a.ID, cfg)
	if next, e := s.ClaimRefund(ctx); e != nil || next != nil {
		t.Fatal("disabled switch ran", e)
	}
	v, _ := s.AdminRefundRecovery(ctx, j.ID)
	if v.Pending || v.Label != "换号调度已暂停" {
		t.Fatal(v)
	}
	cfg.MotherFailover.Enabled = true
	cfg.Enabled = false
	s.SaveRefundSettings(ctx, a.ID, cfg)
	if next, e := s.ClaimRefund(ctx); e != nil || next != nil {
		t.Fatal("disabled queue ran", e)
	}
	if err := s.Transition(ctx, u.ID, j.ID, "cancelled", ""); err != nil {
		t.Fatal(err)
	}
	cfg.Enabled = true
	s.SaveRefundSettings(ctx, a.ID, cfg)
	if next, e := s.ClaimRefund(ctx); e != nil || next != nil {
		t.Fatal("cancelled request ran", e)
	}
	r, _ := s.Refund(ctx, u.ID, j.ID)
	if r.FeeStatus != "released" {
		t.Fatal(r)
	}
}

func TestMotherFailoverBoundedAndNeverSwitchesAfterInvite(t *testing.T) {
	s, a, u := executionFixture(t, 3)
	ctx := context.Background()
	cfg, _ := s.RefundSettings(ctx)
	cfg.MotherFailover.MaxSwitches = 1
	if e := s.SaveRefundSettings(ctx, a.ID, cfg); e != nil {
		t.Fatal(e)
	}
	submitRequests(t, s, u, 1)
	j, _ := s.ClaimRefund(ctx)
	preflightFailed(t, s, j)
	recoveryDue(t, s, j.ID)
	next, e := s.ClaimRefund(ctx)
	if e != nil || next == nil || next.Mother.ID == j.Mother.ID {
		t.Fatal(e)
	}
	preflightFailed(t, s, next)
	r, _ := s.Refund(ctx, u.ID, j.ID)
	if r.Status != "failed" || r.FeeStatus != "released" {
		t.Fatal(r)
	}
	if after, e := s.ClaimRefund(ctx); e != nil || after != nil {
		t.Fatal("limit ignored", e)
	}
	// A preflight code cannot requeue an execution whose invitation began.
	s2, _, u2 := executionFixture(t, 2)
	submitRequests(t, s2, u2, 1)
	j2, _ := s2.ClaimRefund(ctx)
	checkpoint(t, s2, j2, "inviting")
	preflightFailed(t, s2, j2)
	r, _ = s2.Refund(ctx, u2.ID, j2.ID)
	if r.Status == "queued" {
		t.Fatal("unsafe switch")
	}
	e2, _ := s2.RefundExecution(ctx, j2.ID)
	if !e2.Released {
		t.Fatal("disabled recovery did not release after stopping")
	}
}

func TestExpiredAndRejectedMothersSkippedUntilCredentialChanges(t *testing.T) {
	s, a, u := executionFixture(t, 2)
	ctx := context.Background()
	ms, _ := s.RefundMothers(ctx)
	s.Pool.Exec(ctx, "UPDATE refund_mothers SET token_expires_at=now()-interval '1 second' WHERE id=$1", ms[0].ID)
	submitRequests(t, s, u, 2)
	j, e := s.ClaimRefund(ctx)
	if e != nil || j == nil || j.Mother.ID == ms[0].ID {
		t.Fatal(e)
	}
	preflightFailed(t, s, j)
	if next, e := s.ClaimRefund(ctx); e != nil || next != nil {
		t.Fatal("expired or rejected mother used", e)
	}
	mothers, _ := s.RefundMothers(ctx)
	var blocked store.RefundMother
	for _, m := range mothers {
		if m.ID == j.Mother.ID {
			blocked = m
		}
	}
	if !blocked.AuthBlocked {
		t.Fatal("missing blocked state")
	}
	blocked.AccessToken = "replacement-private-token"
	if _, e = s.SaveRefundMother(ctx, a.ID, blocked); e != nil {
		t.Fatal(e)
	}
	// Renewed credentials make the resource eligible again for the original
	// waiting request; never permanently exclude a mother after it is repaired.
	recoveryDue(t, s, j.ID)
	next, e := s.ClaimRefund(ctx)
	if e != nil || next == nil || next.ID != j.ID {
		t.Fatal("renewed mother not usable", e)
	}
	// A fresh successful probe also clears the authorization quarantine.
	s.Pool.Exec(ctx, "UPDATE refund_mothers SET auth_failed_fingerprint=md5(secret) WHERE id=$1", ms[0].ID)
	if e = s.SaveResourceCheck(ctx, a.ID, "mother", ms[0].ID, time.Now().Add(time.Second), store.ResourceCheck{Valid: true, Code: "ok", CheckedAt: time.Now()}); e != nil {
		t.Fatal(e)
	}
	var blockedNow bool
	s.Pool.QueryRow(ctx, "SELECT auth_failed_fingerprint IS NOT NULL FROM refund_mothers WHERE id=$1", ms[0].ID).Scan(&blockedNow)
	if blockedNow {
		t.Fatal("probe did not clear quarantine")
	}
}

func TestMotherFailoverConfigurationAndUnknownStateRemainSafe(t *testing.T) {
	s, a, _ := executionFixture(t, 1)
	ctx := context.Background()
	cfg, _ := s.RefundSettings(ctx)
	if cfg.MotherFailover == nil || !cfg.MotherFailover.Enabled || cfg.MotherFailover.MaxSwitches != 3 || cfg.MotherFailover.RetrySeconds != 30 {
		t.Fatal(cfg.MotherFailover)
	}
	for _, v := range []store.MotherFailoverSettings{{true, -1, 30}, {true, 11, 30}, {true, 3, 4}, {true, 3, 3601}} {
		cfg.MotherFailover = &v
		if e := s.SaveRefundSettings(ctx, a.ID, cfg); e == nil {
			t.Fatal("invalid config accepted", v)
		}
	}
	cfg.MotherFailover = nil
	if e := s.SaveRefundSettings(ctx, a.ID, cfg); e != nil {
		t.Fatal(e)
	}
	got, _ := s.RefundSettings(ctx)
	if got.MotherFailover.MaxSwitches != 3 {
		t.Fatal("legacy settings reset defaults")
	}
}
