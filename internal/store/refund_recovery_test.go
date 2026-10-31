package store_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"llm-refund/internal/store"
)

func enableRecovery(t *testing.T, s *store.Store, a store.User) {
	t.Helper()
	ctx := context.Background()
	cfg, err := s.RefundSettings(ctx)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Recovery = &store.RefundRecoverySettings{Enabled: true, MaxRetries: 3, VerifyIntervals: []int{30, 60, 120, 300}, CleanupIntervals: []int{15, 30, 60}, AlertSeconds: 600, CleanupFailed: true}
	cfg.Recovery.PlanFallback = &store.PlanFallbackSettings{Enabled: false, MaxCycles: 2, MaxAttempts: 3, RetrySeconds: 30}
	if err = s.SaveRefundSettings(ctx, a.ID, cfg); err != nil {
		t.Fatal(err)
	}
}
func recoveryDue(t *testing.T, s *store.Store, id string) {
	t.Helper()
	if _, err := s.Pool.Exec(context.Background(), "UPDATE refund_execution SET next_attempt_at=now()-interval '1 second' WHERE refund_id=$1", id); err != nil {
		t.Fatal(err)
	}
}
func TestUnknownRecoveryBoundedDurableExclusiveAndNeverSettles(t *testing.T) {
	s, a, u := executionFixture(t, 2)
	enableRecovery(t, s, a)
	ids := submitRequests(t, s, u, 3)
	ctx := context.Background()
	j, err := s.ClaimRefund(ctx)
	if err != nil || j == nil {
		t.Fatal(err)
	}
	prepareExport(t, s, j)
	if err = s.FinishRefundAttempt(ctx, j, store.RefundDiagnostic{Code: "export_uncertain", HTTPStatus: 504, DurationMS: 60000}); err != nil {
		t.Fatal(err)
	}
	other, err := s.ClaimRefund(ctx)
	if err != nil || other == nil || other.Mother.ID == j.Mother.ID {
		t.Fatal("other mother blocked", err)
	}
	if next, err := s.ClaimRefund(ctx); err != nil || next != nil {
		t.Fatal("interval ignored", err)
	}
	for n := 0; n < 4; n++ {
		recoveryDue(t, s, j.ID)
		next, err := s.ClaimRefund(ctx)
		if err != nil || next == nil {
			t.Fatal(n, err)
		}
		if next.Step != "submitting" || next.RecoveryMode != "verify" || next.Mother.ID != j.Mother.ID || next.ChildProxy != j.ChildProxy {
			t.Fatal("changed recovery context")
		}
		if err = s.RefundCheckpoint(ctx, next, store.Checkpoint{Method: "merge_v2_ready", TeamID: next.Mother.TeamID, UserID: "child-user"}); !errors.Is(err, store.ErrState) {
			t.Fatal("verification allowed repost", err)
		}
		if err = s.FinishRefundAttempt(ctx, next, store.RefundDiagnostic{Code: "verification_missing", HTTPStatus: 200}); err != nil {
			t.Fatal(err)
		}
	}
	r, _ := s.Refund(ctx, u.ID, j.ID)
	if r.Status != "failed" || r.FeeStatus != "released" {
		t.Fatal("unknown settled", r)
	}
	view, _ := s.AdminRefundRecovery(ctx, j.ID)
	if view.Checks != 4 || view.NextAt != nil {
		t.Fatal("unbounded polling", view)
	}
	if next, err := s.ClaimRefund(ctx); err != nil || next == nil || next.ID == j.ID {
		t.Fatal("released mother did not accept the next request", err)
	}
	var count int
	s.Pool.QueryRow(ctx, "SELECT count(*) FROM ledger WHERE refund_id=$1 AND kind='settlement'", j.ID).Scan(&count)
	if count != 0 {
		t.Fatal("settled uncertain")
	}
	_ = ids
}
func TestKnownFailureCleansBeforeReleaseAndRetriesRemainBounded(t *testing.T) {
	s, a, u := executionFixture(t, 1)
	enableRecovery(t, s, a)
	submitRequests(t, s, u, 1)
	ctx := context.Background()
	j, _ := s.ClaimRefund(ctx)
	checkpoint(t, s, j, "inviting")
	checkpoint(t, s, j, "invited")
	checkpoint(t, s, j, "login")
	for n := 0; n < 4; n++ {
		if err := s.FinishRefundAttempt(ctx, j, store.RefundDiagnostic{Code: "login_failed", Retryable: true}); err != nil {
			t.Fatal(err)
		}
		if r, _ := s.Refund(ctx, u.ID, j.ID); r.FeeStatus != "frozen" {
			t.Fatal("early release")
		}
		recoveryDue(t, s, j.ID)
		next, err := s.ClaimRefund(ctx)
		if err != nil || next == nil {
			t.Fatal(n, err)
		}
		j = next
	}
	if j.RecoveryMode != "cleanup" {
		t.Fatal("retry limit ignored")
	}
	checkpoint(t, s, j, "cleanup_started")
	checkpoint(t, s, j, "cleanup_completed")
	r, _ := s.Refund(ctx, u.ID, j.ID)
	if r.Status != "failed" || r.FeeStatus != "released" {
		t.Fatal(r)
	}
	e, _ := s.RefundExecution(ctx, j.ID)
	if !e.Released {
		t.Fatal("mother held")
	}
	if err := s.RefundCheckpoint(ctx, j, store.Checkpoint{Method: "cleanup_completed"}); !errors.Is(err, store.ErrState) {
		t.Fatal("duplicate cleanup", err)
	}
	var count int
	s.Pool.QueryRow(ctx, "SELECT count(*) FROM ledger WHERE refund_id=$1 AND kind='release'", j.ID).Scan(&count)
	if count != 1 {
		t.Fatal(count)
	}
}
func TestAcceptedCleanupRetriesOnlyRemovalAndSettlesOnce(t *testing.T) {
	s, a, u := executionFixture(t, 1)
	enableRecovery(t, s, a)
	submitRequests(t, s, u, 1)
	ctx := context.Background()
	j, _ := s.ClaimRefund(ctx)
	prepareExport(t, s, j)
	checkpoint(t, s, j, "merge_v2_accepted")
	checkpoint(t, s, j, "removing")
	if err := s.FinishRefundAttempt(ctx, j, store.RefundDiagnostic{Code: "mother_network", NetworkCode: 28}); err != nil {
		t.Fatal(err)
	}
	recoveryDue(t, s, j.ID)
	next, err := s.ClaimRefund(ctx)
	if err != nil || next == nil || next.Step != "removing" {
		t.Fatal(err)
	}
	if err = s.RefundCheckpoint(ctx, next, store.Checkpoint{Method: "login"}); !errors.Is(err, store.ErrState) {
		t.Fatal("accepted re-login")
	}
	checkpoint(t, s, next, "completed")
	if err = s.FinishRefundAttempt(ctx, j, store.RefundDiagnostic{Code: "export_uncertain"}); err != nil {
		t.Fatal(err)
	}
	r, _ := s.Refund(ctx, u.ID, j.ID)
	if r.Status != "succeeded" || r.FeeStatus != "settled" {
		t.Fatal(r)
	}
}
func TestRecoveryCrashAndParallelClaimDoNotDuplicate(t *testing.T) {
	s, a, u := executionFixture(t, 1)
	enableRecovery(t, s, a)
	submitRequests(t, s, u, 1)
	ctx := context.Background()
	j, _ := s.ClaimRefund(ctx)
	prepareExport(t, s, j)
	s.Pool.Exec(ctx, "UPDATE refund_execution SET lease_until=now()-interval '1 second' WHERE refund_id=$1", j.ID)
	if next, err := s.ClaimRefund(ctx); err != nil || next != nil {
		t.Fatal("crash not scheduled", err)
	}
	recoveryDue(t, s, j.ID)
	var wg sync.WaitGroup
	var mu sync.Mutex
	count := 0
	for n := 0; n < 8; n++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			next, err := s.ClaimRefund(ctx)
			if err != nil {
				t.Error(err)
			}
			if next != nil {
				mu.Lock()
				count++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if count != 1 {
		t.Fatal("duplicate lease", count)
	}
	if err := s.RefundCheckpoint(ctx, j, store.Checkpoint{Method: "merge_v2_accepted"}); !errors.Is(err, store.ErrState) {
		t.Fatal("stale worker accepted")
	}
}
func TestRecoveryPrivacyAlertAndSafeManualConfirmation(t *testing.T) {
	s, a, u := executionFixture(t, 1)
	enableRecovery(t, s, a)
	submitRequests(t, s, u, 1)
	ctx := context.Background()
	j, _ := s.ClaimRefund(ctx)
	prepareExport(t, s, j)
	if err := s.FinishRefundAttempt(ctx, j, store.RefundDiagnostic{Code: "export_uncertain", HTTPStatus: 504}); err != nil {
		t.Fatal(err)
	}
	s.Pool.Exec(ctx, "UPDATE refund_execution SET recovery_started_at=now()-interval '601 seconds',last_diagnostic=$2::jsonb WHERE refund_id=$1", j.ID, `{"code":"private-password https://secret.invalid", "http_status":504}`)
	timeline, err := s.AdminRefundTimeline(ctx, j.ID)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(timeline)
	for _, secret := range []string{"private-password", "secret.invalid", "http_status", "child_user_id", "lease_token"} {
		if strings.Contains(string(raw), secret) {
			t.Fatal("leaked", secret)
		}
	}
	monitor, err := s.AdminRefundMonitor(ctx, store.RefundMonitorFilter{Focus: "recovery_alert"})
	if err != nil || monitor.Summary.RecoveryAlert != 1 || len(monitor.Items) != 1 {
		t.Fatal("alert absent", err, monitor)
	}
	if err = s.ResumeRefund(ctx, a.ID, j.ID, "fail_cleaned", "attempt to drop unknown result"); err == nil {
		t.Fatal("unknown released")
	}
	if err = s.ResumeRefund(ctx, a.ID, j.ID, "confirm_accepted", "owner independently confirmed processing"); err != nil {
		t.Fatal(err)
	}
	next, err := s.ClaimRefund(ctx)
	if err != nil || next == nil || next.Step != "accepted" {
		t.Fatal("manual confirmation didn't resume", err)
	}
}

func TestRecoverySettingsValidationAndLegacyAttentionEnrollment(t *testing.T) {
	s, a, u := executionFixture(t, 1)
	enableRecovery(t, s, a)
	submitRequests(t, s, u, 1)
	ctx := context.Background()
	j, _ := s.ClaimRefund(ctx)
	prepareExport(t, s, j)
	if err := legacyAttention(t, s, j); err != nil {
		t.Fatal(err)
	}
	if next, err := s.ClaimRefund(ctx); err != nil || next != nil {
		t.Fatal(err)
	}
	view, err := s.AdminRefundRecovery(ctx, j.ID)
	if err != nil || view.NextAt == nil {
		t.Fatal("legacy not enrolled", err)
	}
	cfg, _ := s.RefundSettings(ctx)
	cfg.Recovery.VerifyIntervals = []int{0}
	if err = s.SaveRefundSettings(ctx, a.ID, cfg); err == nil {
		t.Fatal("invalid interval saved")
	}
}

func TestRecoveryPausePreservesScheduleAndProgress(t *testing.T) {
	s, a, u := executionFixture(t, 1)
	enableRecovery(t, s, a)
	submitRequests(t, s, u, 1)
	ctx := context.Background()
	j, _ := s.ClaimRefund(ctx)
	checkpoint(t, s, j, "inviting")
	if err := s.FinishRefundAttempt(ctx, j, store.RefundDiagnostic{Code: "mother_network", Retryable: true}); err != nil {
		t.Fatal(err)
	}
	r, _ := s.Refund(ctx, u.ID, j.ID)
	if r.Status != "processing" || r.Message != "处理时间延长，正在继续" {
		t.Fatal("stale progress", r)
	}
	cfg, _ := s.RefundSettings(ctx)
	cfg.Recovery.Enabled = false
	if err := s.SaveRefundSettings(ctx, a.ID, cfg); err != nil {
		t.Fatal(err)
	}
	recoveryDue(t, s, j.ID)
	view, err := s.AdminRefundRecovery(ctx, j.ID)
	if err != nil || view.Pending || view.NextAt != nil || view.Label != "自动恢复已暂停" {
		t.Fatal(view, err)
	}
	monitor, err := s.AdminRefundMonitor(ctx, store.RefundMonitorFilter{})
	if err != nil || monitor.Summary.RecoveryPending != 0 || len(monitor.Items) != 1 || monitor.Items[0].NextCheckAt != nil {
		t.Fatal(monitor, err)
	}
	if next, err := s.ClaimRefund(ctx); err != nil || next != nil {
		t.Fatal("paused retry ran", err)
	}
	cfg.Recovery.Enabled = true
	if err := s.SaveRefundSettings(ctx, a.ID, cfg); err != nil {
		t.Fatal(err)
	}
	if next, err := s.ClaimRefund(ctx); err != nil || next == nil || next.ID != j.ID {
		t.Fatal("schedule lost", err)
	}
}

func TestRecoveryClaimRechecksScheduleAfterWaitingForLock(t *testing.T) {
	s, a, u := executionFixture(t, 1)
	enableRecovery(t, s, a)
	submitRequests(t, s, u, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	j, _ := s.ClaimRefund(ctx)
	if err := s.FinishRefundAttempt(ctx, j, store.RefundDiagnostic{Code: "mother_network", Retryable: true}); err != nil {
		t.Fatal(err)
	}
	recoveryDue(t, s, j.ID)
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	if _, err = tx.Exec(ctx, "SELECT id FROM users WHERE id=$1 FOR UPDATE", u.ID); err != nil {
		t.Fatal(err)
	}
	var pid int
	if err = tx.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&pid); err != nil {
		t.Fatal(err)
	}
	type claimResult struct {
		job *store.RefundJob
		err error
	}
	done := make(chan claimResult, 1)
	go func() { job, err := s.ClaimRefund(ctx); done <- claimResult{job, err} }()
	for {
		var blocked bool
		if err = s.Pool.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE $1=ANY(pg_blocking_pids(pid)))", pid).Scan(&blocked); err != nil {
			t.Fatal(err)
		}
		if blocked {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("claim never waited for lock")
		case <-time.After(10 * time.Millisecond):
		}
	}
	if _, err = tx.Exec(ctx, "UPDATE refund_execution SET next_attempt_at=now()+interval '5 minutes' WHERE refund_id=$1", j.ID); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	result := <-done
	if result.err != nil || result.job != nil {
		t.Fatal("new retry interval bypassed", result.err)
	}
}
