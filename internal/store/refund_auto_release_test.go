package store_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"llm-refund/internal/refundprovider"
	"llm-refund/internal/store"
)

func assertAutoRelease(t *testing.T, s *store.Store, j *store.RefundJob) {
	t.Helper()
	ctx := context.Background()
	r, err := s.Refund(ctx, j.UserID, j.ID)
	if err != nil || r.Status != "failed" || r.FeeStatus != "released" || !strings.Contains(r.Message, "释放") {
		t.Fatal(r, err)
	}
	e, err := s.RefundExecution(ctx, j.ID)
	if err != nil || !e.Released || e.Running || e.RecoveryMode != "" {
		t.Fatal(e, err)
	}
	var releases, settlements, notices int
	if err = s.Pool.QueryRow(ctx, "SELECT count(*) FILTER(WHERE kind='release'),count(*) FILTER(WHERE kind='settlement') FROM ledger WHERE refund_id=$1", j.ID).Scan(&releases, &settlements); err != nil {
		t.Fatal(err)
	}
	if err = s.Pool.QueryRow(ctx, "SELECT count(*) FROM notifications WHERE user_id=$1 AND message=$2", j.UserID, r.Email+"："+r.Message).Scan(&notices); err != nil {
		t.Fatal(err)
	}
	if releases != 1 || settlements != 0 || notices != 1 {
		t.Fatal("money/notification not exactly once", releases, settlements, notices)
	}
}

func TestAcceptedCleanupExhaustsAllIntervalsThenReleasesOnceAndPausesMother(t *testing.T) {
	s, a, u := executionFixture(t, 2)
	enableRecovery(t, s, a)
	submitRequests(t, s, u, 2)
	ctx := context.Background()
	j, _ := s.ClaimRefund(ctx)
	prepareExport(t, s, j)
	checkpoint(t, s, j, "merge_v2_accepted")
	checkpoint(t, s, j, "removing")
	d := store.RefundDiagnostic{Code: "mother_http", HTTPStatus: 401}
	original := j
	for attempt := 0; attempt <= 3; attempt++ {
		if err := s.FinishRefundAttempt(ctx, j, d); err != nil {
			t.Fatal(err)
		}
		if attempt == 3 {
			break
		}
		r, _ := s.Refund(ctx, u.ID, j.ID)
		mothers, _ := s.RefundMothers(ctx)
		for _, m := range mothers {
			if m.ID == j.Mother.ID && !m.Enabled {
				t.Fatal("mother paused before final cleanup attempt")
			}
		}
		if r.FeeStatus != "frozen" || r.Status != "processing" {
			t.Fatal("released before exhausting configured cleanup", attempt, r)
		}
		if _, _, err := s.CreateRefunds(ctx, u.ID, "blocked-before-release", []refundprovider.Account{j.Account}); !errors.Is(err, store.ErrDuplicate) {
			t.Fatal("same account resubmitted while original fee/reservation remained", err)
		}
		recoveryDue(t, s, j.ID)
		next, err := s.ClaimRefund(ctx)
		if err != nil || next == nil || next.ID != j.ID || next.RecoveryMode != "cleanup" || next.Step != "removing" {
			t.Fatal("did not keep removal-only recovery", next, err)
		}
		j = next
	}
	assertAutoRelease(t, s, j)
	var enabled bool
	s.Pool.QueryRow(ctx, "SELECT enabled FROM refund_mothers WHERE id=$1", j.Mother.ID).Scan(&enabled)
	if enabled {
		t.Fatal("known failed mother still enabled")
	}
	e, _ := s.RefundExecution(ctx, j.ID)
	if e.AcceptedAt == nil || e.Step != "removing" {
		t.Fatal("lost original acceptance/removal progress")
	}
	timeline, err := s.AdminRefundTimeline(ctx, j.ID)
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range timeline.Items {
		if item.Stage == "移" && item.Status == "已完成" {
			t.Fatal("unconfirmed cleanup shown as completed")
		}
		if item.Stage == "结" && item.Status != "已结束" {
			t.Fatal("released service shown as success", item)
		}
	}
	// A different healthy mother continues, while duplicate old completions are fenced.
	next, err := s.ClaimRefund(ctx)
	if err != nil || next == nil || next.ID == j.ID || next.Mother.ID == j.Mother.ID {
		t.Fatal("healthy queue stalled", next, err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := s.FinishRefundAttempt(ctx, original, d); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if err = s.RefundCheckpoint(ctx, j, store.Checkpoint{Method: "completed"}); !errors.Is(err, store.ErrState) {
		t.Fatal("late completion changed released service", err)
	}
	assertAutoRelease(t, s, j)
	before, _ := s.User(ctx, u.ID)
	ids, _, err := s.CreateRefunds(ctx, u.ID, "new-request-after-auto-release", []refundprovider.Account{original.Account})
	if err != nil || len(ids) != 1 || ids[0] == j.ID {
		t.Fatal("released account cannot submit normally", ids, err)
	}
	after, _ := s.User(ctx, u.ID)
	oldRefund, _ := s.Refund(ctx, u.ID, j.ID)
	if after.Frozen != before.Frozen+oldRefund.Fee {
		t.Fatal("new request fee incorrect")
	}
	// The original accepted_at must not leave this fresh request permanently queued.
	prepareExport(t, s, next)
	checkpoint(t, s, next, "merge_v2_accepted")
	checkpoint(t, s, next, "removing")
	checkpoint(t, s, next, "completed")
	fresh, err := s.ClaimRefund(ctx)
	if err != nil || fresh == nil || fresh.ID != ids[0] || fresh.Step != "assigned" {
		t.Fatal("released accepted history blocked fresh request", fresh, err)
	}
	assertAutoRelease(t, s, j)
}

func TestFinalReleasePausesOnlyProvenUnchangedMother(t *testing.T) {
	for _, tc := range []struct {
		name                  string
		d                     store.RefundDiagnostic
		clean, changed, pause bool
	}{
		{"child401", store.RefundDiagnostic{Code: "login_auth_failed", HTTPStatus: 401}, false, false, false},
		{"child_plan401", store.RefundDiagnostic{Code: "fallback_unavailable", HTTPStatus: 401, Evidence: &store.RefundEvidence{Side: "child", Stage: "fallback_plan"}}, false, false, false},
		{"identity_ambiguous", store.RefundDiagnostic{Code: "member_identity"}, false, false, false},
		{"cleanup_ambiguous", store.RefundDiagnostic{Code: "cleanup_pending"}, false, false, false},
		{"mother400", store.RefundDiagnostic{Code: "mother_http", HTTPStatus: 400}, false, false, false},
		{"mother401", store.RefundDiagnostic{Code: "mother_http", HTTPStatus: 401}, false, false, true},
		{"mother_network", store.RefundDiagnostic{Code: "mother_network", NetworkCode: 7}, false, false, true},
		{"fallback_mother401", store.RefundDiagnostic{Code: "fallback_unavailable", HTTPStatus: 401, Evidence: &store.RefundEvidence{Side: "mother", Stage: "fallback_remove"}}, false, false, true},
		{"already_clean", store.RefundDiagnostic{Code: "mother_http", HTTPStatus: 401}, true, false, false},
		{"credentials_changed", store.RefundDiagnostic{Code: "mother_http", HTTPStatus: 401}, false, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, _, u := executionFixture(t, 1)
			submitRequests(t, s, u, 1)
			ctx := context.Background()
			j, _ := s.ClaimRefund(ctx)
			checkpoint(t, s, j, "inviting")
			checkpoint(t, s, j, "invited")
			checkpoint(t, s, j, "login")
			if tc.clean {
				s.Pool.Exec(ctx, "UPDATE refund_execution SET mother_released_at=now() WHERE refund_id=$1", j.ID)
			}
			if tc.changed {
				s.Pool.Exec(ctx, "UPDATE refund_mothers SET secret=decode('010203','hex') WHERE id=$1", j.Mother.ID)
			}
			if err := s.FinishRefundAttempt(ctx, j, tc.d); err != nil {
				t.Fatal(err)
			}
			assertAutoRelease(t, s, j)
			var enabled bool
			s.Pool.QueryRow(ctx, "SELECT enabled FROM refund_mothers WHERE id=$1", j.Mother.ID).Scan(&enabled)
			if enabled == tc.pause {
				t.Fatal("incorrect mother pause", enabled)
			}
		})
	}
}

func TestInterruptedUnknownWithoutRecoveryReleasesAndPreservesSubmission(t *testing.T) {
	s, _, u := executionFixture(t, 1)
	submitRequests(t, s, u, 2)
	ctx := context.Background()
	j, _ := s.ClaimRefund(ctx)
	prepareExport(t, s, j)
	s.Pool.Exec(ctx, "UPDATE refund_execution SET lease_until=now()-interval '1 second' WHERE refund_id=$1", j.ID)
	if next, err := s.ClaimRefund(ctx); err != nil || next != nil {
		t.Fatal(next, err)
	}
	assertAutoRelease(t, s, j)
	e, _ := s.RefundExecution(ctx, j.ID)
	if e.Step != "submitting" || e.AcceptedAt != nil {
		t.Fatal("submission facts changed", e)
	}
	ids, _, err := s.CreateRefunds(ctx, u.ID, "new-key-after-unknown", []refundprovider.Account{j.Account})
	if err != nil || len(ids) != 1 {
		t.Fatal("released unknown account cannot resubmit", err)
	}
	next, err := s.ClaimRefund(ctx)
	if err != nil || next == nil || next.ID == j.ID {
		t.Fatal("next account blocked", next, err)
	}
	prepareExport(t, s, next)
	checkpoint(t, s, next, "merge_v2_accepted")
	checkpoint(t, s, next, "removing")
	checkpoint(t, s, next, "completed")
	fresh, err := s.ClaimRefund(ctx)
	if err != nil || fresh == nil || fresh.ID != ids[0] || fresh.Step != "assigned" {
		t.Fatal("released unknown history blocked fresh request", fresh, err)
	}
	assertAutoRelease(t, s, j)
}
