package store_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"

	"llm-refund/internal/refundprovider"
	"llm-refund/internal/store"
	"llm-refund/internal/testutil"
)

func executionFixture(t *testing.T, n int) (*store.Store, store.User, store.User) {
	t.Helper()
	s := testutil.Database(t)
	a := testutil.Admin(t, s)
	u := testutil.Member(t, s, a, "customer@test.invalid", "member")
	ctx := context.Background()
	if err := s.Credit(ctx, u.Email, 90000, "fixture"); err != nil {
		t.Fatal(err)
	}
	p, err := s.SaveRefundProxy(ctx, a.ID, store.RefundProxy{Name: "Test proxy", URL: "http://user:private-proxy@proxy.invalid:8080", Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		_, err = s.SaveRefundMother(ctx, a.ID, store.RefundMother{Name: fmt.Sprint("Mother ", i), Email: fmt.Sprintf("mother%d@test.invalid", i), TeamID: fmt.Sprintf("team-fixture-%d", i), ProxyID: p, SeatType: "default", AccessToken: "private-mother-token", Enabled: true})
		if err != nil {
			t.Fatal(err)
		}
	}
	if err = s.SaveRefundSettings(ctx, a.ID, store.RefundSettings{ChildProxyID: p, Enabled: true, MaxParallel: 2, LoginAttempts: 2, RetrySeconds: 5}); err != nil {
		t.Fatal(err)
	}
	// These pre-existing tests exercise the explicit manual-recovery mode.
	if _, err = s.Pool.Exec(ctx, `UPDATE refund_settings SET recovery=jsonb_set(recovery,'{enabled}','false')`); err != nil {
		t.Fatal(err)
	}
	return s, a, u
}
func submitRequests(t *testing.T, s *store.Store, u store.User, n int) []string {
	t.Helper()
	accounts := []refundprovider.Account{}
	for i := 0; i < n; i++ {
		accounts = append(accounts, account(fmt.Sprintf("child%d@test.invalid", i)))
	}
	ids, _, err := s.CreateRefunds(context.Background(), u.ID, "execution-test-submit", accounts)
	if err != nil {
		t.Fatal(err)
	}
	return ids
}
func checkpoint(t *testing.T, s *store.Store, j *store.RefundJob, method string) {
	t.Helper()
	if err := s.RefundCheckpoint(context.Background(), j, store.Checkpoint{Method: method, TeamID: j.Mother.TeamID, UserID: "child-user"}); err != nil {
		t.Fatalf("checkpoint %s: %v", method, err)
	}
}
func prepareExport(t *testing.T, s *store.Store, j *store.RefundJob) {
	for _, method := range []string{"inviting", "invited", "login", "merge_v2_ready", "merge_v2_submitting"} {
		checkpoint(t, s, j, method)
	}
}

// Legacy records remain reviewable, but new failures no longer create attention.
func legacyAttention(t *testing.T, s *store.Store, j *store.RefundJob) error {
	t.Helper()
	_, err := s.Pool.Exec(context.Background(), "UPDATE refunds SET status='attention' WHERE id=$1", j.ID)
	if err != nil {
		return err
	}
	_, err = s.Pool.Exec(context.Background(), "UPDATE refund_execution SET lease_token='',lease_until=NULL,released=false,next_attempt_at=NULL,recovery_mode='' WHERE refund_id=$1", j.ID)
	return err
}

func TestMotherSerialMultipleMothersParallelAndOneSettlement(t *testing.T) {
	s, _, u := executionFixture(t, 2)
	submitRequests(t, s, u, 4)
	ctx := context.Background()
	var wg sync.WaitGroup
	var mu sync.Mutex
	jobs := []*store.RefundJob{}
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			j, err := s.ClaimRefund(ctx)
			if err != nil {
				t.Error(err)
			}
			if j != nil {
				mu.Lock()
				jobs = append(jobs, j)
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if len(jobs) != 2 || jobs[0].Mother.ID == jobs[1].Mother.ID {
		t.Fatalf("must claim two distinct mothers, got %d", len(jobs))
	}
	j := jobs[0]
	if err := s.Transition(ctx, u.ID, j.ID, "cancelled", "cancel"); !errors.Is(err, store.ErrState) {
		t.Fatal("running job cancelled", err)
	}
	prepareExport(t, s, j)
	if err := s.RefundCheckpoint(ctx, j, store.Checkpoint{Method: "merge_v2_submitting", TeamID: j.Mother.TeamID}); !errors.Is(err, store.ErrState) {
		t.Fatal("duplicate submit allowed", err)
	}
	checkpoint(t, s, j, "merge_v2_accepted")
	checkpoint(t, s, j, "removing")
	checkpoint(t, s, j, "completed")
	if err := s.RefundCheckpoint(ctx, j, store.Checkpoint{Method: "completed"}); !errors.Is(err, store.ErrState) {
		t.Fatal("late callback revived job", err)
	}
	user, err := s.User(ctx, u.ID)
	if err != nil || user.Balance != 87000 || user.Frozen != 9000 {
		t.Fatalf("settlement incorrect: %+v %v", user, err)
	}
	var count int
	s.Pool.QueryRow(ctx, "SELECT count(*) FROM ledger WHERE refund_id=$1 AND kind='settlement'", j.ID).Scan(&count)
	if count != 1 {
		t.Fatal("wrong settlement count", count)
	}
	next, err := s.ClaimRefund(ctx)
	if err != nil || next == nil || next.Mother.ID != j.Mother.ID {
		t.Fatal("mother not reusable after completion", err)
	}
}

func TestGenericProgressThrottledAndStopsWhenCompleted(t *testing.T) {
	s, _, u := executionFixture(t, 1)
	submitRequests(t, s, u, 1)
	ctx := context.Background()
	j, err := s.ClaimRefund(ctx)
	if err != nil || j == nil {
		t.Fatal("claim failed", err)
	}
	count := func(want int) {
		t.Helper()
		var got int
		if err := s.Pool.QueryRow(ctx, "SELECT count(*) FROM refund_events WHERE refund_id=$1 AND NOT internal AND message='处理时间延长，正在继续'", j.ID).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("progress count %d, want %d", got, want)
		}
	}
	progress := func() {
		t.Helper()
		if err := s.RefundProgress(ctx, j); err != nil {
			t.Fatal(err)
		}
	}
	age := func() {
		t.Helper()
		if _, err := s.Pool.Exec(ctx, "UPDATE refund_execution SET progress_at=now()-interval '61 seconds' WHERE refund_id=$1", j.ID); err != nil {
			t.Fatal(err)
		}
	}
	progress()
	count(0)
	age()
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := s.RefundProgress(ctx, j); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	count(1)
	progress()
	count(1)
	age()
	progress()
	count(2)
	prepareExport(t, s, j)
	checkpoint(t, s, j, "merge_v2_accepted")
	checkpoint(t, s, j, "removing")
	checkpoint(t, s, j, "completed")
	age()
	progress()
	count(2)
	r, err := s.Refund(ctx, u.ID, j.ID)
	if err != nil || r.Status != "succeeded" || r.Message != "退款服务已完成" {
		t.Fatalf("completed result changed: %+v %v", r, err)
	}
}
func TestLegacyUnknownSubmissionHoldsMotherAndExplicitRecovery(t *testing.T) {
	s, a, u := executionFixture(t, 1)
	submitRequests(t, s, u, 2)
	ctx := context.Background()
	j, err := s.ClaimRefund(ctx)
	if err != nil {
		t.Fatal(err)
	}
	prepareExport(t, s, j)
	if err = legacyAttention(t, s, j); err != nil {
		t.Fatal(err)
	}
	var next *store.RefundJob
	r, _ := s.Refund(ctx, u.ID, j.ID)
	if r.Status != "attention" || r.FeeStatus != "frozen" {
		t.Fatalf("unknown state lost: %+v", r)
	}
	next, err = s.ClaimRefund(ctx)
	if err != nil || next != nil {
		t.Fatal("reserved mother claimed by other job", err)
	}
	if err = s.ResumeRefund(ctx, a.ID, j.ID, "resume", "checked task progress"); err == nil {
		t.Fatal("unknown resume must require result confirmation")
	}
	if err = s.ResumeRefund(ctx, a.ID, j.ID, "fail_cleaned", "checked task progress"); err == nil {
		t.Fatal("unknown submission fee released")
	}
	if err = s.ResumeRefund(ctx, a.ID, j.ID, "confirm_accepted", "confirmed upstream accepted request"); err != nil {
		t.Fatal(err)
	}
	next, err = s.ClaimRefund(ctx)
	if err != nil || next == nil || next.Step != "accepted" || next.ChildUserID != "child-user" || next.Mother.ID != j.Mother.ID {
		t.Fatal("accepted restart did not preserve identity", err)
	}
	if err = s.RefundCheckpoint(ctx, next, store.Checkpoint{Method: "login"}); err == nil {
		t.Fatal("accepted job could log in to resubmit")
	}
	checkpoint(t, s, next, "removing")
	checkpoint(t, s, next, "completed")
}
func TestExpiredAcceptedJobResumesOnlyRemovalAndOldLeaseFenced(t *testing.T) {
	s, _, u := executionFixture(t, 1)
	submitRequests(t, s, u, 1)
	ctx := context.Background()
	j, _ := s.ClaimRefund(ctx)
	prepareExport(t, s, j)
	checkpoint(t, s, j, "merge_v2_accepted")
	s.Pool.Exec(ctx, "UPDATE refund_execution SET lease_until=now()-interval '1 second' WHERE refund_id=$1", j.ID)
	next, err := s.ClaimRefund(ctx)
	if err != nil || next == nil || next.Step != "accepted" {
		t.Fatal("could not recover accepted job", err)
	}
	if err = s.RefundCheckpoint(ctx, j, store.Checkpoint{Method: "removing"}); !errors.Is(err, store.ErrState) {
		t.Fatal("stale worker not fenced", err)
	}
	checkpoint(t, s, next, "removing")
	checkpoint(t, s, next, "completed")
}
func TestSettingsSecretsImmutableReservationAndPermissions(t *testing.T) {
	s, a, u := executionFixture(t, 1)
	ctx := context.Background()
	mothers, _ := s.RefundMothers(ctx)
	proxies, _ := s.RefundProxies(ctx)
	raw, _ := json.Marshal([]any{mothers, proxies})
	if bytes.Contains(raw, []byte("private-")) {
		t.Fatal("settings leaked secrets")
	}
	var ciphertext []byte
	s.Pool.QueryRow(ctx, "SELECT secret FROM refund_mothers LIMIT 1").Scan(&ciphertext)
	if bytes.Contains(ciphertext, []byte("private-mother-token")) {
		t.Fatal("mother token plaintext")
	}
	m := mothers[0]
	m.Name = "renamed"
	if _, err := s.SaveRefundMother(ctx, a.ID, m); err != nil {
		t.Fatal(err)
	}
	if _, err := s.SaveRefundProxy(ctx, u.ID, store.RefundProxy{Name: "forbidden", URL: "http://proxy.invalid:8080", Enabled: true}); !errors.Is(err, store.ErrForbidden) {
		t.Fatal("member edited proxy", err)
	}
	duplicate := m
	duplicate.ID = ""
	duplicate.AccessToken = "private-mother-token"
	duplicate.TeamID = "different-team"
	if _, err := s.SaveRefundMother(ctx, a.ID, duplicate); err == nil {
		t.Fatal("same mother configured twice")
	}
	submitRequests(t, s, u, 1)
	j, err := s.ClaimRefund(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if j.Mother.AccessToken != "private-mother-token" || j.MotherProxy != "http://user:private-proxy@proxy.invalid:8080" {
		t.Fatal("secret preservation failed")
	}
	m.TeamID = "new-team-id"
	if _, err = s.SaveRefundMother(ctx, a.ID, m); err == nil {
		t.Fatal("inflight team changed")
	}
	if err = s.DeleteRefundResource(ctx, a.ID, "mother", m.ID); err == nil {
		t.Fatal("inflight mother deleted")
	}
	if err = legacyAttention(t, s, j); err != nil {
		t.Fatal(err)
	}
	m.TeamID = j.Mother.TeamID
	m.AccessToken = "repaired-mother-token"
	if _, err = s.SaveRefundMother(ctx, a.ID, m); err != nil {
		t.Fatal("cannot repair stopped mother token", err)
	}
	if err = s.ResumeRefund(ctx, a.ID, j.ID, "fail_cleaned", "confirmed all membership and invites cleaned"); err != nil {
		t.Fatal(err)
	}
	user, _ := s.User(ctx, u.ID)
	if user.Frozen != 0 || user.Balance != 90000 {
		t.Fatal("failure did not release funds")
	}
}
func TestCancelVersusClaimAndCrossCustomerDuplicate(t *testing.T) {
	s, a, u := executionFixture(t, 1)
	ctx := context.Background()
	ids := submitRequests(t, s, u, 1)
	other := testutil.Member(t, s, a, "other-user@test.invalid", "member")
	s.Credit(ctx, other.Email, 9000, "funding")
	if _, _, err := s.CreateRefunds(ctx, other.ID, "duplicate-other-user", []refundprovider.Account{account("child0@test.invalid")}); !errors.Is(err, store.ErrDuplicate) {
		t.Fatal("same upstream account accepted from another customer", err)
	}
	var wg sync.WaitGroup
	wg.Add(2)
	var j *store.RefundJob
	var claimErr, cancelErr error
	go func() { defer wg.Done(); j, claimErr = s.ClaimRefund(ctx) }()
	go func() { defer wg.Done(); cancelErr = s.Transition(ctx, u.ID, ids[0], "cancelled", "cancelled") }()
	wg.Wait()
	if claimErr != nil {
		t.Fatal(claimErr)
	}
	r, _ := s.Refund(ctx, u.ID, ids[0])
	if r.Status == "cancelled" {
		if j != nil || cancelErr != nil || r.FeeStatus != "released" {
			t.Fatal("cancel/claim race unsafe")
		}
	} else if r.Status == "processing" {
		if j == nil || !errors.Is(cancelErr, store.ErrState) || r.FeeStatus != "frozen" {
			t.Fatal("claim/cancel race unsafe")
		}
	} else {
		t.Fatal("unexpected state", r.Status)
	}
}
