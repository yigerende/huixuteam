package store_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"llm-refund/internal/store"
)

func TestExtraVerificationCleansBeforeReleaseWithoutLoginRetry(t *testing.T) {
	for _, cleanup := range []bool{true, false} {
		t.Run(map[bool]string{true: "cleanup", false: "manual"}[cleanup], func(t *testing.T) {
			s, a, u := executionFixture(t, 1)
			enableRecovery(t, s, a)
			ctx := context.Background()
			cfg, _ := s.RefundSettings(ctx)
			cfg.Recovery.CleanupFailed = cleanup
			if err := s.SaveRefundSettings(ctx, a.ID, cfg); err != nil {
				t.Fatal(err)
			}
			submitRequests(t, s, u, 1)
			j, err := s.ClaimRefund(ctx)
			if err != nil || j == nil {
				t.Fatal(j, err)
			}
			checkpoint(t, s, j, "inviting")
			checkpoint(t, s, j, "invited")
			checkpoint(t, s, j, "login")
			// Even a malformed worker flag cannot retry an explicit challenge.
			if err = s.FinishRefundAttempt(ctx, j, store.RefundDiagnostic{Code: "extra_verification", Retryable: true}); err != nil {
				t.Fatal(err)
			}
			r, _ := s.Refund(ctx, u.ID, j.ID)
			if cleanup && r.FeeStatus != "frozen" {
				t.Fatal("released before cleanup", r)
			}
			if !cleanup {
				if r.Status != "failed" || r.FeeStatus != "released" {
					t.Fatal(r)
				}
				if next, err := s.ClaimRefund(ctx); err != nil || next != nil {
					t.Fatal(next, err)
				}
				return
			}
			if next, err := s.ClaimRefund(ctx); err != nil || next != nil {
				t.Fatal("cleanup interval ignored", next, err)
			}
			recoveryDue(t, s, j.ID)
			next, err := s.ClaimRefund(ctx)
			if err != nil || next == nil || next.RecoveryMode != "cleanup" {
				t.Fatal(next, err)
			}
			checkpoint(t, s, next, "cleanup_started")
			checkpoint(t, s, next, "cleanup_completed")
			// Late results must not release twice or restart the original login.
			if err = s.FinishRefundAttempt(ctx, j, store.RefundDiagnostic{Code: "extra_verification"}); err != nil {
				t.Fatal(err)
			}
			r, _ = s.Refund(ctx, u.ID, j.ID)
			e, _ := s.RefundExecution(ctx, j.ID)
			var releases, settlements, retries int
			if err = s.Pool.QueryRow(ctx, "SELECT count(*) FILTER (WHERE kind='release'), count(*) FILTER (WHERE kind='settlement') FROM ledger WHERE refund_id=$1", j.ID).Scan(&releases, &settlements); err != nil {
				t.Fatal(err)
			}
			if err = s.Pool.QueryRow(ctx, "SELECT retry_count FROM refund_execution WHERE refund_id=$1", j.ID).Scan(&retries); err != nil {
				t.Fatal(err)
			}
			if r.Status != "failed" || r.FeeStatus != "released" || !e.Released || releases != 1 || settlements != 0 || retries != 0 {
				t.Fatal(r, releases, settlements, retries)
			}
		})
	}
}

func TestLoginFailureAfterSubmissionStillOnlyVerifies(t *testing.T) {
	s, a, u := executionFixture(t, 1)
	enableRecovery(t, s, a)
	submitRequests(t, s, u, 1)
	ctx := context.Background()
	j, _ := s.ClaimRefund(ctx)
	prepareExport(t, s, j)
	if err := s.FinishRefundAttempt(ctx, j, store.RefundDiagnostic{Code: "login_response_incomplete", Retryable: true}); err != nil {
		t.Fatal(err)
	}
	recoveryDue(t, s, j.ID)
	next, err := s.ClaimRefund(ctx)
	if err != nil || next == nil || next.RecoveryMode != "verify" || next.Step != "submitting" {
		t.Fatal(next, err)
	}
}

func TestLoginStepEvidenceAllowlist(t *testing.T) {
	v := store.RefundEvidence{LoginPage: "email_otp_verification", ContinueKind: "email", VerificationMode: "passwordless_login", Reason: "extra_verification"}.Sanitized()
	if v.LoginPage != "email_otp_verification" || v.Reason != "extra_verification" {
		t.Fatal(v)
	}
	v = store.RefundEvidence{LoginPage: "private-token", ContinueKind: "https://private.invalid", VerificationMode: "private-mode"}.Sanitized()
	raw, _ := json.Marshal(v)
	if strings.Contains(string(raw), "private") {
		t.Fatal("untrusted diagnostic retained")
	}
}
