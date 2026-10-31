package store_test

import (
	"context"
	"testing"

	"llm-refund/internal/store"
)

func TestChildGlobalProxySeparateAndPinnedAcrossResume(t *testing.T) {
	s, a, u := executionFixture(t, 2)
	ctx := context.Background()
	child, err := s.SaveRefundProxy(ctx, a.ID, store.RefundProxy{Name: "child", URL: "child.invalid:8081:child-user:child-pass", Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	next, err := s.SaveRefundProxy(ctx, a.ID, store.RefundProxy{Name: "next child", URL: "http://next.invalid:8082", Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	cfg, _ := s.RefundSettings(ctx)
	cfg.ChildProxyID = child
	if err = s.SaveRefundSettings(ctx, a.ID, cfg); err != nil {
		t.Fatal(err)
	}
	submitRequests(t, s, u, 2)
	j, err := s.ClaimRefund(ctx)
	if err != nil || j == nil {
		t.Fatal("no job", err)
	}
	if j.ChildProxy != "http://child-user:child-pass@child.invalid:8081" || j.MotherProxy != "http://user:private-proxy@proxy.invalid:8080" {
		t.Fatal("child and mother proxy selection incorrect")
	}
	cfg.ChildProxyID = next
	if err = s.SaveRefundSettings(ctx, a.ID, cfg); err != nil {
		t.Fatal(err)
	}
	if _, err = s.SaveRefundProxy(ctx, a.ID, store.RefundProxy{ID: child, Name: "child", URL: "http://wrong.invalid:8080", Enabled: true}); err == nil {
		t.Fatal("running child proxy address changed")
	}
	second, err := s.ClaimRefund(ctx)
	if err != nil || second == nil || second.ChildProxy != "http://next.invalid:8082" || second.MotherProxy != j.MotherProxy {
		t.Fatal("new task did not use new global choice", err)
	}
	if err = legacyAttention(t, s, j); err != nil {
		t.Fatal(err)
	}
	if err = s.ResumeRefund(ctx, a.ID, j.ID, "resume", "reviewed fixture failure"); err != nil {
		t.Fatal(err)
	}
	resumed, err := s.ClaimRefund(ctx)
	if err != nil || resumed == nil || resumed.ID != j.ID || resumed.ChildProxy != j.ChildProxy {
		t.Fatal("resume moved child to a new exit", err)
	}
}

func TestMissingChildProxyQueuesButAcceptedCleanupCanFinish(t *testing.T) {
	s, a, u := executionFixture(t, 1)
	ctx := context.Background()
	cfg, _ := s.RefundSettings(ctx)
	saved := cfg.ChildProxyID
	cfg.ChildProxyID = ""
	if err := s.SaveRefundSettings(ctx, a.ID, cfg); err == nil {
		t.Fatal("enabled queue without child global proxy")
	}
	submitRequests(t, s, u, 1)
	if _, err := s.Pool.Exec(ctx, "UPDATE refund_settings SET child_proxy_id=NULL"); err != nil {
		t.Fatal(err)
	}
	if j, err := s.ClaimRefund(ctx); err != nil || j != nil {
		t.Fatal("claimed without child proxy", err)
	}
	cfg.ChildProxyID = saved
	if err := s.SaveRefundSettings(ctx, a.ID, cfg); err != nil {
		t.Fatal(err)
	}
	j, err := s.ClaimRefund(ctx)
	if err != nil || j == nil {
		t.Fatal(err)
	}
	prepareExport(t, s, j)
	checkpoint(t, s, j, "merge_v2_accepted")
	if err = legacyAttention(t, s, j); err != nil {
		t.Fatal(err)
	}
	if err = s.ResumeRefund(ctx, a.ID, j.ID, "resume", "retry remaining cleanup only"); err != nil {
		t.Fatal(err)
	}
	s.Pool.Exec(ctx, "UPDATE refund_settings SET child_proxy_id=NULL")
	resumed, err := s.ClaimRefund(ctx)
	if err != nil || resumed == nil || resumed.Step != "accepted" || resumed.ChildProxy != "" || resumed.MotherProxy != j.MotherProxy {
		t.Fatal("accepted cleanup blocked by absent child proxy", err)
	}
}
