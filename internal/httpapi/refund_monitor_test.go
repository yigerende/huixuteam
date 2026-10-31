package httpapi_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"llm-refund/internal/httpapi"
	"llm-refund/internal/refundprovider"
	"llm-refund/internal/testutil"
)

func TestRefundMonitorPermissionsAndPublicSchema(t *testing.T) {
	s := testutil.Database(t)
	a := testutil.Admin(t, s)
	u := testutil.Member(t, s, a, "monitor@test.invalid", "member")
	o := testutil.Member(t, s, a, "operator@test.invalid", "operator")
	ctx := context.Background()
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(s.Credit(ctx, u.Email, 3000, "fixture"))
	ids, _, err := s.CreateRefunds(ctx, u.ID, "monitor-http", []refundprovider.Account{{Email: "child@test.invalid", Password: "private-password", TOTPSecret: "JBSWY3DPEHPK3PXP"}})
	must(err)
	_, err = s.Pool.Exec(ctx, "UPDATE refunds SET message='private-canary export_and_deactivate' WHERE id=$1", ids[0])
	must(err)
	server := httptest.NewServer(httpapi.New(s, false))
	defer server.Close()
	request := func(path, cookie, key string, want int) []byte {
		t.Helper()
		req, err := http.NewRequest("GET", server.URL+path, nil)
		must(err)
		if cookie != "" {
			req.AddCookie(&http.Cookie{Name: "refund_session", Value: cookie})
		}
		if key != "" {
			req.Header.Set("Authorization", "Bearer "+key)
		}
		resp, err := server.Client().Do(req)
		must(err)
		defer resp.Body.Close()
		raw, err := io.ReadAll(resp.Body)
		must(err)
		if resp.StatusCode != want {
			t.Fatalf("%s: got %d expected %d: %s", path, resp.StatusCode, want, raw)
		}
		return raw
	}
	path := "/api/admin/refund-monitor"
	request(path, "", "", 401)
	mem, err := s.NewSession(ctx, u.ID)
	must(err)
	request(path, mem, "", 403)
	key, err := s.CreateKey(ctx, a.ID, "admin API key")
	must(err)
	request(path, "", key, 401)
	for _, id := range []string{a.ID, o.ID} {
		cookie, err := s.NewSession(ctx, id)
		must(err)
		raw := request(path, cookie, "", 200)
		for _, private := range []string{"private-canary", "export_and_deactivate", "password", "JBSWY3DPEHPK3PXP", "lease_token", "step", "mother", "proxy"} {
			if strings.Contains(string(raw), private) {
				t.Fatal("private data exposed", private)
			}
		}
		var data struct {
			Items []map[string]any `json:"items"`
			Total int              `json:"total"`
			Size  int              `json:"size"`
		}
		must(json.Unmarshal(raw, &data))
		if data.Total != 1 || data.Size != 100 || len(data.Items) != 1 {
			t.Fatal("bad pagination", data)
		}
		allowed := strings.Fields("id email status fee fee_status message created_at updated_at user_id user_name user_email started_at finished_at wait_seconds handle_seconds long_wait long_running recovery_alert recovery_pending next_check_at")
		if len(data.Items[0]) != len(allowed) {
			t.Fatal("unexpected response schema", data.Items[0])
		}
		for _, field := range allowed {
			if _, ok := data.Items[0][field]; !ok {
				t.Fatal("missing field", field)
			}
		}
		request(path+"?status=invalid", cookie, "", 400)
		request(path+"?status=running&size=1", cookie, "", 200)
	}
	timeline := "/api/admin/refunds/" + ids[0] + "/timeline"
	request(timeline, "", "", 401)
	request(timeline, mem, "", 403)
	request(timeline, "", key, 401)
	opsSession, err := s.NewSession(ctx, o.ID)
	must(err)
	request(timeline, opsSession, "", 403)
	_, err = s.Pool.Exec(ctx, "INSERT INTO refund_events(refund_id,message,internal) VALUES($1,'private-canary export_and_deactivate private-password',true)", ids[0])
	must(err)
	_, err = s.Pool.Exec(ctx, `INSERT INTO refund_events(refund_id,message,internal,created_at)
 VALUES($1,'正在使用密码和 2FA 登录受邀团队。',true,now()-interval '60 seconds'),
 ($1,'已确认团队会话，准备提交导出并停用个人空间。',true,now()-interval '10 seconds')`, ids[0])
	must(err)
	_, err = s.Pool.Exec(ctx, `INSERT INTO refund_execution_diagnostics(refund_id,diagnostic)
 VALUES($1,jsonb_build_object('observed_at',now()-interval '30 seconds','http_status',200,'evidence',
 '{"stage":"workspace_observation","side":"child","workspaces":{"source":"personal_accounts_check","phase":"before_team_select","result":"observed","items":[{"structure":"personal","workspace_id":"private-space","fields":{"account.plan_type":"pro","entitlement.subscription_plan":"unknown_pro_50x","account.access_token":"private-token"}}]}}'::jsonb))`, ids[0])
	must(err)
	for _, id := range []string{a.ID} {
		cookie, err := s.NewSession(ctx, id)
		must(err)
		raw := request(timeline, cookie, "", 200)
		if !strings.Contains(string(raw), "unknown_pro_50x") {
			t.Fatal("admin cannot see observed plan")
		}
		for _, private := range []string{"private-canary", "export_and_deactivate", "password", "JBSWY3DPEHPK3PXP", "lease", "mother", "proxy", "internal", "step"} {
			if strings.Contains(string(raw), private) {
				t.Fatal("timeline leaks", private)
			}
		}
		var dto map[string]json.RawMessage
		must(json.Unmarshal(raw, &dto))
		if len(dto) != 3 {
			t.Fatal("timeline root schema", dto)
		}
		var items []map[string]any
		must(json.Unmarshal(dto["items"], &items))
		for _, item := range items {
			fields := strings.Fields("stage attempt status started_at finished_at duration_seconds reason")
			if item["stage"] == "登" {
				fields = append(fields, "personal_plan")
			}
			if len(item) != len(fields) {
				t.Fatal("timeline item schema", item)
			}
			for _, field := range fields {
				if _, ok := item[field]; !ok {
					t.Fatal("missing field", field)
				}
			}
		}
		request("/api/admin/refunds/nonexistent/timeline", cookie, "", 404)
	}
	public := request("/api/refunds/"+ids[0], mem, "", 200)
	if strings.Contains(string(public), "unknown_pro_50x") || strings.Contains(string(public), "personal_plan") {
		t.Fatal("personal plan leaked through customer API")
	}
}
