package store

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// Admin-only presentation of the observed plan. Never include account IDs,
// request details, credentials, or use these display fields as an eligibility rule.
type RefundPersonalPlanView struct {
	Status string                    `json:"status"`
	Label  string                    `json:"label"`
	Fields []RefundPersonalPlanField `json:"fields,omitempty"`
}

type RefundPersonalPlanField struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

func personalPlanView(d RefundDiagnostic) *RefundPersonalPlanView {
	if d.Evidence == nil || d.Evidence.Stage != "workspace_observation" || d.Evidence.Side != "child" || d.Evidence.Workspaces == nil {
		return nil
	}
	w := d.Evidence.Workspaces.sanitized()
	if w == nil || w.Source != "personal_accounts_check" || w.Phase != "before_team_select" {
		return nil
	}
	v := &RefundPersonalPlanView{Status: w.Result, Label: "个人套餐未采集"}
	switch w.Result {
	case "unavailable", "identity_unavailable", "invalid":
		v.Label = "个人套餐查询未完成"
	case "not_personal":
		v.Label = "未确认个人空间"
	case "observed":
		if d.HTTPStatus != 200 || len(w.Items) != 1 || w.Items[0].Structure != "personal" || w.Items[0].TargetTeam {
			return &RefundPersonalPlanView{Status: "missing", Label: "个人套餐未采集"}
		}
		row := w.Items[0]
		labels := []struct{ key, name string }{
			{"plan_type", "套餐类型"}, {"subscription_plan", "订阅标识"},
			{"subscription_tier", "订阅档位"}, {"plan_tier", "套餐档位"},
			{"tier", "档位"}, {"rate_limit_multiplier", "倍率"},
			{"plan_display_name", "套餐名称"}, {"plan_name", "套餐名称"},
		}
		var summary []string
		seen := map[string]bool{}
		for _, field := range labels {
			for _, prefix := range []string{"account", "entitlement", "workspace"} {
				value := row.Fields[prefix+"."+field.key]
				if value == "" || seen[field.key+"="+value] {
					continue
				}
				seen[field.key+"="+value] = true
				v.Fields = append(v.Fields, RefundPersonalPlanField{Name: field.name, Value: value})
				if field.key == "plan_type" || field.key == "subscription_plan" {
					if !seen["summary="+value] {
						summary = append(summary, value)
						seen["summary="+value] = true
					}
				}
			}
		}
		if len(summary) == 0 && len(v.Fields) > 0 {
			summary = append(summary, v.Fields[0].Value)
		}
		if len(summary) > 0 {
			v.Label = "个人套餐：" + strings.Join(summary, " / ")
		} else {
			v.Status, v.Label = "missing", "个人套餐标识未返回"
		}
		for _, field := range v.Fields {
			if field.Name == "倍率" {
				v.Label += " · 倍率 " + field.Value
				break
			}
		}
	}
	return v
}

func attachTimelinePersonalPlan(out *RefundTimeline, d RefundDiagnostic, latest map[int]time.Time) {
	v := personalPlanView(d)
	if v == nil || d.ObservedAt.IsZero() {
		return
	}
	// Use observation time, not asynchronous insert time. A delayed log from a
	// previous login must never overwrite a subsequent login's plan.
	for i := len(out.Items) - 1; i >= 0; i-- {
		row := &out.Items[i]
		if row.Stage != "登" || row.StartedAt == nil || d.ObservedAt.Before(*row.StartedAt) {
			continue
		}
		if row.FinishedAt != nil && d.ObservedAt.After(*row.FinishedAt) {
			return
		}
		if d.ObservedAt.After(out.GeneratedAt) || d.ObservedAt.Before(latest[i]) {
			return
		}
		row.PersonalPlan = v
		latest[i] = d.ObservedAt
		return
	}
}

func readTimelinePersonalPlans(ctx context.Context, tx pgx.Tx, id string, out *RefundTimeline) error {
	rows, err := tx.Query(ctx, `SELECT diagnostic FROM refund_execution_diagnostics
 WHERE refund_id=$1 AND diagnostic->'evidence'->'workspaces'->>'source'='personal_accounts_check'
 AND diagnostic->'evidence'->'workspaces'->>'phase'='before_team_select' ORDER BY id`, id)
	if err != nil {
		return err
	}
	defer rows.Close()
	latest := map[int]time.Time{}
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return err
		}
		var d RefundDiagnostic
		if json.Unmarshal(raw, &d) == nil {
			attachTimelinePersonalPlan(out, d, latest)
		}
	}
	return rows.Err()
}
