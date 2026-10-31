package store

import (
	"context"
	"encoding/json"
	"time"

	"github.com/jackc/pgx/v5"
)

type RefundRecoverySettings struct {
	VerificationHTTPStatuses []int                 `json:"verification_http_statuses"`
	PlanFallback             *PlanFallbackSettings `json:"plan_fallback,omitempty"`
	Enabled                  bool                  `json:"enabled"`
	MaxRetries               int                   `json:"max_retries"`
	VerifyIntervals          []int                 `json:"verify_intervals"`
	CleanupIntervals         []int                 `json:"cleanup_intervals"`
	AlertSeconds             int                   `json:"alert_seconds"`
	CleanupFailed            bool                  `json:"cleanup_failed"`
}

func (v RefundRecoverySettings) Validate() error {
	if len(v.VerificationHTTPStatuses) > 30 {
		return invalid("结果核验状态码最多设置 30 个")
	}
	seen := map[int]bool{}
	for _, status := range v.VerificationHTTPStatuses {
		if status < 400 || status > 599 || seen[status] {
			return invalid("结果核验状态码须为不重复的 400～599 整数，用英文逗号分隔")
		}
		seen[status] = true
	}
	if v.PlanFallback != nil {
		if err := v.PlanFallback.Validate(); err != nil {
			return err
		}
	}
	if v.MaxRetries < 0 || v.MaxRetries > 10 || v.AlertSeconds < 60 || v.AlertSeconds > 86400 {
		return invalid("自动重试须为 0～10 次，告警阈值须为 60～86400 秒")
	}
	for _, values := range [][]int{v.VerifyIntervals, v.CleanupIntervals} {
		if len(values) < 1 || len(values) > 10 {
			return invalid("核实和清理须各设置 1～10 轮间隔")
		}
		for _, n := range values {
			if n < 5 || n > 86400 {
				return invalid("每轮间隔须为 5～86400 秒")
			}
		}
	}
	return nil
}

func (v *RefundRecoverySettings) RequiresResultVerification(status int) bool {
	if v == nil || v.VerificationHTTPStatuses == nil {
		return status == 404
	}
	for _, configured := range v.VerificationHTTPStatuses {
		if configured == status && status >= 400 && status <= 599 {
			return true
		}
	}
	return false
}

// Internal, typed evidence only. Never persist response bodies, tokens, URLs,
// headers, exception strings or arbitrary upstream error codes.
type RefundDiagnostic struct {
	ObservedAt  time.Time       `json:"observed_at,omitempty"`
	Evidence    *RefundEvidence `json:"evidence,omitempty"`
	Code        string          `json:"code"`
	HTTPStatus  int             `json:"http_status,omitempty"`
	NetworkCode int             `json:"network_code,omitempty"`
	DurationMS  int             `json:"duration_ms,omitempty"`
	Retryable   bool            `json:"retryable,omitempty"`
}

var recoveryReasons = map[string]string{
	"export_verification_required": "提交响应需核验实际结果，不重复提交",
	"fallback_paid_unconfirmed":    "个人套餐仍为付费，继续核验，不重新提交",
	"password_verification_failed": "登录验证未通过（401），本次申请已停止办理",
	"login_auth_failed":            "登录验证未通过（401），未确认密码错误",
	"protocol_trace":               "内部诊断记录",
	"fallback_unavailable":         "兜底核验未完成，将按配置重试",
	"fallback_paid":                "个人套餐仍为付费，已安排再次办理",
	"fallback_free":                "个人套餐已核验为 Free，按配置完成服务",
	"mother_preflight_auth":        "服务资源授权已失效，尚未开始办理",
	"mother_preflight_failed":      "服务资源连接未通过，尚未开始办理",
	"export_uncertain":             "处理结果未明确，正在核实",
	"export_rejected":              "本次请求未受理",
	"export_not_sent":              "连接未建立，请求尚未发送",
	"export_accepted":              "已收到明确受理确认",
	"login_failed":                 "账号登录未完成",
	"login_response_incomplete":    "登录响应不完整，等待重新登录",
	"team_session_expired":         "会话需要更新",
	"extra_verification":           "账号需要补充验证",
	"account_unavailable":          "账号当前不可用",
	"mother_network":               "服务连接未完成",
	"mother_http":                  "服务请求未完成",
	"mother_shape":                 "服务查询结果无法确认",
	"mother_pagination":            "服务查询结果不完整",
	"mother_identity":              "服务身份校验未通过",
	"member_identity":              "账号身份校验未通过",
	"proxy_missing":                "网络配置不可用",
	"protocol_failed":              "办理中断，已保留进度",
	"worker_interrupted":           "办理中断，已安排恢复检查",
	"verification_active":          "原个人空间仍可见，受理结果仍待核实",
	"verification_missing":         "个人空间已不可见，仍需确认处理结果",
	"verification_deactivated":     "原个人空间显示已停用，仍需确认受理依据",
	"verification_changed":         "账户空间信息发生变化，需要人工核实",
	"verification_unavailable":     "本轮未取得完整核实结果",
	"verification_no_baseline":     "缺少提交前记录，需要人工核实",
	"cleanup_pending":              "正在核对资源清理结果",
	"cleanup_invite_pending":       "待加入记录尚未清理，需要人工跟进",
	"cleanup_failed":               "资源清理未完成",
	"cleanup_completed":            "本次资源已确认清理",
}

func (d RefundDiagnostic) Sanitized() RefundDiagnostic {
	if d.Evidence != nil {
		d.Evidence = d.Evidence.Sanitized()
	}
	if _, ok := recoveryReasons[d.Code]; !ok {
		d.Code = "protocol_failed"
		d.Retryable = false
	}
	if d.Code == "extra_verification" {
		d.Retryable = false
	}
	if d.HTTPStatus < 100 || d.HTTPStatus > 599 {
		d.HTTPStatus = 0
	}
	if d.NetworkCode < 0 || d.NetworkCode > 999 {
		d.NetworkCode = 0
	}
	if d.DurationMS < 0 || d.DurationMS > 900000 {
		d.DurationMS = 0
	}
	return d
}

type PersonalSnapshot struct {
	ID    string `json:"id"`
	State string `json:"state"`
}

func (v PersonalSnapshot) Valid() bool {
	return len(v.ID) <= 150 && oneOf(v.State, "active", "deactivated", "missing", "unknown") && (v.State == "missing" || v.State == "unknown" || v.ID != "")
}

func recordDiagnostic(ctx context.Context, tx pgx.Tx, id string, d RefundDiagnostic) error {
	d = d.Sanitized()
	if d.ObservedAt.IsZero() {
		d.ObservedAt = time.Now().UTC()
	}
	if _, err := tx.Exec(ctx, "INSERT INTO refund_execution_diagnostics(refund_id,diagnostic) VALUES($1,$2)", id, d); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, "UPDATE refund_execution SET last_diagnostic=$2 WHERE refund_id=$1", id, d)
	return err
}

// A delayed attempt does not hold a worker lease. Its mother reservation stays
// exclusive until verified cleanup; neither elapsed time nor a 404 releases it.
func scheduleRefundRecovery(ctx context.Context, tx pgx.Tx, j *RefundJob, step string, d RefundDiagnostic) error {
	d = d.Sanitized()
	if err := recordDiagnostic(ctx, tx, j.ID, d); err != nil {
		return err
	}
	var retries, checks, cleanups, fallbackCycles, fallbackAttempts int
	var mode, fallbackPhase, terminalCode string
	if err := tx.QueryRow(ctx, "SELECT retry_count,verify_count,cleanup_count,recovery_mode,fallback_cycles,fallback_attempts,fallback_phase,terminal_code FROM refund_execution WHERE refund_id=$1", j.ID).Scan(&retries, &checks, &cleanups, &mode, &fallbackCycles, &fallbackAttempts, &fallbackPhase, &terminalCode); err != nil {
		return err
	}
	cfg := j.Settings.Recovery
	nextMode, status, message, delay := "", "processing", "处理时间延长，正在继续", 0
	passwordFailed := terminalCode == "password_verification_failed" && oneOf(step, "invited", "login", "ready")
	if passwordFailed {
		// Password rejection always ends the attempt. Recovery switches may
		// disable business retries, but must not leave this cleanup abandoned.
		intervals := []int{15, 30, 60}
		if cfg != nil && len(cfg.CleanupIntervals) > 0 {
			intervals = cfg.CleanupIntervals
		}
		if cleanups < len(intervals) {
			nextMode, delay = "cleanup", intervals[cleanups]
		}
	} else if cfg != nil && cfg.Enabled {
		switch {
		case step == "submitting" && cfg.PlanFallback != nil && cfg.PlanFallback.Enabled:
			f := cfg.PlanFallback
			if fallbackAttempts < f.MaxAttempts && (fallbackPhase != "" || fallbackCycles < f.MaxCycles) {
				nextMode, delay = "fallback", f.RetrySeconds
			}
		case step == "submitting" && fallbackPhase != "":
			// A removed child cannot use the old team-only verifier. Keep its phase.
		case step == "submitting":
			if checks < len(cfg.VerifyIntervals) {
				nextMode = "verify"
				delay = cfg.VerifyIntervals[checks]
			}
		case step == "accepted" || step == "removing" || mode == "cleanup":
			if cleanups < len(cfg.CleanupIntervals) {
				nextMode = "cleanup"
				delay = cfg.CleanupIntervals[cleanups]
			}
		case fallbackPhase == "reinvite" && d.Code != "extra_verification":
			if cfg.PlanFallback != nil && cfg.PlanFallback.Enabled {
				nextMode, delay = "retry", cfg.PlanFallback.RetrySeconds
			}
		case d.Retryable && retries < cfg.MaxRetries:
			nextMode = "retry"
			delay = j.Settings.RetrySeconds
		case cfg.CleanupFailed && oneOf(step, "assigned", "inviting", "invited", "login", "ready") && !oneOf(d.Code, "mother_identity", "member_identity", "proxy_missing", "protocol_failed"):
			// An explicit verification requirement is not retryable, but before
			// submission it can safely end after confirmed scoped cleanup.
			nextMode = "cleanup"
			delay = cfg.CleanupIntervals[0]
		}
	}
	if nextMode == "" {
		return autoReleaseRefund(ctx, tx, j, step, d)
	}
	if nextMode == "verify" {
		status = "verifying"
	}
	if passwordFailed {
		message = passwordEndingMessage
	}
	if err := transitionTx(ctx, tx, j.UserID, j.ID, status, message); err != nil {
		return err
	}
	// A same-status retry still needs a current customer-facing progress message.
	if _, err := tx.Exec(ctx, "UPDATE refunds SET message=$2,updated_at=now() WHERE id=$1", j.ID, message); err != nil {
		return err
	}
	internal := "已安排按间隔从原阶段续办。"
	if nextMode == "verify" {
		internal = "已安排自动核实，不重复提交。"
	}
	if nextMode == "cleanup" {
		internal = "已安排资源清理及结果核对。"
	}
	if nextMode == "fallback" {
		internal = "已安排移出后套餐兜底核验。"
	}
	if _, err := tx.Exec(ctx, "INSERT INTO refund_events(refund_id,message,internal) VALUES($1,$2,true)", j.ID, internal); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, `UPDATE refund_execution SET recovery_mode=$2,recovery_started_at=COALESCE(recovery_started_at,now()),
 next_attempt_at=CASE WHEN $2='' THEN NULL ELSE now()+($3 * interval '1 second') END,
 lease_until=NULL,lease_token='',updated_at=now() WHERE refund_id=$1`, j.ID, nextMode, delay)
	return err
}

func (s *Store) FinishRefundAttempt(ctx context.Context, j *RefundJob, d RefundDiagnostic) error {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, "SELECT id FROM users WHERE id=$1 FOR UPDATE", j.UserID); err != nil {
		return err
	}
	var token, step string
	var released bool
	if err = tx.QueryRow(ctx, "SELECT lease_token,step,released FROM refund_execution WHERE refund_id=$1 FOR UPDATE", j.ID).Scan(&token, &step, &released); err != nil {
		return err
	}
	if released || token != j.LeaseToken {
		return nil
	}
	// Read current settings, including changes made while an attempt was running.
	if err = tx.QueryRow(ctx, "SELECT recovery,retry_seconds,mother_failover FROM refund_settings WHERE id").Scan(&j.Settings.Recovery, &j.Settings.RetrySeconds, &j.Settings.MotherFailover); err != nil {
		return err
	}
	if handled, e := scheduleMotherFailover(ctx, tx, j, step, d); handled || e != nil {
		if e != nil {
			return e
		}
		return tx.Commit(ctx)
	}
	if err = scheduleRefundRecovery(ctx, tx, j, step, d); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

type RefundRecoveryView struct {
	Fallbacks int        `json:"fallbacks"`
	Switches  int        `json:"switches"`
	Pending   bool       `json:"pending"`
	Running   bool       `json:"running"`
	Alert     bool       `json:"alert"`
	Label     string     `json:"label"`
	Reason    string     `json:"reason"`
	StartedAt *time.Time `json:"started_at"`
	NextAt    *time.Time `json:"next_at"`
	Retries   int        `json:"retries"`
	Checks    int        `json:"checks"`
	Cleanups  int        `json:"cleanups"`
}

type recoveryQuerier interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

func (s *Store) AdminRefundRecovery(ctx context.Context, id string) (*RefundRecoveryView, error) {
	return readRefundRecovery(ctx, s.Pool, id)
}
func readRefundRecovery(ctx context.Context, q recoveryQuerier, id string) (*RefundRecoveryView, error) {
	var v RefundRecoveryView
	var mode string
	var raw []byte
	var released, enabled, switchEnabled, fallbackEnabled bool
	var fallbackPhase string
	var status, terminalCode string
	err := q.QueryRow(ctx, `SELECT e.recovery_mode,e.recovery_started_at,e.next_attempt_at,e.retry_count,e.verify_count,e.cleanup_count,e.last_diagnostic,e.released,
 COALESCE(e.lease_until>now(),false),(NOT e.released OR e.recovery_mode='mother_switch') AND COALESCE(e.recovery_started_at<=now()-((s.recovery->>'alert_seconds')::int * interval '1 second'),false),(s.recovery->>'enabled')::boolean,
	 e.mother_switch_count,s.enabled AND (s.mother_failover->>'enabled')::boolean,r.status,e.fallback_cycles,COALESCE((s.recovery->'plan_fallback'->>'enabled')::boolean,false),e.fallback_phase,e.terminal_code
 FROM refund_execution e JOIN refunds r ON r.id=e.refund_id CROSS JOIN refund_settings s WHERE e.refund_id=$1`, id).Scan(&mode, &v.StartedAt, &v.NextAt, &v.Retries, &v.Checks, &v.Cleanups, &raw, &released, &v.Running, &v.Alert, &enabled, &v.Switches, &switchEnabled, &status, &v.Fallbacks, &fallbackEnabled, &fallbackPhase, &terminalCode)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	v.Pending = !released && (v.NextAt != nil || v.Running)
	v.Label = map[string]string{"retry": "等待续办", "verify": "自动核实", "cleanup": "清理核对", "fallback": "移出后套餐核验", "": "等待人工核实"}[mode]
	if !released && terminalCode == "" && (!enabled || ((mode == "fallback" || fallbackPhase == "reinvite") && !fallbackEnabled)) && mode != "" && !v.Running {
		v.Pending = false
		v.NextAt = nil
		v.Label = "自动恢复已暂停"
	}
	if released && !(mode == "mother_switch" && status == "queued") {
		v.Label = "处理已结束"
		v.NextAt = nil
		v.Alert = false
	}
	if mode == "mother_switch" && status == "queued" {
		v.Pending = switchEnabled
		v.Label = "排队等待可用资源"
		if !switchEnabled {
			v.Label = "换号调度已暂停"
			v.NextAt = nil
		}
	}
	var d RefundDiagnostic
	_ = json.Unmarshal(raw, &d)
	if d.Code != "" {
		v.Reason = recoveryReasons[d.Sanitized().Code]
	}
	if terminalCode == "password_verification_failed" {
		v.Reason = recoveryReasons[terminalCode]
	}
	return &v, nil
}
