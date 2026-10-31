package store

import (
	"context"
	"github.com/jackc/pgx/v5"
)

type MotherFailoverSettings struct {
	Enabled      bool `json:"enabled"`
	MaxSwitches  int  `json:"max_switches"`
	RetrySeconds int  `json:"retry_seconds"`
}

func (v MotherFailoverSettings) Validate() error {
	if v.MaxSwitches < 0 || v.MaxSwitches > 10 || v.RetrySeconds < 5 || v.RetrySeconds > 3600 {
		return invalid("换母号次数须为 0～10 次，排队重试间隔须为 5～3600 秒")
	}
	return nil
}

// Only the initial, read-only preflight can release a reservation for switching.
// Inviting and all later stages may have upstream effects and cannot use this path.
func scheduleMotherFailover(ctx context.Context, tx pgx.Tx, j *RefundJob, step string, d RefundDiagnostic) (bool, error) {
	if step != "assigned" || !oneOf(d.Code, "mother_preflight_auth", "mother_preflight_failed") {
		return false, nil
	}
	var untouched bool
	if err := tx.QueryRow(ctx, "SELECT child_user_id='' AND accepted_at IS NULL FROM refund_execution WHERE refund_id=$1", j.ID).Scan(&untouched); err != nil {
		return true, err
	}
	if !untouched {
		return false, nil
	}
	if err := recordDiagnostic(ctx, tx, j.ID, d); err != nil {
		return true, err
	}
	if d.Code == "mother_preflight_auth" && j.MotherFingerprint != "" {
		if _, err := tx.Exec(ctx, "UPDATE refund_mothers SET auth_failed_fingerprint=$2 WHERE id=$1 AND md5(secret)=$2", j.Mother.ID, j.MotherFingerprint); err != nil {
			return true, err
		}
	}
	var switches int
	if err := tx.QueryRow(ctx, "SELECT mother_switch_count FROM refund_execution WHERE refund_id=$1", j.ID).Scan(&switches); err != nil {
		return true, err
	}
	cfg := j.Settings.MotherFailover
	if cfg == nil || !cfg.Enabled || switches >= cfg.MaxSwitches {
		return true, autoReleaseRefund(ctx, tx, j, step, d)
	}
	// This restricted transition is safe to cancel: no invite/login/submission was
	// performed. Do not enable generic processing -> queued transitions elsewhere.
	if _, err := tx.Exec(ctx, `UPDATE refunds SET status='queued',message=$2,updated_at=now() WHERE id=$1 AND fee_status='frozen'`, j.ID, publicRefundMessage("queued", "")); err != nil {
		return true, err
	}
	if _, err := tx.Exec(ctx, `UPDATE refund_execution SET released=true,lease_token='',lease_until=NULL,recovery_mode='mother_switch',
 next_attempt_at=now()+($2 * interval '1 second'),mother_switch_count=mother_switch_count+1,
 failed_mothers=array_append(failed_mothers,$3),recovery_started_at=COALESCE(recovery_started_at,now()),updated_at=now()
 WHERE refund_id=$1 AND child_user_id='' AND accepted_at IS NULL`, j.ID, cfg.RetrySeconds, j.MotherAttemptKey); err != nil {
		return true, err
	}
	_, err := tx.Exec(ctx, "INSERT INTO refund_events(refund_id,message,internal) VALUES($1,'资源暂不可用，已重新排队等待调度。',true)", j.ID)
	return true, err
}
