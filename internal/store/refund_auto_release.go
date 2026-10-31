package store

import (
	"context"

	"github.com/jackc/pgx/v5"
)

const autoReleasedMessage = "本次申请自动处理未完成，已结束并释放冻结服务费。"
const autoUnconfirmedMessage = "本次申请未能确认最终结果，已结束并释放冻结服务费。"

// Only explicit resource-side evidence can pause a mother. Child login/plan
// errors, ambiguous cleanup and worker failures cannot establish that cause.
func motherFailure(d RefundDiagnostic) bool {
	switch d.Code {
	case "mother_preflight_auth", "mother_preflight_failed", "mother_network", "mother_shape", "mother_pagination":
		return true
	case "mother_http":
		return d.HTTPStatus == 401 || d.HTTPStatus == 403 || d.HTTPStatus == 429 || d.HTTPStatus >= 500
	case "fallback_unavailable":
		return d.Evidence != nil && d.Evidence.Side == "mother" && d.Evidence.Stage == "fallback_remove" && (d.NetworkCode > 0 || d.HTTPStatus == 401 || d.HTTPStatus == 403 || d.HTTPStatus == 429 || d.HTTPStatus >= 500)
	}
	return false
}

// Called only after the existing recovery planner has no remaining automatic
// attempt. The caller owns the user/execution locks and has fenced the lease.
// End the service without claiming upstream failure, acceptance or removal.
func autoReleaseRefund(ctx context.Context, tx pgx.Tx, j *RefundJob, step string, d RefundDiagnostic) error {
	var terminal string
	var clean, accepted bool
	if err := tx.QueryRow(ctx, "SELECT terminal_code,mother_released_at IS NOT NULL,accepted_at IS NOT NULL FROM refund_execution WHERE refund_id=$1", j.ID).Scan(&terminal, &clean, &accepted); err != nil {
		return err
	}
	message := autoReleasedMessage
	if step == "submitting" || accepted {
		message = autoUnconfirmedMessage
	} else if terminal == "password_verification_failed" {
		message = passwordFailedMessage
	}
	if err := transitionTx(ctx, tx, j.UserID, j.ID, "failed", message); err != nil {
		return err
	}
	// Preserve the last protocol step and accepted_at for investigation. Allow
	// new requests after release, but never invent mother_released_at.
	if _, err := tx.Exec(ctx, `UPDATE refund_execution SET released=true,lease_token='',lease_until=NULL,
 recovery_mode='',next_attempt_at=NULL,completed_at=COALESCE(completed_at,now()),updated_at=now() WHERE refund_id=$1`, j.ID); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, "INSERT INTO refund_events(refund_id,message,internal) VALUES($1,'自动处理已结束，冻结服务费已释放；原始处理结果及清理证据保留。',true)", j.ID); err != nil {
		return err
	}
	if !clean && motherFailure(d) && j.MotherAttemptKey != "" {
		// Do not disable credentials/proxy replaced by the administrator while
		// this old attempt was finishing, or a mother already released to new work.
		tag, err := tx.Exec(ctx, `UPDATE refund_mothers m SET enabled=false,updated_at=now()
 FROM refund_proxies p WHERE m.id=$1 AND p.id=m.proxy_id AND m.enabled
 AND m.id||':'||md5(m.secret)||':'||md5(p.secret)=$2`, j.Mother.ID, j.MotherAttemptKey)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 1 {
			_, err = tx.Exec(ctx, "INSERT INTO refund_events(refund_id,message,internal) VALUES($1,'已确认服务资源请求异常，暂停对应母号接单；其他母号继续调度。',true)", j.ID)
			return err
		}
	}
	return nil
}
