package store

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
)

// This is a presentation projection, never an execution/log response. Only
// literal allowlisted events can affect it; unknown diagnostics are discarded.
type RefundTimelineItem struct {
	Stage           string     `json:"stage"`
	Attempt         int        `json:"attempt"`
	Status          string     `json:"status"`
	StartedAt       *time.Time `json:"started_at"`
	FinishedAt      *time.Time `json:"finished_at"`
	DurationSeconds *int64     `json:"duration_seconds"`
	Reason          string     `json:"reason"`
}
type RefundTimeline struct {
	Recovery    *RefundRecoveryView  `json:"recovery,omitempty"`
	Items       []RefundTimelineItem `json:"items"`
	Active      bool                 `json:"active"`
	GeneratedAt time.Time            `json:"generated_at"`
}
type timelineEvent struct {
	message  string
	at       time.Time
	internal bool
}

// AdminRefundTimeline uses a consistent, read-only snapshot. No worker, proxy,
// credentials, lease mutations or upstream client are involved in this path.
func (s *Store) AdminRefundTimeline(ctx context.Context, id string) (*RefundTimeline, error) {
	tx, err := s.Pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	var status string
	var created, now time.Time
	var accepted, completed *time.Time
	err = tx.QueryRow(ctx, `SELECT r.status,r.created_at,now(),e.accepted_at,e.completed_at FROM refunds r LEFT JOIN refund_execution e ON e.refund_id=r.id WHERE r.id=$1`, id).Scan(&status, &created, &now, &accepted, &completed)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	rows, err := tx.Query(ctx, `SELECT message,created_at,internal FROM refund_events WHERE refund_id=$1 ORDER BY created_at,id`, id)
	if err != nil {
		return nil, err
	}
	events := []timelineEvent{}
	for rows.Next() {
		var e timelineEvent
		if err = rows.Scan(&e.message, &e.at, &e.internal); err != nil {
			rows.Close()
			return nil, err
		}
		events = append(events, e)
	}
	rows.Close()
	if err = rows.Err(); err != nil {
		return nil, err
	}
	out := projectRefundTimeline(status, created, now, accepted, completed, events)
	out.Recovery, err = readRefundRecovery(ctx, tx, id)
	if err != nil {
		return nil, err
	}
	return out, tx.Commit(ctx)
}

func projectRefundTimeline(status string, created, now time.Time, accepted, completed *time.Time, events []timelineEvent) *RefundTimeline {
	out := &RefundTimeline{Items: []RefundTimelineItem{}, Active: status == "queued" || status == "processing" || status == "verifying", GeneratedAt: now}
	counts := map[string]int{}
	passwordRejected := false
	last := func(stage string) int {
		for i := len(out.Items) - 1; i >= 0; i-- {
			if out.Items[i].Stage == stage {
				return i
			}
		}
		return -1
	}
	finish := func(i int, at time.Time, state, reason string) {
		if i < 0 {
			return
		}
		r := &out.Items[i]
		r.Status = state
		r.FinishedAt = &at
		r.Reason = reason
	}
	add := func(stage, state string, at time.Time, instant bool, reason string) {
		counts[stage]++
		r := RefundTimelineItem{Stage: stage, Attempt: counts[stage], Status: state, Reason: reason}
		if instant {
			r.FinishedAt = &at
		} else {
			r.StartedAt = &at
		}
		out.Items = append(out.Items, r)
	}
	finishStage := func(stage string, at time.Time, state, reason string) {
		i := last(stage)
		if i >= 0 && (out.Items[i].Status == "进行中" || out.Items[i].Status == "排队中") {
			finish(i, at, state, reason)
		} else {
			add(stage, state, at, true, reason)
		}
	}
	closeQueue := func(at time.Time) {
		i := last("候")
		if i >= 0 && out.Items[i].Status == "排队中" {
			finish(i, at, "已完成", "")
		}
	}
	start := func(stage string, at time.Time) {
		if counts["候"] > 1 {
			closeQueue(at)
		} else if i := last("候"); i >= 0 && out.Items[i].Status == "排队中" {
			out.Items[i].Status = "未记录"
			out.Items[i].Reason = "缺少本阶段结束记录"
		}
		i := last(stage)
		if i >= 0 && out.Items[i].Status == "进行中" {
			finish(i, at, "已重试", "本次已结束，继续下一次处理")
		}
		add(stage, "进行中", at, false, "")
	}
	interrupt := func(at time.Time) {
		for i := len(out.Items) - 1; i >= 0; i-- {
			r := out.Items[i]
			if r.Status == "进行中" || r.Status == "排队中" {
				state, reason := "待跟进", "本次处理已暂停，请核实后继续"
				if r.Stage == "停" {
					state, reason = "待核实", "处理结果尚未确认"
				}
				finish(i, at, state, reason)
				return
			}
		}
	}
	add("提", "已完成", created, true, "")
	add("候", "排队中", created, false, "")
	for _, e := range events {
		if !e.internal {
			switch normalizeLoginFailureMessage(e.message) {
			case passwordEndingMessage:
				passwordRejected = true
				finishStage("登", e.at, "未通过", "登录验证未通过（401）")
			case passwordAttentionMessage:
				passwordRejected = true
				interrupt(e.at)
			case passwordFailedMessage:
				passwordRejected = true
				add("结", "已结束", e.at, true, "登录验证未通过（401），费用已释放")
			case "开始处理":
				closeQueue(e.at)
			case "正在继续处理":
				add("候", "排队中", e.at, false, "已安排续办")
			case "申请需要跟进，请联系服务专员。":
				interrupt(e.at)
			case "退款服务已完成":
				if last("移") >= 0 && out.Items[last("移")].Status == "进行中" {
					finishStage("移", e.at, "已完成", "")
				}
				add("结", "已完成", e.at, true, "")
			case "本次服务未完成，冻结服务费已释放。", "申请已取消，冻结积分已释放。", autoReleasedMessage:
				interrupt(e.at)
				add("结", "已结束", e.at, true, "服务未完成，费用已释放")
			case autoUnconfirmedMessage:
				interrupt(e.at)
				add("结", "已结束", e.at, true, "未能确认最终结果，费用已释放")
			}
			continue
		}
		switch e.message {
		case "提交响应需核验实际结果，不重复提交。":
			finishStage("停", e.at, "待核实", "正在核验实际结果，不重复提交")
		case "兜底：个人套餐仍为付费，继续核验，不重新提交。":
			finishStage("验", e.at, "待核实", "个人套餐仍为付费，按配置间隔继续核验")
		case "已安排移出后套餐兜底核验。":
			interrupt(e.at)
		case "兜底：正在移出本次账号。":
			start("移", e.at)
			out.Items[len(out.Items)-1].Reason = "待核实兜底：先移出再核验个人套餐"
		case "兜底：已确认移出本次账号。":
			finishStage("移", e.at, "已完成", "兜底移出已核实")
		case "兜底：正在重新登录个人账号。":
			start("登", e.at)
			out.Items[len(out.Items)-1].Reason = "兜底：重新登录个人账号"
		case "兜底：个人身份已确认，正在核验套餐。":
			finishStage("登", e.at, "已完成", "个人账号身份已确认")
			start("验", e.at)
		case "兜底：个人套餐为 Free，按业务规则完成。":
			finishStage("验", e.at, "已确认", "个人套餐为 Free，按业务规则完成；不代表退款到账")
			finishStage("停", e.at, "已确认", "移出后套餐核验通过（兜底依据）")
		case "兜底：个人套餐仍为付费，等待重新邀请办理。":
			finishStage("验", e.at, "已确认", "个人套餐仍为付费")
			add("复", "已安排", e.at, true, "按配置间隔重新邀请办理，保留原单与费用")
		case "资源暂不可用，已重新排队等待调度。":
			interrupt(e.at)
			add("候", "排队中", e.at, false, "等待资源后继续办理")
		case "已安排自动核实，不重复提交。", "已安排按间隔从原阶段续办。", "已安排资源清理及结果核对。", "自动恢复已暂停，需人工核实。":
			interrupt(e.at)
		case "正在只读核实本次处理结果。":
			start("核", e.at)
		case "本轮只读核实结束，处理结果仍待确认。":
			finishStage("核", e.at, "待核实", "本轮核对结束，尚无明确完成依据")
		case "正在清理未完成申请的本次资源。":
			start("移", e.at)
		case "本次资源已确认清理。":
			finishStage("移", e.at, "已完成", "")
		case "正在发送团队邀请。":
			start("邀", e.at)
		case "团队邀请已确认，准备登录账号。":
			finishStage("邀", e.at, "已完成", "")
		case "正在使用密码和 2FA 登录受邀团队。":
			start("登", e.at)
		case "已确认团队会话，准备提交导出并停用个人空间。":
			finishStage("登", e.at, "已完成", "")
			add("团", "已确认", e.at, true, "")
		case "正在提交导出并停用个人空间。":
			start("停", e.at)
		case "停用请求未受理，保留账号并等待重新处理。":
			finishStage("停", e.at, "未受理", "将按既有规则继续处理")
		case "导出停用已受理，准备移出团队。":
			finishStage("停", e.at, "已受理", "")
		case "管理员已核实停用受理，将继续移出团队。":
			finishStage("停", e.at, "已受理", "人工核实")
		case "管理员已核实请求未受理，将重新登录后提交。":
			finishStage("停", e.at, "未受理", "人工核实")
		case "正在移出本次账号。":
			start("移", e.at)
		}
	}
	// Persisted facts may restore missing completion records, never missing start
	// times. Old rows without timing are explicitly left unrecorded.
	if accepted != nil {
		i := last("停")
		if i < 0 || out.Items[i].Status != "已受理" {
			finishStage("停", *accepted, "已受理", "")
		}
	}
	if completed != nil && last("结") < 0 {
		if status == "succeeded" {
			finishStage("移", *completed, "已完成", "")
			add("结", "已完成", *completed, true, "")
		} else {
			add("结", "已结束", *completed, true, "")
		}
	}
	if !out.Active {
		for i := range out.Items {
			r := &out.Items[i]
			if r.Status == "进行中" || r.Status == "排队中" {
				r.Status = "未记录"
				r.Reason = "缺少本阶段结束记录"
			}
		}
	}
	stages := []string{"提", "候", "邀", "登", "团", "停", "移", "结"}
	maxStage := 0
	for n, stage := range stages {
		if counts[stage] > 0 && stage != "结" {
			maxStage = n
		}
	}
	for n, stage := range stages {
		if counts[stage] == 0 {
			state := "未开始"
			if passwordRejected && oneOf(stage, "团", "停") {
				state = "未执行"
			} else if n < maxStage || status == "succeeded" {
				state = "未记录"
			} else if status == "failed" || status == "banned" || status == "cancelled" {
				state = "未执行"
			}
			missing := RefundTimelineItem{Stage: stage, Attempt: 0, Status: state}
			pos := len(out.Items)
			for i, item := range out.Items {
				for rank, label := range stages {
					if item.Stage == label && rank > n && i < pos {
						pos = i
					}
				}
			}
			out.Items = append(out.Items, RefundTimelineItem{})
			copy(out.Items[pos+1:], out.Items[pos:])
			out.Items[pos] = missing
		}
	}
	for i := range out.Items {
		r := &out.Items[i]
		if r.StartedAt != nil {
			end := r.FinishedAt
			if end == nil && out.Active && (r.Status == "进行中" || r.Status == "排队中") {
				end = &now
			}
			if end != nil && !end.Before(*r.StartedAt) {
				d := int64(end.Sub(*r.StartedAt).Seconds())
				r.DurationSeconds = &d
			}
		}
	}
	return out
}
