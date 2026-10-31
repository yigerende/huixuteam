package store

import (
	"net/mail"
	"strconv"
	"strings"
)

const passwordFailedMessage = "登录验证未通过（401）。冻结服务费已释放。"
const passwordEndingMessage = "登录验证未通过（401）。申请正在结束处理中。"
const passwordAttentionMessage = "登录验证未通过（401）。申请结束处理需要跟进，请联系服务专员。"

// Normalize old descriptions on read without rewriting history or reopening jobs.
func normalizeLoginFailureMessage(message string) string {
	switch message {
	case "密码验证失败（401），请核对 ChatGPT 登录密码。冻结服务费已释放。":
		return passwordFailedMessage
	case "密码验证失败（401），请核对 ChatGPT 登录密码。申请正在结束处理中。":
		return passwordEndingMessage
	case "密码验证失败（401），请核对 ChatGPT 登录密码。申请结束处理需要跟进，请联系服务专员。":
		return passwordAttentionMessage
	}
	return message
}

func sanitizeRefund(r *Refund) {
	r.Message = publicRefundMessage(r.Status, r.Message)
	r.ErrorCode, r.HTTPStatus = "", 0
	if oneOf(r.Message, passwordFailedMessage, passwordEndingMessage, passwordAttentionMessage) {
		r.ErrorCode, r.HTTPStatus = "login_verification_failed", 401
	}
}

// Public messages are an allowlist, never a pass-through of worker/upstream logs.
// This also protects historical rows written before the private event split.
func publicProgress(message string) bool {
	switch message {
	case passwordFailedMessage, passwordEndingMessage, passwordAttentionMessage, autoReleasedMessage, autoUnconfirmedMessage:
		return true
	case "申请已受理", "开始处理", "处理时间延长，正在继续", "退款服务已完成", "正在继续处理", "申请需要跟进，请联系服务专员。", "本次服务未完成，冻结服务费已释放。", "申请已取消，冻结积分已释放。":
		return true
	default:
		return false
	}
}
func publicRefundMessage(status, message string) string {
	message = normalizeLoginFailureMessage(message)
	if status == "queued" {
		return "申请正在排队，系统将自动安排处理，无需重复提交；开始处理前可撤回。"
	}
	if publicProgress(message) {
		return message
	}
	switch status {
	case "processing", "verifying":
		return "开始处理"
	case "succeeded":
		return "退款服务已完成"
	case "cancelled":
		return "申请已取消，冻结积分已释放。"
	case "failed", "banned":
		return "本次服务未完成，冻结服务费已释放。"
	default:
		return "申请需要跟进，请联系服务专员。"
	}
}

func publicNotification(message string) string {
	message = normalizeLoginFailureMessage(message)
	if message == "兑换成功，积分已存入可用余额。" || publicProgress(message) {
		return message
	}
	const prefix, suffix = "已受理 ", " 笔退款申请，等待处理。"
	if strings.HasPrefix(message, prefix) && strings.HasSuffix(message, suffix) {
		count := strings.TrimSuffix(strings.TrimPrefix(message, prefix), suffix)
		if n, err := strconv.Atoi(count); err == nil && n > 0 && n <= 100 {
			return prefix + strconv.Itoa(n) + suffix
		}
	}
	if email, progress, ok := strings.Cut(message, "："); ok {
		progress = normalizeLoginFailureMessage(progress)
		if addr, err := mail.ParseAddress(email); err == nil && addr.Address == email && publicProgress(progress) {
			return email + "：" + progress
		}
	}
	return "申请状态已更新，请查看申请详情。"
}
