// Package refundworker runs durable merge2 jobs independently of HTTP requests.
package refundworker

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"

	"llm-refund/internal/store"
)

type Runner interface {
	Run(context.Context, *store.RefundJob, func(store.Checkpoint) error) error
}
type ProtocolRunner struct{ Python, Script string }

type ProtocolError struct{ Diagnostic store.RefundDiagnostic }

func (e *ProtocolError) Error() string { return protocolMessage(e.Diagnostic.Code) }

func DefaultRunner() ProtocolRunner {
	python := os.Getenv("REFUND_PYTHON")
	if python == "" {
		python = "python"
	}
	script := os.Getenv("REFUND_RUNNER")
	if script == "" {
		script = filepath.Join("runtime", "refund_runner.py")
	}
	return ProtocolRunner{Python: python, Script: script}
}
func (p ProtocolRunner) Check(ctx context.Context) error {
	cmd := exec.CommandContext(ctx, p.Python, p.Script, "--check")
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	return cmd.Run()
}
func protocolMessage(code string) string {
	switch code {
	case "extra_verification":
		return "账号要求额外邮箱或身份验证，请联系管理员处理。"
	case "account_unavailable":
		return "上游提示账号不可用，请管理员核实并清理邀请。"
	case "mother_identity", "member_identity":
		return "团队或账号身份校验未通过，已停止操作，请管理员核实。"
	case "mother_http", "mother_network", "mother_shape", "mother_pagination":
		return "团队操作未完成，请管理员检查母号、代理及当前进度后续跑。"
	case "login_failed", "team_session_expired", "chatgpt_rate_limited":
		return "登录或团队会话获取未完成，请管理员检查凭据、代理后续跑。"
	case "export_rejected":
		return "导出停用请求被上游拒绝，账号保留在团队内，等待处理。"
	case "export_uncertain", "export_verification_required":
		return "停用请求结果待核实，已停止自动重发。"
	default:
		return "任务未完成，进度已保留，请管理员核实后续跑。"
	}
}
func (p ProtocolRunner) Run(ctx context.Context, j *store.RefundJob, checkpoint func(store.Checkpoint) error) error {
	observe, closeObservations := diagnosticQueue(j.ID, checkpoint)
	defer closeObservations()
	failure := func(reason string, code int) error {
		d := store.RefundDiagnostic{Code: "protocol_failed", Evidence: &store.RefundEvidence{Stage: "runtime", Side: "worker", Event: "failure", Reason: reason, ExitCode: code}}
		observe(d)
		return &ProtocolError{Diagnostic: d.Sanitized()}
	}
	cmd := exec.CommandContext(ctx, p.Python, "-u", p.Script)
	cmd.Env = append(os.Environ(), "PYTHONUTF8=1", "PYTHONIOENCODING=utf-8")
	// Raw upstream output can contain cookies or credentials. Only allowlisted
	// checkpoints and result codes cross the process boundary into application data.
	cmd.Stderr = io.Discard
	input, err := cmd.StdinPipe()
	if err != nil {
		return failure("process_io", 0)
	}
	output, err := cmd.StdoutPipe()
	if err != nil {
		return failure("process_io", 0)
	}
	if err = cmd.Start(); err != nil {
		return failure("process_start", 0)
	}
	defer input.Close()
	enc := json.NewEncoder(input)
	if err = enc.Encode(j); err != nil {
		cmd.Process.Kill()
		cmd.Wait()
		return failure("process_io", 0)
	}
	scan := bufio.NewScanner(output)
	scan.Buffer(make([]byte, 4096), 1<<20)
	var gotResult, success bool
	diagnostic := store.RefundDiagnostic{Code: "protocol_failed"}
	for scan.Scan() {
		var m struct {
			Type string `json:"type"`
			store.Checkpoint
			OK          bool                  `json:"ok"`
			Code        string                `json:"code"`
			HTTPStatus  int                   `json:"http_status"`
			NetworkCode int                   `json:"network_code"`
			DurationMS  int                   `json:"duration_ms"`
			Retryable   bool                  `json:"retryable"`
			Evidence    *store.RefundEvidence `json:"evidence"`
		}
		if json.Unmarshal(scan.Bytes(), &m) != nil {
			observe(store.RefundDiagnostic{Code: "protocol_failed", Evidence: &store.RefundEvidence{Stage: "runtime", Side: "worker", Event: "failure", Reason: "malformed_protocol"}})
			cmd.Process.Kill()
			break
		}
		switch m.Type {
		case "diagnostic":
			observe(store.RefundDiagnostic{Code: "protocol_trace", HTTPStatus: m.HTTPStatus, NetworkCode: m.NetworkCode, DurationMS: m.DurationMS, Evidence: m.Evidence})
		case "checkpoint":
			e := checkpoint(m.Checkpoint)
			if e != nil {
				observe(store.RefundDiagnostic{Code: "protocol_failed", Evidence: &store.RefundEvidence{Stage: "checkpoint", Side: "worker", Event: "failure", Reason: "checkpoint_failed"}})
			}
			if e = enc.Encode(map[string]bool{"ok": e == nil}); e != nil {
				cmd.Process.Kill()
			}
		case "result":
			gotResult = true
			success = m.OK
			diagnostic = (store.RefundDiagnostic{Code: m.Code, HTTPStatus: m.HTTPStatus, NetworkCode: m.NetworkCode, DurationMS: m.DurationMS, Retryable: m.Retryable, Evidence: m.Evidence}).Sanitized()
			if !success {
				observe(diagnostic)
			}
		default:
			cmd.Process.Kill()
		}
	}
	waitErr := cmd.Wait()
	if ctx.Err() != nil {
		reason := "cancelled"
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			reason = "deadline"
		}
		return failure(reason, 0)
	}
	if waitErr != nil {
		code := 0
		var exit *exec.ExitError
		if errors.As(waitErr, &exit) {
			code = exit.ExitCode()
		}
		return failure("process_exit", code)
	}
	if scan.Err() != nil {
		return failure("process_io", 0)
	}
	if !gotResult {
		return failure("missing_result", 0)
	}
	if !success {
		return &ProtocolError{Diagnostic: diagnostic}
	}
	return nil
}

type Worker struct {
	DB     *store.Store
	Runner Runner
}

func (w *Worker) Run(ctx context.Context) {
	defer w.DB.SetRefundRuntimeAvailable(false)
	for {
		if ctx.Err() != nil {
			return
		}
		err := error(nil)
		if checked, ok := w.Runner.(interface{ Check(context.Context) error }); ok {
			checkCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			err = checked.Check(checkCtx)
			cancel()
		}
		if err == nil {
			w.DB.SetRefundRuntimeAvailable(true)
			break
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(10 * time.Second):
		}
	}
	var wg sync.WaitGroup
	defer func() { wg.Wait(); diagnosticWriters.Wait() }()
	timer := time.NewTicker(2 * time.Second)
	defer timer.Stop()
	for {
		if ctx.Err() != nil {
			return
		}
		job, err := w.DB.ClaimRefund(ctx)
		if err != nil && ctx.Err() == nil {
			slog.Error("refund queue claim failed")
		}
		if job != nil {
			wg.Add(1)
			go func() { defer wg.Done(); w.Execute(ctx, job) }()
			continue
		}
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
	}
}
func (w *Worker) Execute(parent context.Context, j *store.RefundJob) {
	ctx, cancel := context.WithTimeout(parent, 15*time.Minute)
	defer cancel()
	done := make(chan struct{})
	defer close(done)
	go func() {
		t := time.NewTicker(15 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-ctx.Done():
				return
			case <-t.C:
				if w.DB.RenewRefundLease(ctx, j) != nil {
					cancel()
					return
				}
				if w.DB.RefundProgress(ctx, j) != nil {
					cancel()
					return
				}
			}
		}
	}()
	err := w.Runner.Run(ctx, j, func(c store.Checkpoint) error {
		if c.Method == "diagnostic_batch" {
			writeCtx, stop := context.WithTimeout(context.Background(), 2*time.Second)
			defer stop()
			return w.DB.AppendRefundDiagnostics(writeCtx, j, c.Diagnostics)
		}
		// Persist a late acceptance even on shutdown; it must not disappear with the
		// cancelled HTTP/process context. All other steps respect cancellation.
		callCtx := ctx
		var stop context.CancelFunc
		if c.Method == "merge_v2_accepted" {
			callCtx, stop = context.WithTimeout(context.Background(), 5*time.Second)
			defer stop()
		}
		err := w.DB.RefundCheckpoint(callCtx, j, c)
		// Retry only persistence of an observed acceptance, never its upstream POST.
		for attempt := 0; c.Method == "merge_v2_accepted" && err != nil && !errors.Is(err, store.ErrState) && attempt < 2; attempt++ {
			select {
			case <-callCtx.Done():
				return err
			case <-time.After(200 * time.Millisecond):
			}
			err = w.DB.RefundCheckpoint(callCtx, j, c)
		}
		return err
	})
	finishCtx, finishCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer finishCancel()
	state, readErr := w.DB.RefundExecution(finishCtx, j.ID)
	if readErr == nil && state != nil && state.Released {
		return
	}
	// Always use the persisted planner; a disabled recovery switch means no
	// further attempts, not an indefinitely frozen service request.
	diagnostic := store.RefundDiagnostic{Code: "protocol_failed"}
	var pe *ProtocolError
	if errors.As(err, &pe) {
		diagnostic = pe.Diagnostic
	}
	finishErr := w.DB.FinishRefundAttempt(finishCtx, j, diagnostic)
	if finishErr != nil {
		slog.Error("refund progress persistence failed", "refund_id", j.ID)
	}
}
