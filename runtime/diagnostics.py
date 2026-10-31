"""Private, bounded evidence. Never send raw messages, URLs, headers or bodies."""
import contextlib
import re
import time
import traceback
from pathlib import Path
from urllib.parse import urlsplit

STAGES = {
    "api_auth_csrf": "csrf", "api_auth_signin_openai": "signin",
    "oauth_authorize": "authorize", "api_accounts_authorize_continue": "identifier",
    "api_accounts_password_verify": "password", "api_accounts_mfa_challenge": "mfa_challenge",
    "api_accounts_mfa_verify": "mfa_verify", "api_accounts_workspace_select": "workspace",
    "api_auth_callback_openai": "callback", "api_auth_session": "session",
    "sentinel": "sentinel", "totp": "mfa_verify", "waiting_code": "email_verification",
}
REASONS = set("csrf_or_risk_blocked chatgpt_signin_failed oauth_session_missing chatgpt_rate_limited totp_secret_missing totp_secret_invalid totp_factor_missing totp_dep_missing phone_verification_skipped passkey_or_challenge account_deactivated account_deleted account_banned account_disabled account_suspended incorrect_password invalid_password login_auth_failed invalid_grant invalid_workspace_selected invalid_session invalid_auth_step token_expired rate_limit_exceeded invalid_totp_code invalid_otp".split())
EXCEPTIONS = set("RuntimeError ValueError KeyError TypeError AttributeError ImportError ModuleNotFoundError TimeoutError OSError ConnectionError LoginFlowError DeadAccountError NeedsVerification UpstreamFailure VerificationUnavailable RequestsError ProxyError SSLError DNSError ConnectTimeout ReadTimeout ConnectionResetError JSONDecodeError".split())
REASONS.update({"extra_verification", "login_response_incomplete", "login_step_unsupported"})
FILES = {"refund_runner.py", "refund_recovery.py", "refund_plan_fallback.py", "diagnostics.py", "adapter.py", "upstream.py", "merge_v2.py", "bridge.py", "tls.py", "sentinel_vm.py"}
_sink = None
_count = 0
_workspace_count = 0
_sequence = 0


def integer(value, maximum=900000):
    return int(value) if isinstance(value, int) and not isinstance(value, bool) and 0 <= value <= maximum else 0


def stage(value):
    value = str(value or "")
    if "://" in value:
        value = urlsplit(value).path.strip("/").replace("/", "_")
    if value.startswith("api_auth_callback_"):
        return "callback"
    if value.startswith("oauth_") or value in {"authorize", "u_login", "log_in"}:
        return "authorize"
    return STAGES.get(value, "login")


def exception_evidence(exc, operation, side="child", flow=None):
    status = getattr(exc, "status", 0) or getattr(flow, "last_status", 0)
    code = getattr(exc, "code", "")
    evidence = {"stage": operation, "side": side, "event": "failure",
                "reason": code if isinstance(code, str) and code in REASONS else "unknown_error",
                "exception": type(exc).__name__ if type(exc).__name__ in EXCEPTIONS else "other"}
    login_evidence = getattr(flow, "_login_step_evidence", None)
    if isinstance(login_evidence, dict):
        evidence.update({k: login_evidence[k] for k in ("login_page", "continue_kind", "verification_mode") if k in login_evidence})
    network = integer(getattr(exc, "network_code", 0), 999)
    current, seen = exc, set()
    while current is not None and id(current) not in seen and len(seen) < 8:
        seen.add(id(current))
        network = network or integer(getattr(current, "code", 0), 999)
        current = current.__cause__ or current.__context__
    if network:
        evidence["reason"] = "network_error"
    proxy = re.search(r"CONNECT tunnel failed, response (\d{3})", str(exc), re.I)
    if proxy:
        evidence["proxy_http_status"] = integer(int(proxy.group(1)), 599)
    # Local source location identifies unexpected failures without exception text.
    for frame in traceback.extract_tb(exc.__traceback__):
        name = Path(frame.filename).name
        if name in FILES:
            evidence.update(source=name, line=integer(frame.lineno, 100000))
    return {"http_status": integer(status, 599), "network_code": network, "evidence": evidence}


def record(evidence, **numbers):
    global _count, _workspace_count, _sequence
    if _sink is None:
        return
    if evidence.get("stage") == "workspace_observation":
        # Reserve a separate small budget so verbose OAuth requests cannot use
        # up the requested before-selection observations before they happen.
        if _workspace_count >= 16:
            return
        _workspace_count += 1
    else:
        if _count >= 128:
            return
        _count += 1
        if _count == 128:
            evidence = {"stage": "runtime", "side": "worker", "event": "truncated", "reason": "evidence_limit"}
            numbers = {}
    _sequence += 1
    _sink("diagnostic", {"diagnostic": {"code": "protocol_trace", **numbers, "evidence": {**evidence, "sequence": _sequence}}})


def failure(exc, operation, side="child", flow=None):
    evidence = exception_evidence(exc, operation, side, flow)
    record(evidence["evidence"], http_status=evidence["http_status"], network_code=evidence["network_code"])
    return evidence


def login_step(data, operation):
    # Fixed categories only. No URLs, response bodies, factor IDs or OTPs.
    from manager_oauth.upstream import ChatGPTProtocolLogin
    page = ChatGPTProtocolLogin.extract_page_type(data).lower()
    mode = ChatGPTProtocolLogin.extract_email_verification_mode(data).lower()
    url = ChatGPTProtocolLogin.extract_continue_url(data)
    known_pages = {"login_password", "email_otp_verification", "mfa_challenge", "consent", "auth_challenge", "passkey", "phone_verification"}
    kind = "none"
    if url:
        kind = "other"
        for marker, category in [("/email-verification", "email"), ("/log-in/password", "password"), ("/mfa-challenge", "mfa"), ("/auth/callback", "callback"), ("/consent", "consent")]:
            if marker in urlsplit(url).path.lower():
                kind = category
                break
    evidence = {"stage": operation, "side": "child", "event": "response",
                "login_page": page if page in known_pages else ("other" if page else "missing"),
                "continue_kind": kind,
                "verification_mode": mode if mode in {"passwordless_login", "login", "signup"} else ("other" if mode else "missing")}
    record(evidence)
    return evidence


def request_event(operation, event, *, side="child", response=None, exc=None, started=None, attempt=0):
    numbers = {}
    evidence = {"stage": operation, "side": side, "event": event, "transport_attempt": attempt}
    if started is not None:
        numbers["duration_ms"] = min(900000, int((time.monotonic()-started)*1000))
    if exc is not None:
        detail = exception_evidence(exc, operation, side)
        numbers.update(http_status=detail["http_status"], network_code=detail["network_code"])
        evidence.update(detail["evidence"], event=event)
    if response is not None:
        numbers["http_status"] = integer(getattr(response, "status_code", getattr(response, "status", 0)), 599)
        text = str(getattr(response, "text", ""))[:32768].lower()
        try:
            data = response.json()
            evidence["body_kind"] = "html" if "<html" in text[:1000] else ("json" if isinstance(data, (dict, list)) else "text")
        except (ValueError, TypeError):
            data = {}
            evidence["body_kind"] = "html" if "<html" in str(getattr(response, "text", ""))[:1000].lower() else "text"
        error = data.get("error") if isinstance(data, dict) else None
        code = error.get("code") if isinstance(error, dict) else ""
        evidence["upstream_code"] = code if isinstance(code, str) and code in REASONS else ("other" if code else "")
        evidence["challenge"] = any(s in text for s in ("cf-chl-", "challenge-platform", "just a moment"))
        headers = getattr(response, "headers", {})
        retry = str(headers.get("retry-after", ""))
        evidence["retry_after_seconds"] = integer(int(retry), 86400) if retry.isdigit() and len(retry) < 7 else 0
    record(evidence, **numbers)


@contextlib.contextmanager
def capture(checkpoint):
    global _sink, _count, _workspace_count, _sequence
    previous = _sink, _count, _workspace_count, _sequence
    _sink, _count, _workspace_count, _sequence = checkpoint, 0, 0, 0
    try:
        yield
    finally:
        _sink, _count, _workspace_count, _sequence = previous
