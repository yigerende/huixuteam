"""Observe the personal plan before team selection, using one authenticated session.

This module never submits credentials, invites/removes a member, or interprets a
plan as eligibility/success. The existing fallback is deliberately independent.
"""
import re
import time
import uuid
from urllib.parse import parse_qs, urlencode, urljoin, urlsplit

import diagnostics
from manager_oauth import upstream
from workspace_observation import observe, snapshot


class PersonalWorkspaceUnavailable(Exception):
    pass


def record(result, *, data=None, account_id="", status=0, network_code=0, started=None):
    """One-way diagnostic output; never wait for a database acknowledgement."""
    try:
        evidence = {"source": "personal_accounts_check", "phase": "before_team_select", "result": result}
        if data is not None:
            rows = data.get("accounts") if isinstance(data, dict) else None
            if not isinstance(rows, dict):
                evidence["result"] = "invalid"
            else:
                matching = []
                for row in rows.values():
                    account = row.get("account") if isinstance(row, dict) else None
                    if isinstance(account, dict) and account.get("account_id") == account_id:
                        if account.get("structure") != "personal":
                            evidence["result"] = "not_personal"
                            break
                        matching.append(row)
                else:
                    if matching:
                        safe = snapshot({"workspaces": matching}, phase="before_team_select")
                        evidence.update(items=safe["items"], truncated=safe["truncated"])
                    else:
                        evidence["result"] = "missing"
        diagnostics.record({"stage": "workspace_observation", "side": "child", "event": "response",
                            "workspaces": evidence}, http_status=status, network_code=network_code,
                           duration_ms=int((time.monotonic() - started) * 1000) if started is not None else 0)
    except Exception:
        # Telemetry failures do not change the business result.
        pass


def select_personal_workspace(flow, referer_url):
    data = flow.decode_oauth_session_cookie()
    rows = data.get("workspaces") if isinstance(data, dict) else None
    choices = set()
    for row in rows if isinstance(rows, list) else []:
        if not isinstance(row, dict):
            continue
        account = row.get("account") if isinstance(row.get("account"), dict) else row
        account_id = account.get("account_id") or account.get("id")
        personal = (row.get("is_personal") is True or account.get("structure") == "personal"
                    or account.get("type") == "personal")
        if personal and isinstance(account_id, str) and account_id and account_id != flow.target_team:
            choices.add(account_id)
    if len(choices) != 1:
        # No authorization code was consumed: the original team continuation
        # remains usable. Never guess another Business workspace is personal.
        raise PersonalWorkspaceUnavailable()
    url = "https://auth.openai.com/api/accounts/workspace/select"
    response = flow.request(url, method="POST", json_data={"workspace_id": choices.pop()},
                            headers=flow.headers(url, {"Accept": "application/json", "Origin": "https://auth.openai.com", "Referer": referer_url}), timeout=15)
    if response.status in {301, 302, 303, 307, 308} and response.location():
        return urljoin(url, response.location())
    next_url = flow.extract_continue_url(response.json()) if response.status == 200 else ""
    if not next_url:
        raise RuntimeError("Personal authorization did not return a continuation")
    return flow.normalize_auth_url(next_url)


def session_identity(flow, session):
    token = session.get("accessToken") or session.get("access_token") or ""
    claims = upstream.jwt_payload(token)
    auth = claims.get("https://api.openai.com/auth") or {}
    user = session.get("user") or {}
    emails = [e for e in (user.get("email"), claims.get("email"),
                         (claims.get("https://api.openai.com/profile") or {}).get("email")) if e is not None]
    expected = flow.payload["email"].casefold()
    account_id, user_id = auth.get("chatgpt_account_id"), auth.get("chatgpt_user_id")
    if (not token or not emails or any(not isinstance(e, str) or e.casefold() != expected for e in emails)
            or not isinstance(account_id, str) or not account_id or not isinstance(user_id, str) or not user_id
            or account_id == flow.target_team or (session.get("account") or {}).get("id") not in (None, account_id)):
        raise ValueError("Personal session identity unavailable")
    return token, account_id, user_id


def query_personal_plan(flow):
    started = time.monotonic()
    response = None
    user_id = ""
    try:
        # Read the personal session once. No credential retry, token refresh, or
        # fallback login is performed for observation.
        url = "https://chatgpt.com/api/auth/session"
        response = flow.request(url, headers=flow.headers(url, {"Referer": "https://chatgpt.com/"}), timeout=5)
        if response.status != 200:
            record("unavailable", status=response.status, started=started)
            return ""
        token, account_id, user_id = session_identity(flow, response.json())
        headers = {"Authorization": "Bearer " + token, "Accept": "application/json", "Origin": "https://chatgpt.com",
                   "Referer": "https://chatgpt.com/", "oai-language": "zh-CN", "oai-device-id": flow.device_id}
        response = None
        response = flow.web_session.get("https://chatgpt.com/backend-api/me", headers=headers, timeout=5, allow_redirects=False)
        me = response.json() if response.status_code == 200 else {}
        if not isinstance(me, dict) or not isinstance(me.get("email"), str) or me["email"].casefold() != flow.payload["email"].casefold():
            record("identity_unavailable", status=response.status_code, started=started)
            return user_id
        response = None
        response = flow.web_session.get("https://chatgpt.com/backend-api/accounts/check/v4-2023-04-27?timezone_offset_min=-480",
                                        headers=headers, timeout=5, allow_redirects=False)
        if response.status_code != 200:
            record("unavailable", status=response.status_code, started=started)
        else:
            record("observed", data=response.json(), account_id=account_id, status=200, started=started)
    except Exception as exc:
        status = getattr(response, "status_code", getattr(response, "status", 0)) if response is not None else 0
        detail = diagnostics.exception_evidence(exc, "workspace_observation")
        record("unavailable", status=status if type(status) is int else 0, network_code=detail["network_code"], started=started)
    return user_id


def checked_url(url):
    parsed = urlsplit(url)
    if (parsed.scheme != "https" or parsed.hostname not in {"chatgpt.com", "auth.openai.com"}
            or parsed.username or parsed.password or parsed.port not in (None, 443)):
        raise RuntimeError("Unexpected authorization destination")
    return parsed


def team_authorization(flow):
    """Fresh OAuth state, existing authenticated cookies; never another login.

The personal callback has already consumed its authorization code/state. Obtain
new web state through the ordinary sign-in entry, requesting consent rather
than password reauthentication, then select the exact configured team.
"""
    csrf = flow.get_csrf_token()
    url = "https://chatgpt.com/api/auth/signin/openai?" + urlencode({
        "prompt": "consent", "ext-oai-did": flow.device_id,
        "auth_session_logging_id": str(uuid.uuid4()), "login_hint": flow.payload["email"],
    })
    response = flow.request(url, method="POST", form_data={"callbackUrl": "https://chatgpt.com/", "csrfToken": csrf, "json": "true"},
                            headers=flow.headers(url, {"Accept": "application/json", "Content-Type": "application/x-www-form-urlencoded",
                                                     "Origin": "https://chatgpt.com", "Referer": "https://chatgpt.com/"}), timeout=20)
    current = upstream.first_text(response.json().get("url"), response.location())
    if response.status >= 400 or not current:
        raise RuntimeError("Team authorization could not start")
    current = urljoin(url, current)
    parsed = checked_url(current)
    if parsed.hostname != "auth.openai.com":
        raise RuntimeError("Team authorization entry is not the authentication service")
    flow.remember_oauth_params_from_authorize_url(current)
    expected_state = parse_qs(parsed.query).get("state", [""])[0]
    selected = False
    for _ in range(18):
        parsed = checked_url(current)
        # Do not consume an automatically issued default-space callback. Ask
        # the existing workspace selector for the target within the new state.
        if parsed.hostname == "chatgpt.com" and parsed.path.startswith("/api/auth/callback/"):
            if not selected:
                current = flow.submit_workspace_and_org("https://auth.openai.com/consent")
                selected = True
                continue
            callback_query = parse_qs(parsed.query)
            if callback_query.get("error"):
                raise RuntimeError("Team authorization was not granted")
            if expected_state and callback_query.get("state", [""])[0] != expected_state:
                raise RuntimeError("Team authorization state mismatch")
            flow.follow_callback(current)
            return flow._chatgpt_web_session()
        if parsed.hostname != "auth.openai.com":
            raise RuntimeError("Team authorization ended before workspace selection")
        lowered = parsed.path.lower()
        if any(part in lowered for part in ("/log-in", "/login", "/password", "/mfa", "/email-verification", "/auth-challenge", "/passkey", "/phone-verification")):
            # The normal recovery policy may handle a failed attempt. This
            # transition itself never resubmits a password or a 2FA code.
            raise upstream.LoginFlowError("Existing authentication is insufficient for team authorization",
                                          code="team_session_switch_failed", retryable=False)
        response = flow.request(current, headers=flow.headers(current, {"Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"}), timeout=20)
        if response.status in {301, 302, 303, 307, 308} and response.location():
            current = urljoin(current, response.location())
            continue
        if response.status == 200 and flow.is_workspace_or_consent_url(current):
            current = flow.submit_workspace_and_org(current)
            selected = True
            continue
        if response.status == 200 and "/choose-an-account" in lowered:
            # Only one authenticated identity exists in this job's cookie jar;
            # refuse ambiguous account-picker pages instead of guessing.
            if len(set(re.findall(r"us_[A-Za-z0-9_-]{12,}", response.text or ""))) != 1:
                raise RuntimeError("Authenticated account selection is ambiguous")
            current = flow.choose_account_from_html(response.text, current)
            continue
        raise RuntimeError("Team authorization requires an unsupported interaction")
    raise RuntimeError("Team authorization redirect limit exceeded")


def finish_personal_then_team(flow, continue_url):
    observe(flow, "authenticated_before_callback")
    flow._selecting_personal_plan = True
    original_continuation_usable = False
    personal_user = ""
    try:
        flow.follow_callback(continue_url)
        personal_user = query_personal_plan(flow)
    except PersonalWorkspaceUnavailable:
        record("missing")
        original_continuation_usable = True
    except Exception:
        record("unavailable")
    finally:
        flow._selecting_personal_plan = False
    if original_continuation_usable:
        # No personal workspace and no consumed callback: retain the original
        # team route, with no new credentials or guessed personal subscription.
        flow.follow_callback(continue_url)
        return flow._chatgpt_web_session()
    session = team_authorization(flow)
    _, team_user = flow.validate_team(session)
    if personal_user and personal_user != team_user:
        raise RuntimeError("Personal and team sessions refer to different users")
    return session
