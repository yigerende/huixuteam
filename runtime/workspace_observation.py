"""Observe authenticated workspace metadata before selecting a workspace."""
import re

import diagnostics


PLAN_KEYS = (
    "plan_type", "subscription_plan", "plan_display_name", "plan_name",
    "subscription_tier", "plan_tier", "tier", "rate_limit_multiplier",
)
LABEL = re.compile(r"[A-Za-z0-9][A-Za-z0-9 _+()-]{0,63}\Z")


def label(value):
    # Preserve previously unknown plan IDs; reject arbitrary text/URLs/tokens.
    if isinstance(value, str) and LABEL.fullmatch(value):
        return value
    if type(value) is int and 0 <= value <= 10000:
        return str(value)
    return ""


def snapshot(data, *, phase, target_team=""):
    source = "auth_workspaces"
    if not isinstance(data, dict):
        return {"source": source, "phase": phase, "result": "invalid"}
    rows = data.get("workspaces")
    if not isinstance(rows, list):
        return {"source": source, "phase": phase, "result": "missing"}
    accounts, seen = [], set()
    truncated = len(rows) > 64
    for item in rows[:64]:
        if not isinstance(item, dict):
            continue
        account = item.get("account", item)
        if not isinstance(account, dict):
            continue
        account_id = account.get("account_id") or account.get("id")
        fields = {}
        for prefix, obj in (("account", item.get("account")), ("entitlement", item.get("entitlement")), ("workspace", item)):
            if not isinstance(obj, dict):
                continue
            for key in PLAN_KEYS:
                value = label(obj.get(key))
                if value:
                    fields[prefix + "." + key] = value
        structure = account.get("structure") or account.get("type")
        row = {"structure": structure if structure in ("personal", "workspace", "team", "business") else "unknown",
               "target_team": bool(account_id and account_id == target_team), "fields": fields}
        if isinstance(account_id, str) and re.fullmatch(r"[A-Za-z0-9_-]{1,100}", account_id):
            row["workspace_id"] = account_id
        for key in ("is_personal", "is_default"):
            if type(item.get(key)) is bool:
                row[key] = item[key]
        role = item.get("role")
        if role in ("owner", "admin", "member", "standard-user", "user"):
            row["role"] = role
        ent = item.get("entitlement")
        if isinstance(ent, dict) and type(ent.get("has_active_subscription")) is bool:
            row["has_active_subscription"] = ent["has_active_subscription"]
        if type(account.get("is_deactivated")) is bool:
            row["is_deactivated"] = account["is_deactivated"]
        # Keep conflicting observations; skip exact duplicate workspace rows.
        fingerprint = (account_id if isinstance(account_id, str) else "", repr(row))
        if fingerprint in seen:
            continue
        seen.add(fingerprint)
        if len(accounts) == 16:
            truncated = True
            break
        accounts.append(row)
    return {"source": source, "phase": phase, "result": "observed" if accounts else "missing",
            "items": accounts, "truncated": truncated}


def observe(flow, phase):
    # Diagnostic storage is the existing one-way asynchronous channel. Even a
    # telemetry-only failure must not turn a successful login into a retry.
    try:
        observation = snapshot(flow.decode_oauth_session_cookie(), phase=phase,
                               target_team=flow.payload.get("merge_v2", {}).get("team_id") or flow.payload.get("excluded_team", ""))
        diagnostics.record({"stage": "workspace_observation", "side": "child", "event": "response",
                            "workspaces": observation})
    except Exception:
        try:
            diagnostics.record({"stage": "workspace_observation", "side": "child", "event": "error",
                                "workspaces": {"source": "auth_workspaces", "phase": phase, "result": "unavailable"}})
        except Exception:
            pass


class WorkspaceObservationMixin:
    def _finish_chatgpt_login(self, continue_url):
        # Password/TOTP have completed. The callback (which may select the
        # invited team) has not been followed yet. No extra HTTP/login is made.
        observe(self, "authenticated_before_callback")
        return super()._finish_chatgpt_login(continue_url)
