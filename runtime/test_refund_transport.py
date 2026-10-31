"""Loopback-only transport integration: real login, TOTP and curl_cffi requests.

Two local HTTP proxy fixtures terminate every request. A guarded test adapter
changes HTTPS to HTTP only inside this harness; production URLs/headers remain
asserted. No real accounts, external requests or anti-bot challenges are used.
"""
import base64
import contextlib
import io
import json
import sys
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlsplit, parse_qs
from unittest.mock import patch

import pyotp
from curl_cffi import requests

sys.path.insert(0, str(Path(__file__).parent))
import refund_runner as runner
from manager_oauth import merge_v2, upstream


class ProxyFlow:
    def __init__(self, job, export_status=200, export_body=None, remove_failure=False, password_failure=False):
        self.job = job
        self.export_status = export_status
        self.export_body = {"success": True} if export_body is None else export_body
        self.remove_failure = remove_failure
        self.password_failure = password_failure
        self.password_responses = []
        self.members = False
        self.invited = False
        self.records = []
        self.errors = []
        self.servers = []
        self.threads = []
        self.sent_tokens = []
        self.cookies = []
        self.selected_space = ""
        self.authenticated = False
        self.authorization = 0
        self.used_codes = set()
        self.plan_status = 200
        self.child_sessions = set()
        claims = {"email": job["account"]["email"], "https://api.openai.com/auth": {
            "chatgpt_account_id": job["mother"]["team_id"], "chatgpt_user_id": "child-user"}}
        self.token = "x." + base64.urlsafe_b64encode(json.dumps(claims).encode()).decode().rstrip("=") + ".x"
        claims["https://api.openai.com/auth"]["chatgpt_account_id"] = "fixture-personal"
        self.personal_token = "x." + base64.urlsafe_b64encode(json.dumps(claims).encode()).decode().rstrip("=") + ".x"

    def __enter__(self):
        fixture = self
        class Proxy(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"
            def log_message(self, *_): pass
            def do_CONNECT(self): raise AssertionError("Test must never establish an external tunnel")
            def handle_request(self):
                body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
                parsed = urlsplit(self.path)
                role = self.server.role
                fixture.records.append((role, self.command, parsed.path, dict(self.headers), body))
                try:
                    auth = base64.b64encode((role + ":fixture-secret").encode()).decode()
                    assert self.headers.get("Proxy-Authorization") == "Basic " + auth
                    status, result, headers = fixture.reply(role, self.command, parsed, self.headers, body)
                except Exception as exc:
                    fixture.errors.append(type(exc).__name__ + ": " + str(exc))
                    status, result, headers = 400, {"error": "fixture contract failed"}, {}
                encoded = json.dumps(result).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(encoded)))
                for key, value in headers.items(): self.send_header(key, value)
                self.end_headers()
                self.wfile.write(encoded)
            do_GET = do_POST = do_DELETE = handle_request
        for role in ("child", "mother"):
            server = ThreadingHTTPServer(("127.0.0.1", 0), Proxy)
            server.daemon_threads = True
            server.role = role
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            self.servers.append(server); self.threads.append(thread)
            self.job[role + "_proxy"] = f"http://{role}:fixture-secret@127.0.0.1:{server.server_port}"
        original = requests.Session.request
        allowed = {self.job["child_proxy"], self.job["mother_proxy"]}
        def guarded(session, method, url, **kwargs):
            assert urlsplit(url).hostname in {"chatgpt.com", "auth.openai.com"}
            assert url.startswith("https://")
            assert set(session.proxies.values()) <= allowed and session.proxies
            assert session.proxies.get("http") in allowed and session.proxies.get("https") in allowed
            if session.proxies.get("https") == self.job["child_proxy"]:
                self.child_sessions.add(id(session))
            response = original(session, method, url.replace("https://", "http://", 1), **kwargs)
            response.url = url
            return response
        self.stack = contextlib.ExitStack()
        self.stack.enter_context(patch.object(requests.Session, "request", guarded))
        def sentinel(device, stage, proxy):
            assert proxy == self.job["child_proxy"]
            self.sent_tokens.append(stage)
            return "fixture-sentinel"
        self.stack.enter_context(patch.object(upstream, "generate_openai_sentinel_token", sentinel))
        # The production worker discards protocol stderr too. Keep fixture logs
        # in memory; no account credentials/protocol payloads enter test output.
        self.stack.enter_context(contextlib.redirect_stderr(io.StringIO()))
        return self

    def __exit__(self, *args):
        self.stack.close()
        for server in self.servers: server.shutdown(); server.server_close()
        for thread in self.threads: thread.join(2)

    def reply(self, role, method, url, headers, raw):
        path = url.path
        data = json.loads(raw) if raw and headers.get("Content-Type", "").startswith("application/json") else {}
        team = self.job["mother"]["team_id"]
        email = self.job["account"]["email"]
        if path == "/backend-api/accounts/check/v4-2023-04-27":
            assert role == "child" and method == "GET"
            assert headers["Authorization"] in {"Bearer " + self.token, "Bearer " + self.personal_token}
            return self.plan_status, {"accounts":{"personal":{"account":{"account_id":"fixture-personal","structure":"personal","is_deactivated":False,"plan_type":"pro"},
                    "entitlement":{"subscription_plan":"unknown_pro_50x","rate_limit_multiplier":50,"has_active_subscription":True}},
                    "team":{"account":{"account_id":team,"structure":"workspace","plan_type":"team"}}}}, {}
        if path == "/backend-api/me":
            assert headers["Authorization"] == "Bearer " + self.personal_token
            return 200, {"email": email}, {}
        if path.startswith("/backend-api/accounts/") and path != "/backend-api/accounts/export_and_deactivate":
            assert role == "mother"
            assert headers["Authorization"] == "Bearer " + self.job["mother"]["access_token"]
            assert headers["chatgpt-account-id"] == team
            prefix = "/backend-api/accounts/" + team
            if method == "GET" and path in {prefix + "/users", prefix + "/invites"}:
                assert parse_qs(url.query)["query"] == [email]
                rows = [{"email": email, "id": "child-user", "role": "standard-user"}] if self.members and path.endswith("/users") else []
                if self.invited and path.endswith("/invites"):
                    rows = [{"email_address": email, "id": "invite-child", "role": "standard-user"}]
                return 200, {"items": rows}, {}
            if method == "POST" and path == prefix + "/invites":
                assert data["email_addresses"] == [email] and data["role"] == "standard-user"
                self.invited = True
                return 200, {}, {}
            if method == "DELETE" and path == prefix + "/invites/invite-child":
                return 405, {"detail": "Method Not Allowed"}, {}
            if method == "DELETE" and path == prefix + "/invites":
                assert data == {"email_address": email}
                self.invited = False
                return 200, {}, {}
            if method == "DELETE" and path == prefix + "/users/child-user":
                if self.remove_failure: return 503, {"error": "fixture error"}, {}
                self.members = False
                return 200, {}, {}
            raise AssertionError("Unexpected mother operation")
        assert role == "child"
        assert "Authorization" not in headers or path.endswith("export_and_deactivate")
        if path == "/": return 200, {}, {}
        if path == "/api/auth/csrf": return 200, {"csrfToken": "fixture-csrf"}, {"Set-Cookie": "fixture_login=shared; Path=/"}
        if path == "/api/auth/signin/openai":
            assert method == "POST" and parse_qs(raw.decode())["csrfToken"] == ["fixture-csrf"]
            self.authorization += 1
            if parse_qs(url.query).get("prompt") == ["consent"]:
                assert self.authenticated
                return 200, {"url": "https://auth.openai.com/authorize-team?state=team-state"}, {}
            return 200, {"url": "https://auth.openai.com/authorize"}, {}
        if path == "/authorize-team":
            assert self.authenticated and "login_session=fixture-login" in headers.get("Cookie", "")
            return 302, {}, {"Location": "https://auth.openai.com/consent"}
        if path == "/consent": return 200, {}, {}
        if path == "/authorize": return 200, {}, {"Set-Cookie": "login_session=fixture-login; Path=/"}
        if path == "/api/accounts/authorize/continue":
            assert data["username"]["value"] == email
            return 200, {"page": {"type": "login_password"}, "continue_url": "https://auth.openai.com/log-in/password"}, {}
        if path == "/api/accounts/password/verify":
            assert data["password"] == self.job["account"]["password"]
            if self.password_responses:
                status, body = self.password_responses.pop(0)
                return status, body, {}
            if self.password_failure: return 401, {"error": {"code": "invalid_password"}}, {}
            return 200, {"page": {"type": "mfa_challenge", "payload": {"factors": [{"id": "factor", "factor_type": "totp"}]}}}, {}
        if path == "/api/accounts/mfa/issue_challenge":
            assert data["id"] == "factor" and data["type"] == "totp"
            return 200, {}, {}
        if path == "/api/accounts/mfa/verify":
            assert pyotp.TOTP(self.job["account"]["totp_secret"]).verify(data["code"], valid_window=1)
            self.authenticated = True
            return 200, {"continue_url": "https://auth.openai.com/workspace"}, {}
        if path == "/workspace":
            cookie = base64.urlsafe_b64encode(json.dumps({"workspaces":[{"id":"fixture-personal","type":"personal"},{"id":team,"type":"workspace"}]}).encode()).decode().rstrip("=")
            return 200, {}, {"Set-Cookie":"oai-client-auth-session="+cookie+"; Path=/"}
        if path == "/api/accounts/workspace/select":
            assert data in ({"workspace_id": team}, {"workspace_id":"fixture-personal"})
            self.selected_space = data["workspace_id"]
            if self.selected_space == team:
                self.members = True
                self.invited = False
            code = str(self.authorization) + "-" + self.selected_space
            suffix = "&state=team-state" if self.authorization > 1 else ""
            return 200, {"continue_url": "https://chatgpt.com/api/auth/callback/openai?code="+code+suffix}, {}
        if path == "/api/auth/callback/openai":
            assert "fixture_login=shared" in headers.get("Cookie", "")
            code = parse_qs(url.query)["code"][0]
            assert code not in self.used_codes
            self.used_codes.add(code)
            return 302, {}, {"Location": "https://chatgpt.com/", "Set-Cookie": "fixture_team=selected; Path=/"}
        if path == "/api/auth/session":
            assert "fixture_team=selected" in headers.get("Cookie", "")
            if self.selected_space == "fixture-personal":
                return 200, {"accessToken": self.personal_token, "account": {"id": "fixture-personal"}, "user": {"email": email}}, {}
            assert self.members
            return 200, {"accessToken": self.token, "account": {"id": team}, "user": {"email": email}}, {}
        if path == "/backend-api/accounts/export_and_deactivate":
            assert method == "POST" and not raw
            assert headers["Authorization"] == "Bearer " + self.token
            assert "chatgpt-account-id" not in {key.lower() for key in headers}
            assert headers["User-Agent"] == merge_v2.SAFARI_UA
            assert headers["Origin"] == headers["Referer"] == "https://chatgpt.com"
            assert headers["oai-language"] == "zh-CN"
            assert "fixture_team=selected" in headers.get("Cookie", "")
            return self.export_status, self.export_body, {}
        raise AssertionError("Unexpected child operation")


def job():
    return {"id": "transport-fixture", "step": "assigned", "child_user_id": "",
            "account": {"email": "child@example.test", "password": "fixture-password", "totp_secret": "JBSWY3DPEHPK3PXP"},
            "mother": {"email": "mother@example.test", "team_id": "fixture-team", "access_token": "fixture-mother-at", "seat_type": "default"},
            "settings": {"max_attempts": 2, "retry_seconds": 5}}


class ProxyTransportTests(unittest.TestCase):
    def test_original_password_totp_login_to_cleanup_over_separate_proxies(self):
        value = job(); checkpoints = []
        with ProxyFlow(value) as fixture:
            result = runner.execute(value, lambda method, *args: checkpoints.append(method), sleep=lambda _: None)
        self.assertEqual(fixture.errors, [])
        self.assertEqual(result, {"ok": True})
        self.assertEqual([s for s in checkpoints if s != 'diagnostic'], ["inviting", "invited", "login", "merge_v2_ready", "merge_v2_submitting", "merge_v2_accepted", "removing", "completed"])
        self.assertIn("password_verify", fixture.sent_tokens)
        self.assertFalse(fixture.members)
        self.assertEqual(sum(path.endswith("export_and_deactivate") for _, _, path, _, _ in fixture.records), 1)

    def test_unknown_export_never_retries_or_removes_member(self):
        for status, body in [(200, {}), (200, {"success": "true"}), (500, {"success": True}), (522, {})]:
            with self.subTest(status=status, body=body):
                value = job()
                with ProxyFlow(value, status, body) as fixture:
                    result = runner.execute(value, lambda *args: None, sleep=lambda _: None)
                self.assertFalse(result["ok"])
                self.assertEqual(fixture.errors, [])
                self.assertTrue(fixture.members)
                self.assertEqual(sum(path.endswith("export_and_deactivate") for _, _, path, _, _ in fixture.records), 1)
                self.assertFalse(any(method == "DELETE" for _, method, _, _, _ in fixture.records))

    def test_failed_cleanup_resumes_without_login_or_resubmit(self):
        value = job()
        with ProxyFlow(value, remove_failure=True) as fixture:
            first = runner.execute(value, lambda *args: None, sleep=lambda _: None)
            self.assertFalse(first["ok"])
            value.update(step="accepted", child_user_id="child-user", child_proxy="")
            fixture.remove_failure = False
            start = len(fixture.records)
            result = runner.execute(value, lambda *args: None)
            resumed = fixture.records[start:]
        self.assertEqual(fixture.errors, [])
        self.assertTrue(result["ok"])
        self.assertTrue(all(role == "mother" for role, *_ in resumed))
        self.assertEqual(sum(path.endswith("export_and_deactivate") for _, _, path, _, _ in fixture.records), 1)

    def test_wrong_password_revokes_invitation_without_submission(self):
        value = job()
        checkpoints = []
        with ProxyFlow(value, password_failure=True) as fixture:
            result = runner.execute(value, lambda method, *args: checkpoints.append(method), sleep=lambda _: None)
        self.assertTrue(result["ok"])  # Cleanup succeeded; durable result is failed.
        self.assertEqual(fixture.errors, [])
        self.assertFalse(fixture.invited)
        self.assertFalse(fixture.members)
        self.assertEqual([s for s in checkpoints if s != 'diagnostic'], ['inviting', 'invited', 'login', 'password_failed', 'cleanup_started', 'cleanup_completed'])
        self.assertFalse(any(path.endswith("export_and_deactivate") or '/users/' in path or '/mfa/' in path for _, _, path, _, _ in fixture.records))
        self.assertEqual(sum(path.endswith('/password/verify') for _, _, path, _, _ in fixture.records), 1)
        self.assertEqual([(role, path) for role, method, path, _, _ in fixture.records if method == 'DELETE'], [('mother', '/backend-api/accounts/fixture-team/invites')])

    def test_unclassified_401_relogs_with_fresh_session_then_submits_once(self):
        value = job()
        checkpoints, sessions, waits = [], [], []
        def login_factory(*args):
            flow = runner.CustomerLogin(*args)
            sessions.append(flow.web_session)
            return flow
        with ProxyFlow(value) as fixture:
            fixture.password_responses = [(401, {'error': {'code': 'invalid_session'}})]
            result = runner.execute(value, lambda method, *args: checkpoints.append(method), login_factory=login_factory, sleep=waits.append)
        self.assertTrue(result['ok'])
        self.assertEqual(fixture.errors, [])
        self.assertEqual(waits, [value['settings']['retry_seconds']])
        self.assertEqual(len(sessions), 2)
        self.assertIsNot(sessions[0], sessions[1])
        self.assertEqual(checkpoints.count('login'), 2)
        self.assertNotIn('password_failed', checkpoints)
        self.assertNotIn('cleanup_started', checkpoints)
        self.assertEqual(sum(path.endswith('/password/verify') for _, _, path, _, _ in fixture.records), 2)
        self.assertEqual(sum(path.endswith('export_and_deactivate') for _, _, path, _, _ in fixture.records), 1)
        self.assertEqual(sum(method == 'POST' and path.endswith('/invites') for _, method, path, _, _ in fixture.records), 1)
        self.assertFalse(fixture.invited or fixture.members)


if __name__ == "__main__": unittest.main()
