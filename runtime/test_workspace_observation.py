import json
import unittest
from unittest.mock import Mock, patch

import diagnostics
from manager_oauth.adapter import ProjectProtocolLogin
from manager_oauth.merge_v2 import TeamProtocolLogin
from refund_runner import CustomerLogin
from refund_plan_fallback import PersonalLogin
from workspace_observation import observe, snapshot


class WorkspaceObservationTests(unittest.TestCase):
    def flow(self, cls=CustomerLogin):
        flow = cls.__new__(cls)
        flow.payload = {"merge_v2": {"team_id": "our-team"}, "excluded_team": "our-team"}
        # These contracts cover the metadata-only hook. The personal-first
        # CustomerLogin path has separate end-to-end transport tests.
        flow.payload["_skip_personal_plan_observation"] = True
        flow.decode_oauth_session_cookie = Mock(return_value={"workspaces": [
            {"id": "customer-team", "type": "workspace", "role": "owner", "plan_type": "team", "name": "private-name"},
            {"id": "our-team", "type": "workspace", "is_personal": False},
            {"id": "personal", "type": "personal", "is_personal": True, "subscription_plan": "unknown_pro_50x"},
        ]})
        flow.request = Mock()
        return flow

    def test_snapshot_keeps_unknown_plan_and_workspace_identity_not_credentials(self):
        data = self.flow().decode_oauth_session_cookie()
        data.update(access_token="private-token", email="private@example.com")
        data["workspaces"][0].update(cookie="private-cookie", token="private-token")
        data["workspaces"][2]["entitlement"] = {"has_active_subscription": False, "subscription_tier": "pro_50x"}
        data["workspaces"].append(data["workspaces"][2])
        got = snapshot(data, phase="before_team_select", target_team="our-team")
        self.assertEqual(len(got["items"]), 3)
        self.assertTrue(got["items"][1]["target_team"])
        self.assertFalse(got["items"][0]["target_team"])
        self.assertFalse(got["items"][2]["has_active_subscription"])
        self.assertEqual(got["items"][2]["fields"]["workspace.subscription_plan"], "unknown_pro_50x")
        self.assertNotIn("private", json.dumps(got))

    def test_invalid_missing_and_oversized_metadata_are_not_empty_team_proof(self):
        self.assertEqual(snapshot({}, phase="before_team_select")["result"], "missing")
        self.assertEqual(snapshot(None, phase="before_team_select")["result"], "invalid")
        self.assertEqual(snapshot({"workspaces": []}, phase="before_team_select")["result"], "missing")
        rows = [{"id": str(i), "plan_type": "https://private.example", "subscription_plan": "x" * 200} for i in range(100)]
        got = snapshot({"workspaces": rows}, phase="before_team_select")
        self.assertTrue(got["truncated"])
        self.assertEqual(len(got["items"]), 16)
        self.assertTrue(all(not r["fields"] for r in got["items"]))

    def test_before_callback_hook_has_no_additional_requests_for_both_login_modes(self):
        for cls in (CustomerLogin, PersonalLogin):
            with self.subTest(cls=cls):
                flow, events = self.flow(cls), []
                session = {"access_token": "private-token"}
                def finish(url):
                    self.assertEqual(events[0]["phase"], "authenticated_before_callback")
                    self.assertEqual(url, "original-callback")
                    return session
                with diagnostics.capture(lambda _, p: events.append(p["diagnostic"]["evidence"]["workspaces"])), patch.object(ProjectProtocolLogin, "_finish_chatgpt_login", side_effect=finish) as original:
                    self.assertIs(flow._finish_chatgpt_login("original-callback"), session)
                original.assert_called_once()
                flow.request.assert_not_called()

    def test_before_team_selection_logs_latest_metadata_and_preserves_return(self):
        flow, events = self.flow(), []
        def select(url):
            self.assertEqual(events[0]["phase"], "before_team_select")
            self.assertTrue(events[0]["items"][1]["target_team"])
            return "original-next"
        with diagnostics.capture(lambda _, p: events.append(p["diagnostic"]["evidence"]["workspaces"])), patch.object(TeamProtocolLogin, "submit_workspace_and_org", side_effect=select) as original:
            self.assertEqual(flow.submit_workspace_and_org("original-referer"), "original-next")
        original.assert_called_once_with("original-referer")
        flow.request.assert_not_called()

    def test_personal_fallback_keeps_existing_choice_and_exactly_one_select_request(self):
        flow, events = self.flow(PersonalLogin), []
        flow.headers = lambda url, headers: headers
        flow.extract_continue_url = lambda data: data["continue_url"]
        flow.normalize_auth_url = lambda url: url
        flow.request.return_value = Mock(status=200, json=lambda: {"continue_url": "next"})
        with diagnostics.capture(lambda _, p: events.append(p["diagnostic"]["evidence"]["workspaces"])):
            self.assertEqual(flow.submit_workspace_and_org("referer"), "next")
        self.assertEqual(events[0]["phase"], "before_personal_select")
        flow.request.assert_called_once()
        self.assertEqual(flow.request.call_args.kwargs["json_data"], {"workspace_id": "personal"})

    def test_metadata_or_log_failure_does_not_change_original_success_or_exception(self):
        flow = self.flow()
        flow.decode_oauth_session_cookie.side_effect = ValueError("private-data")
        for bad_sink in (False, True):
            def sink(*args):
                if bad_sink:
                    raise RuntimeError("private-write-error")
            with diagnostics.capture(sink), patch.object(TeamProtocolLogin, "submit_workspace_and_org", return_value="next"):
                self.assertEqual(flow.submit_workspace_and_org("referer"), "next")
        original_error = RuntimeError("original selection error")
        with diagnostics.capture(lambda *_: None), patch.object(TeamProtocolLogin, "submit_workspace_and_org", side_effect=original_error):
            with self.assertRaises(RuntimeError) as caught:
                flow.submit_workspace_and_org("referer")
        self.assertIs(caught.exception, original_error)
        flow.request.assert_not_called()

    def test_observation_has_reserved_bounded_budget_and_resets(self):
        rows, flow = [], self.flow()
        with diagnostics.capture(lambda _, p: rows.append(p)):
            for _ in range(200):
                diagnostics.record({"stage": "login"})
            for _ in range(200):
                observe(flow, "before_team_select")
        self.assertEqual(len(rows), 144)
        self.assertEqual(rows[-1]["diagnostic"]["evidence"]["sequence"], 144)
        self.assertEqual(rows[-1]["diagnostic"]["evidence"]["workspaces"]["result"], "observed")
        with diagnostics.capture(lambda _, p: rows.append(p)):
            observe(flow, "before_team_select")
        self.assertEqual(rows[-1]["diagnostic"]["evidence"]["sequence"], 1)


if __name__ == "__main__":
    unittest.main()
