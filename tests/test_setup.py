import json
import copy
import os
from pathlib import Path
import tempfile
import types
import unittest
from unittest.mock import patch

import peer
from setup_helper import (check_codex, choose_profile, dws_login_command, ensure_codex_auth,
                          resolve_group_identities, run_supervisor)
from windows_secret import protect, unprotect


class CompatibilityTests(unittest.TestCase):
    def test_launcher_restarts_updated_supervisor_and_preserves_normal_exit(self):
        with patch('setup_helper.subprocess.run', side_effect=[types.SimpleNamespace(returncode=76), types.SimpleNamespace(returncode=130)]) as launch:
            self.assertEqual(run_supervisor(['python', 'supervisor.py']), 130)
            self.assertEqual(launch.call_count, 2)

    def test_login_avoids_interactive_scope_picker_when_supported(self):
        with patch('setup_helper.subprocess.run', return_value=types.SimpleNamespace(stdout='--recommend', returncode=0)):
            self.assertEqual(dws_login_command('existing-dws.exe'), ['existing-dws.exe','auth','login','--recommend'])

    def test_login_preserves_older_dws_without_new_flags(self):
        with patch('setup_helper.subprocess.run', return_value=types.SimpleNamespace(stdout='Usage: auth login', returncode=0)):
            self.assertEqual(dws_login_command('existing-dws.exe'), ['existing-dws.exe','auth','login'])

    def test_configured_api_does_not_invoke_openai_login(self):
        with patch('setup_helper.codex_requires_openai_auth', return_value=False), \
                patch('setup_helper.subprocess.run') as run:
            self.assertTrue(ensure_codex_auth('codex.exe'))
            run.assert_not_called()

    def test_default_or_older_codex_keeps_login_flow(self):
        for requirement in (True, None):
            with self.subTest(requirement=requirement), \
                    patch('setup_helper.codex_requires_openai_auth', return_value=requirement), \
                    patch('setup_helper.subprocess.run', side_effect=[types.SimpleNamespace(returncode=1),
                                                                       types.SimpleNamespace(returncode=0)]) as run:
                self.assertFalse(ensure_codex_auth('codex.exe'))
                self.assertEqual([call.args[0] for call in run.call_args_list],
                                 [['codex.exe', 'login', 'status'], ['codex.exe', 'login']])

    def test_gateway_probe_uses_native_model_and_hides_failed_provider_output(self):
        config = {'codex_path': 'codex.exe', 'model': 'old-override', 'isolate_codex_config': True}
        with tempfile.TemporaryDirectory() as directory, patch('setup_helper.ROOT', Path(directory)), \
                patch.object(peer, 'codex_capabilities', return_value={'--ignore-user-config'}), \
                patch('setup_helper.subprocess.run', return_value=types.SimpleNamespace(
                    returncode=1, stderr='fixture-secret')) as run:
            with self.assertRaisesRegex(peer.PeerError, r'config.toml.*API 凭据') as failure:
                check_codex(config, provider_auth=True)
            command = run.call_args.args[0]
            self.assertNotIn('--ignore-user-config', command)
            self.assertNotIn('--model', command)
            self.assertNotIn('fixture-secret', str(failure.exception))
            self.assertEqual(config['model'], 'old-override')
    def test_old_codex_keeps_read_only_without_new_optional_flags(self):
        config = {"codex_path": "existing-codex.exe", "model": None}
        with patch.object(peer, "codex_capabilities", return_value={"--json"}):
            cmd, _ = peer.codex_command(config, {"workdir": "project"}, Path("out.json"), Path("schema.json"))
        self.assertEqual(cmd[:4], ["existing-codex.exe", "exec", "--sandbox", "read-only"])
        self.assertNotIn("--output-schema", cmd)
        self.assertNotIn("--ignore-user-config", cmd)
        self.assertNotIn("--model", cmd)

    def test_old_codex_jsonl_output_is_validated(self):
        answer = {"action": "finish", "to": "A", "text": "检查完成"}
        result = types.SimpleNamespace(stdout=json.dumps({"type": "item.completed", "item": {"type": "agent_message", "text": "```json\n" + json.dumps(answer) + "\n```"}}))
        self.assertEqual(peer.parse_agent_response(result, Path("missing.json"), {"--json"}), answer)

    def test_multiple_profiles_require_explicit_selection(self):
        data = {"profiles": [{"profile": "first", "userName": "first"}, {"profile": "chosen", "userName": "chosen"}]}
        self.assertEqual(choose_profile(data, input_fn=lambda _: "2")["profile"], "chosen")

    def test_unique_org_current_profile_can_be_reused(self):
        data = {"profiles": [{"profile": "first"}, {"profile": "current", "isOrgCurrent": True}]}
        self.assertEqual(choose_profile(data, input_fn=lambda _: self.fail("Should reuse current identity"))["profile"], "current")

    @unittest.skipUnless(os.name == "nt", "DPAPI requires Windows")
    def test_webhook_is_encrypted_on_disk_and_round_trips(self):
        value = "https://oapi.dingtalk.com/robot/send?access_token=nonproduction-test-value"
        with tempfile.TemporaryDirectory(prefix="AI peer test ") as directory:
            path = Path(directory) / "credential.protected"
            protect(value, path)
            self.assertNotIn(b"nonproduction-test-value", path.read_bytes())
            self.assertEqual(unprotect(path), value)


class GroupIdentityTests(unittest.TestCase):
    def setUp(self):
        self.template = json.loads((peer.ROOT / "config.example.json").read_text(encoding="utf-8"))
        self.legacy = copy.deepcopy(self.template)
        for participant in self.legacy["peers"]:
            participant.pop("bot_id")
            participant.pop("owner_member_id")
        self.membership = {
            "conversationId": self.template["group_id"], "complete": True,
            "bots": [
                {"openBotId": "yg2KpLMKmY6ortoRByPIfA==", "openDingtalkId": "DHeXhunz4sAEmHB3zWKnJg0M2jDBPii5U6", "name": "renamed A"},
                {"openBotId": "zftWZgz08tu3WGJ3nv/eag==", "openDingtalkId": "DHeXhunz4sAEmHB3zWKnJgiPItii9fiiiSPkp", "name": "renamed B"},
            ],
            "users": [
                {"memberDingtalkId": "$:LWCP_v1:$ejJxSWoBTL/ouRlIA69fSJ/1tfzQNUTP", "openDingtalkId": "Dw2iSNBiSVS1iSiSBsuMu0quiSNNIdkn5stNsV"},
                {"memberDingtalkId": "$:LWCP_v1:$dfjf0Q3MPjfimy8uXzxj1E2XiyIEbD2n", "openDingtalkId": "DHeXhunz4sAFXREWfCb7Ih7zUsYSb00IK"},
            ],
        }

    def test_colleague_ids_are_bound_by_stable_ids_not_names(self):
        original = copy.deepcopy(self.legacy)
        config = resolve_group_identities(self.legacy, self.membership, self.template)
        self.assertEqual(self.legacy, original)
        self.assertEqual(config["peers"][0]["sender_id"], self.membership["bots"][0]["openDingtalkId"])
        self.assertEqual(config["peers"][0]["owner_id"], self.membership["users"][0]["openDingtalkId"])
        self.assertEqual(config["peers"][1]["sender_id"], self.membership["bots"][1]["openDingtalkId"])
        self.assertEqual(config["peers"][1]["owner_id"], self.membership["users"][1]["openDingtalkId"])
        self.assertEqual(resolve_group_identities(config, self.membership, self.template), config)
        event = {"conversation_id": config["group_id"], "sender_open_dingtalk_id": config["peers"][1]["sender_id"]}
        self.assertTrue(peer.trusted(config, event, {"source": "B"}))
        event["sender_open_dingtalk_id"] = original["peers"][1]["sender_id"]
        self.assertFalse(peer.trusted(config, event, {"source": "B"}))

    def test_renamed_or_forged_name_does_not_replace_pinned_bot(self):
        self.membership["bots"][1].update(openBotId="different-bot", name=self.template["peers"][1]["name"])
        with self.assertRaises(peer.PeerError):
            resolve_group_identities(self.legacy, self.membership, self.template)

    def test_partial_wrong_group_or_duplicate_bot_is_rejected(self):
        cases = []
        for update in ({"complete": False}, {"partial": True}, {"hasMore": True}, {"failedCount": 1}, {"conversationId": "other-group"}):
            value = copy.deepcopy(self.membership)
            value.update(update)
            cases.append(value)
        duplicate = copy.deepcopy(self.membership)
        duplicate["bots"].append(copy.deepcopy(duplicate["bots"][0]))
        cases.append(duplicate)
        for membership in cases:
            with self.subTest(membership=membership), self.assertRaises(peer.PeerError):
                resolve_group_identities(self.legacy, membership, self.template)

    def test_missing_owner_or_unknown_legacy_identity_is_rejected(self):
        self.membership["users"].pop()
        with self.assertRaises(peer.PeerError):
            resolve_group_identities(self.legacy, self.membership, self.template)
        unknown = copy.deepcopy(self.legacy)
        unknown["peers"][0]["sender_id"] = "unrecognized-bot"
        with self.assertRaises(peer.PeerError):
            resolve_group_identities(unknown, self.membership, self.template)


if __name__ == "__main__":
    unittest.main()
