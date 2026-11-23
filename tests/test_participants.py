import contextlib
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import peer
import setup_helper
from reliability import PeerError


def invitation():
    return {"group_id": "group", "project_id": "example-project", "peers": [
        {"id": name, "name": "机器人" + name, "owner_name": "同事" + name,
         "bot_id": "stable-bot-" + name, "owner_member_id": "stable-owner-" + name}
        for name in ("A", "C")]}


class ParticipantTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="新同事接入 ")
        self.root = Path(self.temp.name)
        self.addCleanup(self.temp.cleanup)
        (self.root / "config.example.json").write_bytes((peer.ROOT / "config.example.json").read_bytes())
        (self.root / "participants.json").write_text(json.dumps(invitation()), encoding="utf-8")
        item = patch.object(setup_helper, "ROOT", self.root)
        item.start()
        self.addCleanup(item.stop)

    def test_roster_can_be_read_before_toolchain_installation(self):
        output = io.StringIO()
        with patch("sys.argv", ["setup_helper.py", "participants"]), contextlib.redirect_stdout(output):
            self.assertEqual(setup_helper.main(), 0)
        roster = json.loads(output.getvalue())
        self.assertEqual([p["id"] for p in roster["peers"]], ["A", "C"])
        self.assertEqual(roster["project_id"], "example-project")
        self.assertFalse((self.root / ".tools").exists())

    def test_new_person_can_configure_from_invitation_without_source_changes(self):
        tools = dict(python_path="python", dws_path="dws", codex_path="codex")
        webhook = "https://oapi.dingtalk.com/robot/send?access_token=synthetic-C-token"
        with patch.object(setup_helper, "protect") as protect:
            path = setup_helper.save_config(tools, "C", self.root, webhook=webhook)
        config = json.loads(path.read_text(encoding="utf-8"))
        self.assertEqual(config["project_id"], "example-project")
        self.assertEqual([p["id"] for p in config["peers"]], ["A", "C"])
        self.assertEqual(config["peers"][1]["workdir"], str(self.root.resolve()))
        self.assertEqual(protect.call_args.args, (webhook, self.root / ".local/webhook-C.protected"))
        self.assertNotIn("synthetic-C-token", path.read_text(encoding="utf-8"))
        machine = json.loads((self.root / ".local/machine.json").read_text(encoding="utf-8"))
        self.assertEqual(machine["peer"], "C")

    def test_public_invitation_rejects_credentials_missing_pins_and_duplicates(self):
        cases = []
        value = invitation();value["access_token"] = "do-not-package";cases.append(value)
        value = invitation();value["peers"][1]["credential_file"] = "private";cases.append(value)
        value = invitation();value["peers"][1].pop("bot_id");cases.append(value)
        value = invitation();value["peers"][1]["owner_member_id"] = value["peers"][0]["owner_member_id"];cases.append(value)
        for value in cases:
            path = self.root / "invalid.json"
            path.write_text(json.dumps(value), encoding="utf-8")
            with self.subTest(value=value), self.assertRaises(PeerError):
                setup_helper.read_participants(path)

    def test_no_invitation_keeps_existing_deployment_roster(self):
        (self.root / "participants.json").unlink()
        template = setup_helper.configuration_template()
        self.assertEqual([p["id"] for p in template["peers"]], ["A", "B"])


if __name__ == "__main__":
    unittest.main()
