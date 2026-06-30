from __future__ import annotations

import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path
from unittest.mock import patch

import deploy_webhook
from deploy_webhook import (
    resolve_notify_chat_ids,
    send_telegram_notification,
    success_message,
)


class DeployWebhookTests(unittest.TestCase):
    def test_resolves_recipient_by_telegram_username(self) -> None:
        with tempfile.TemporaryDirectory() as temp_directory:
            database_path = Path(temp_directory) / "echo.db"
            with closing(sqlite3.connect(database_path)) as connection:
                connection.execute(
                    "CREATE TABLE users (user_id INTEGER PRIMARY KEY, username TEXT)"
                )
                connection.execute(
                    "INSERT INTO users (user_id, username) VALUES (?, ?)",
                    (963330818, "BimBim2BamBam"),
                )
                connection.commit()

            result = resolve_notify_chat_ids(
                database_path,
                configured_ids=(),
                usernames=("bimbim2bambam",),
            )

        self.assertEqual(result, [963330818])

    def test_success_message_contains_verified_commit(self) -> None:
        message = success_message("abcdef123456", "Update matching")

        self.assertIn("abcdef1", message)
        self.assertIn("Update matching", message)
        self.assertIn("служба active", message)

    def test_deploy_notification_uses_admin_bot_token(self) -> None:
        class Response:
            status = 200

            def __enter__(self):
                return self

            def __exit__(self, *args):
                return None

        with (
            patch.object(deploy_webhook, "ADMIN_BOT_TOKEN", "admin-token"),
            patch.object(deploy_webhook, "resolve_notify_chat_ids", return_value=[123]),
            patch.object(
                deploy_webhook.urlrequest,
                "urlopen",
                return_value=Response(),
            ) as urlopen,
        ):
            delivered = send_telegram_notification("test")

        self.assertEqual(delivered, 1)
        request = urlopen.call_args.args[0]
        self.assertEqual(
            request.full_url,
            "https://api.telegram.org/botadmin-token/sendMessage",
        )

    def test_missing_admin_bot_token_does_not_fall_back_to_echo(self) -> None:
        with (
            patch.object(deploy_webhook, "ADMIN_BOT_TOKEN", ""),
            patch.object(deploy_webhook.urlrequest, "urlopen") as urlopen,
        ):
            delivered = send_telegram_notification("test")

        self.assertEqual(delivered, 0)
        urlopen.assert_not_called()


if __name__ == "__main__":
    unittest.main()
