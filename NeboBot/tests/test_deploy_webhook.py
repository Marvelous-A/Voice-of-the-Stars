from __future__ import annotations

import hashlib
import hmac
import unittest
from unittest.mock import MagicMock, patch

import deploy_webhook


class DeployWebhookTests(unittest.TestCase):
    def test_valid_signature_accepts_matching_sha256_hmac(self) -> None:
        payload = b'{"ref":"refs/heads/main"}'
        secret = b"test-secret"
        signature = "sha256=" + hmac.new(secret, payload, hashlib.sha256).hexdigest()

        with patch.object(deploy_webhook, "SECRET", secret):
            self.assertTrue(deploy_webhook.valid_signature(payload, signature))
            self.assertFalse(deploy_webhook.valid_signature(payload + b"x", signature))

    def test_notification_is_sent_only_through_admin_bot(self) -> None:
        response = MagicMock()
        response.status = 200
        response.__enter__.return_value = response

        with (
            patch.object(deploy_webhook, "ADMIN_BOT_TOKEN", "admin-token"),
            patch.object(deploy_webhook, "NOTIFY_CHAT_IDS", (123,)),
            patch.object(deploy_webhook.urlrequest, "urlopen", return_value=response) as urlopen,
        ):
            delivered = deploy_webhook.send_telegram_notification("updated")

        self.assertEqual(delivered, 1)
        request = urlopen.call_args.args[0]
        self.assertEqual(
            request.full_url,
            "https://api.telegram.org/botadmin-token/sendMessage",
        )
        self.assertIn(b'"chat_id": 123', request.data)

    def test_missing_admin_token_skips_notification(self) -> None:
        with (
            patch.object(deploy_webhook, "ADMIN_BOT_TOKEN", ""),
            patch.object(deploy_webhook.urlrequest, "urlopen") as urlopen,
        ):
            self.assertEqual(deploy_webhook.send_telegram_notification("updated"), 0)
        urlopen.assert_not_called()


if __name__ == "__main__":
    unittest.main()
