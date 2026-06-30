from __future__ import annotations

import asyncio
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import AsyncMock

from config import Settings
from database import Database
from payments import SubscriptionPayments


class SubscriptionPaymentSecurityTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.temp_directory = tempfile.TemporaryDirectory()
        database_path = Path(self.temp_directory.name) / "test.db"
        self.database = Database(database_path)
        await self.database.connect()
        self.settings = Settings(
            bot_token="test-token",
            database_path=database_path,
            call_base_url="https://meet.example.test",
            admin_ids=frozenset(),
            log_level="INFO",
        )
        self.payments = SubscriptionPayments(self.database, self.settings)
        await self.database.upsert_user(123456, "payer", "Payer")
        await self.database.confirm_adult(123456)
        await self.database.create_payment_order(
            order_id="7220260620123456123456123456",
            user_id=123456,
            plan_code="premium_7",
            amount_kopeks=14_900,
            invoice_url="https://example.test/pay",
            expires_at=datetime.now(timezone.utc) + timedelta(hours=1),
        )

    async def asyncTearDown(self) -> None:
        await self.database.close()
        for attempt in range(10):
            try:
                self.temp_directory.cleanup()
                break
            except OSError:
                if attempt == 9:
                    raise
                await asyncio.sleep(0.05)

    async def test_wrong_amount_is_quarantined_before_credit(self) -> None:
        wrong_payment = {
            "regPayNum": "wrong-amount",
            "state": "PAYED",
            "amount": 14_899,
            "properties": [
                "7220260620123456123456123456",
                "",
                "123456",
            ],
        }
        self.payments.client.get_new_payments = AsyncMock(
            return_value=[wrong_payment]
        )

        credited = await self.payments.process_updates()

        self.assertEqual(credited, [])
        membership = await self.database.get_membership(123456)
        self.assertFalse(membership["has_premium"])
        order = await self.database.get_payment_order(
            "7220260620123456123456123456"
        )
        self.assertEqual(order["status"], "created")

    async def test_matching_payment_is_credited(self) -> None:
        payment = {
            "regPayNum": "correct",
            "state": "PAYED",
            "amount": 14_900,
            "properties": [
                "7220260620123456123456123456",
                "",
                "123456",
            ],
        }
        self.payments.client.get_new_payments = AsyncMock(return_value=[payment])

        credited = await self.payments.process_updates()

        self.assertEqual(len(credited), 1)
        self.assertEqual(credited[0]["order_id"], payment["properties"][0])
        membership = await self.database.get_membership(123456)
        self.assertTrue(membership["has_premium"])


if __name__ == "__main__":
    unittest.main()
