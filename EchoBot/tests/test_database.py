from __future__ import annotations

import asyncio
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from database import DailyDialogLimitReached, Database


class DatabaseTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.temp_directory = tempfile.TemporaryDirectory()
        self.database = Database(Path(self.temp_directory.name) / "test.db")
        await self.database.connect()

    async def asyncTearDown(self) -> None:
        await self.database.close()
        # На Windows антивирус может на несколько миллисекунд удержать SQLite WAL.
        for attempt in range(10):
            try:
                self.temp_directory.cleanup()
                break
            except OSError:
                if attempt == 9:
                    raise
                await asyncio.sleep(0.05)

    async def create_adult(
        self, user_id: int, gender: str | None = None, preferred: str = "any"
    ) -> None:
        await self.database.upsert_user(user_id, f"user{user_id}", f"User {user_id}")
        await self.database.confirm_adult(user_id)
        await self.database.set_gender(user_id, gender)
        await self.database.set_preferred_gender(user_id, preferred)

    async def test_matching_creates_one_active_dialog_for_both_users(self) -> None:
        await self.create_adult(1, "male")
        await self.create_adult(2, "female")

        self.assertIsNone(await self.database.enqueue_or_match(1))
        match = await self.database.enqueue_or_match(2)

        self.assertIsNotNone(match)
        self.assertEqual(match.partner_id, 1)
        self.assertEqual((await self.database.get_active(1)).partner_id, 2)
        self.assertEqual((await self.database.get_active(2)).partner_id, 1)

        first_room = await self.database.get_or_create_call_room(match.dialog_id)
        second_room = await self.database.get_or_create_call_room(match.dialog_id)
        self.assertEqual(first_room, second_room)
        self.assertTrue(first_room.startswith("EchoDialog-"))

    async def test_relayed_reply_target_is_mapped_in_both_directions(self) -> None:
        await self.create_adult(1)
        await self.create_adult(2)
        await self.database.enqueue_or_match(1)
        match = await self.database.enqueue_or_match(2)

        await self.database.record_relayed_message(
            match.dialog_id,
            sender_id=1,
            sender_message_id=101,
            recipient_id=2,
            recipient_message_id=201,
        )

        self.assertEqual(
            await self.database.get_relayed_reply_target(
                match.dialog_id, user_id=1, message_id=101, partner_id=2
            ),
            201,
        )
        self.assertEqual(
            await self.database.get_relayed_reply_target(
                match.dialog_id, user_id=2, message_id=201, partner_id=1
            ),
            101,
        )
        self.assertIsNone(
            await self.database.get_relayed_reply_target(
                match.dialog_id, user_id=2, message_id=999, partner_id=1
            )
        )

        await self.database.end_dialog(1, "stopped")
        self.assertIsNone(
            await self.database.get_relayed_reply_target(
                match.dialog_id, user_id=2, message_id=201, partner_id=1
            )
        )

    async def test_gender_filters_must_be_mutually_compatible(self) -> None:
        await self.create_adult(1, "male", preferred="female")
        await self.database.grant_membership(1, "premium", 7)
        await self.create_adult(2, "male", preferred="any")

        self.assertIsNone(await self.database.enqueue_or_match(1))
        self.assertIsNone(await self.database.enqueue_or_match(2))
        self.assertIsNone(await self.database.get_active(1))

        await self.create_adult(3, "female", preferred="male")
        await self.database.grant_membership(3, "premium", 7)
        match = await self.database.enqueue_or_match(3)
        self.assertIsNotNone(match)
        self.assertEqual(match.partner_id, 1)

    async def test_free_gender_filter_is_ignored(self) -> None:
        await self.create_adult(1, "male", preferred="female")
        await self.create_adult(2, "male", preferred="any")

        self.assertIsNone(await self.database.enqueue_or_match(1))
        match = await self.database.enqueue_or_match(2)

        self.assertIsNotNone(match)
        self.assertEqual(match.partner_id, 1)

    async def test_free_daily_dialog_limit_is_enforced(self) -> None:
        await self.create_adult(1)
        await self.create_adult(2)
        await self.database.enqueue_or_match(1, free_daily_limit=1)
        await self.database.enqueue_or_match(2, free_daily_limit=1)
        await self.database.end_dialog(1, "stopped")

        with self.assertRaises(DailyDialogLimitReached):
            await self.database.enqueue_or_match(1, free_daily_limit=1)

        await self.database.grant_membership(1, "premium", 7)
        self.assertIsNone(await self.database.enqueue_or_match(1, free_daily_limit=1))

    async def test_vip_gets_queue_priority(self) -> None:
        await self.create_adult(1, "male", preferred="female")
        await self.create_adult(2, "male", preferred="female")
        await self.create_adult(3, "female", preferred="male")
        await self.database.grant_membership(1, "premium", 7)
        await self.database.grant_membership(2, "vip", 365)
        await self.database.grant_membership(3, "premium", 7)

        await self.database.enqueue_or_match(1)
        await self.database.enqueue_or_match(2)
        match = await self.database.enqueue_or_match(3)

        self.assertIsNotNone(match)
        self.assertEqual(match.partner_id, 2)

    async def test_paid_order_credits_subscription_once(self) -> None:
        await self.create_adult(123456)
        expires_at = datetime.now(timezone.utc) + timedelta(hours=1)
        await self.database.create_payment_order(
            order_id="20260620123456123456",
            user_id=123456,
            plan_code="premium_7",
            amount_kopeks=14_900,
            invoice_url="https://example.test/pay",
            expires_at=expires_at,
        )
        result = await self.database.apply_ckassa_payment(
            payment_key="regPayNum:one",
            order_id="20260620123456123456",
            state="PAYED",
            payment={"state": "PAYED", "regPayNum": "one"},
        )
        duplicate = await self.database.apply_ckassa_payment(
            payment_key="regPayNum:one",
            order_id="20260620123456123456",
            state="PAYED",
            payment={"state": "PAYED", "regPayNum": "one"},
        )

        self.assertEqual(result["plan_code"], "premium_7")
        self.assertIsNone(duplicate)
        self.assertTrue((await self.database.get_membership(123456))["has_premium"])

    async def test_foreign_payment_event_is_not_consumed(self) -> None:
        await self.create_adult(123456)
        order_id = "7220260620123456123456123456"
        payment = {"state": "PAYED", "regPayNum": "foreign-first"}

        missing = await self.database.apply_ckassa_payment(
            payment_key="regPayNum:foreign-first:PAYED",
            order_id=order_id,
            state="PAYED",
            payment=payment,
        )
        self.assertIsNone(missing)

        await self.database.create_payment_order(
            order_id=order_id,
            user_id=123456,
            plan_code="premium_7",
            amount_kopeks=14_900,
            invoice_url="https://example.test/pay",
            expires_at=datetime.now(timezone.utc) + timedelta(hours=1),
        )
        credited = await self.database.apply_ckassa_payment(
            payment_key="regPayNum:foreign-first:PAYED",
            order_id=order_id,
            state="PAYED",
            payment=payment,
        )

        self.assertIsNotNone(credited)
        self.assertEqual(credited["order_id"], order_id)

    async def test_report_blocks_future_rematch(self) -> None:
        await self.create_adult(1)
        await self.create_adult(2)
        await self.database.enqueue_or_match(1)
        match = await self.database.enqueue_or_match(2)
        await self.database.end_dialog(1, "stopped")

        reported_id, created = await self.database.create_report(1, match.dialog_id, "spam")
        self.assertEqual(reported_id, 2)
        self.assertTrue(created)

        await self.database.enqueue_or_match(1)
        self.assertIsNone(await self.database.enqueue_or_match(2))
        self.assertIsNone(await self.database.get_active(1))

    async def test_delete_user_removes_profile(self) -> None:
        await self.create_adult(1)
        await self.database.toggle_interest(1, "music")

        await self.database.delete_user(1)

        self.assertIsNone(await self.database.get_user(1))
        self.assertEqual(await self.database.get_interests(1), set())

    async def test_start_source_uses_first_touch_and_counts_users(self) -> None:
        await self.database.upsert_user(1, "first", "First")
        await self.database.upsert_user(2, "second", "Second")

        self.assertEqual(
            await self.database.record_start_source(1, "channel_one"),
            (True, 1),
        )
        self.assertEqual(
            await self.database.record_start_source(1, "channel_two"),
            (False, 0),
        )
        self.assertEqual(
            await self.database.record_start_source(2, "channel_one"),
            (True, 2),
        )
        self.assertEqual(await self.database.source_stats(), [("channel_one", 2)])

    async def test_resolves_admin_id_by_username_case_insensitively(self) -> None:
        await self.database.upsert_user(963330818, "BimBim2BamBam", "Admin")

        self.assertEqual(
            await self.database.get_user_ids_by_usernames(
                frozenset({"bimbim2bambam"})
            ),
            {963330818},
        )

    async def test_notification_recipients_are_registered_and_reachable(self) -> None:
        await self.create_adult(1)
        await self.create_adult(2)
        await self.database.upsert_user(3, "user3", "User 3")
        await self.database.mark_unreachable(2)
        await self.database.set_banned(3, True)

        self.assertEqual(await self.database.notification_recipient_ids(), [1])
        self.assertEqual(await self.database.notification_recipients(), [(1, None)])

    async def test_scheduled_job_is_persistent_and_claimed_atomically(self) -> None:
        first_run = datetime(2026, 6, 21, 18, tzinfo=timezone.utc)
        next_run = first_run + timedelta(days=3)

        self.assertEqual(
            await self.database.get_or_create_scheduled_job("reminder", first_run),
            first_run,
        )
        self.assertTrue(
            await self.database.advance_scheduled_job("reminder", first_run, next_run)
        )
        self.assertFalse(
            await self.database.advance_scheduled_job("reminder", first_run, next_run)
        )
        self.assertEqual(
            await self.database.get_or_create_scheduled_job(
                "reminder", first_run + timedelta(days=30)
            ),
            next_run,
        )


if __name__ == "__main__":
    unittest.main()
