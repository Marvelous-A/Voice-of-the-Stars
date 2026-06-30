from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, Mock, call, patch

from reminders import (
    REMINDER_TEXT,
    broadcast_reminder,
    first_reminder_at,
    following_reminder_at,
    reminder_text,
)


class ReminderScheduleTests(unittest.TestCase):
    def test_first_reminder_is_next_nine_pm_in_moscow(self) -> None:
        before = datetime(2026, 6, 21, 17, 30, tzinfo=timezone.utc)
        after = datetime(2026, 6, 21, 18, 30, tzinfo=timezone.utc)

        self.assertEqual(
            first_reminder_at(before),
            datetime(2026, 6, 21, 18, tzinfo=timezone.utc),
        )
        self.assertEqual(
            first_reminder_at(after),
            datetime(2026, 6, 22, 18, tzinfo=timezone.utc),
        )

    def test_following_reminder_keeps_three_day_cadence(self) -> None:
        scheduled = datetime(2026, 6, 21, 18, tzinfo=timezone.utc)

        self.assertEqual(
            following_reminder_at(scheduled, scheduled),
            datetime(2026, 6, 24, 18, tzinfo=timezone.utc),
        )
        self.assertEqual(
            following_reminder_at(scheduled, scheduled + timedelta(days=7)),
            datetime(2026, 6, 30, 18, tzinfo=timezone.utc),
        )


class ReminderBroadcastTests(unittest.IsolatedAsyncioTestCase):
    async def test_broadcast_sends_to_every_eligible_recipient(self) -> None:
        bot = Mock()
        bot.send_message = AsyncMock()
        database = Mock()
        database.notification_recipients = AsyncMock(
            return_value=[(10, "male"), (20, "female"), (30, None)]
        )

        with (
            patch("reminders.asyncio.sleep", new=AsyncMock()),
            patch("reminders.secrets.choice", side_effect=["Алина", "Максим"]),
        ):
            result = await broadcast_reminder(bot, database)

        self.assertEqual(result, (3, 3))
        self.assertEqual(
            bot.send_message.await_args_list,
            [
                call(10, reminder_text_for_test("Алина", "Она")),
                call(20, reminder_text_for_test("Максим", "Он")),
                call(30, REMINDER_TEXT),
            ],
        )


def reminder_text_for_test(name: str, pronoun: str) -> str:
    return (
        f"<b>{name} ждёт тебя в «ЭХО».</b>\n\n"
        f"{pronoun} хочет поговорить именно с тобой — без имён, анкет и лишнего шума.\n\n"
        f"📡 Подай сигнал. {name} уже ждёт."
    )


class ReminderTextTests(unittest.TestCase):
    def test_male_recipient_gets_female_name(self) -> None:
        with patch("reminders.secrets.choice", return_value="Алина"):
            self.assertEqual(
                reminder_text("male"), reminder_text_for_test("Алина", "Она")
            )

    def test_female_recipient_gets_male_name(self) -> None:
        with patch("reminders.secrets.choice", return_value="Максим"):
            self.assertEqual(
                reminder_text("female"), reminder_text_for_test("Максим", "Он")
            )

    def test_unknown_gender_gets_certain_gender_neutral_text(self) -> None:
        self.assertEqual(reminder_text(None), REMINDER_TEXT)


if __name__ == "__main__":
    unittest.main()
