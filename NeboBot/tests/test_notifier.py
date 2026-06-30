from __future__ import annotations

import tempfile
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path

from database import Database
from models import Event, Region
from notifier import EventMonitor


def event(
    event_id: int,
    minute: int,
    *,
    title: str = "Опасность БПЛА",
    incident_type: str = "Опасность по БПЛА",
    region_id: int = 82,
    region_name: str = "Рязанская область",
) -> Event:
    return Event(
        id=event_id,
        published_at=datetime(2026, 6, 23, 17, minute, tzinfo=UTC),
        url=f"https://example.test/{event_id}",
        title=title,
        description="Описание",
        region_ids=(region_id,),
        region_names=(region_name,),
        incident_type=incident_type,
        threat_level="Высокий",
    )


class FakeBot:
    def __init__(self) -> None:
        self.messages: list[tuple[int, str]] = []

    async def send_message(self, chat_id: int, text: str, **kwargs) -> None:
        self.messages.append((chat_id, text))


class FakeSource:
    def __init__(self, initial: list[Event]) -> None:
        self.initial = initial
        self.since = list(initial)

    async def get_latest_events(self, limit: int = 100) -> list[Event]:
        return self.initial[:limit]

    async def get_events_since(self, after: datetime) -> list[Event]:
        return [item for item in self.since if item.published_at > after]


class EventMonitorTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.db = Database(Path(self.temp_dir.name) / "monitor.sqlite3")
        await self.db.connect()
        await self.db.register_user(123)
        await self.db.set_region(
            123, Region(82, "Рязанская область", "ryazanskaya-oblast", 0, "")
        )

    async def asyncTearDown(self) -> None:
        await self.db.close()
        self.temp_dir.cleanup()

    async def test_initial_feed_is_seeded_and_only_new_event_is_sent(self) -> None:
        old_event = event(1, 10)
        new_event = event(2, 11)
        source = FakeSource([old_event])
        bot = FakeBot()
        monitor = EventMonitor(bot, self.db, source, poll_interval_seconds=60)

        await monitor.run_once()
        self.assertEqual(bot.messages, [])

        source.since = [old_event, new_event]
        await monitor.run_once()
        self.assertEqual(len(bot.messages), 1)
        self.assertEqual(bot.messages[0][0], 123)
        self.assertIn("Опасность БПЛА", bot.messages[0][1])

    async def test_important_mode_keeps_threats_and_clear_messages(self) -> None:
        old_event = event(10, 10)
        source = FakeSource([old_event])
        bot = FakeBot()
        monitor = EventMonitor(bot, self.db, source, poll_interval_seconds=60)
        await monitor.run_once()
        await self.db.set_notification_mode(123, "important")

        source.since = [
            old_event,
            event(11, 11, title="Информационное сообщение", incident_type="Прочее"),
            event(12, 12),
            event(13, 13, title="Отбой опасности", incident_type="Отбой тревоги"),
        ]
        await monitor.run_once()

        self.assertEqual(len(bot.messages), 2)
        sent_text = "\n".join(text for _, text in bot.messages)
        self.assertIn("Опасность БПЛА", sent_text)
        self.assertIn("Отбой опасности", sent_text)
        self.assertNotIn("Информационное сообщение", sent_text)

    async def test_all_russia_scope_delivers_other_region(self) -> None:
        old_event = event(20, 10)
        source = FakeSource([old_event])
        bot = FakeBot()
        monitor = EventMonitor(bot, self.db, source, poll_interval_seconds=60)
        await monitor.run_once()

        source.since = [
            old_event,
            event(
                21,
                11,
                region_id=17,
                region_name="Краснодарский край",
            ),
        ]
        await monitor.run_once()
        self.assertEqual(bot.messages, [])

        await self.db.set_notification_scope(123, "all")
        source.since.append(
            event(
                22,
                12,
                region_id=13,
                region_name="Московская область",
            )
        )
        await monitor.run_once()
        self.assertEqual(len(bot.messages), 1)
        self.assertIn("Московская область", bot.messages[0][1])


if __name__ == "__main__":
    unittest.main()
