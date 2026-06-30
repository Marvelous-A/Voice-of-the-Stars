from __future__ import annotations

import tempfile
import unittest
import sqlite3
from datetime import UTC, datetime
from pathlib import Path

from database import Database
from models import Event, Region


class DatabaseTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.db = Database(Path(self.temp_dir.name) / "test.sqlite3")
        await self.db.connect()

    async def asyncTearDown(self) -> None:
        await self.db.close()
        self.temp_dir.cleanup()

    async def test_user_region_and_notification_settings(self) -> None:
        self.assertTrue(await self.db.register_user(123, "test_user", "Test User"))
        self.assertFalse(await self.db.register_user(123, "renamed", "Renamed User"))
        self.assertEqual(await self.db.count_users(), 1)
        refreshed_user = await self.db.get_user(123)
        self.assertEqual(refreshed_user["username"], "renamed")
        region = Region(82, "Рязанская область", "ryazanskaya-oblast", 100, "")
        await self.db.set_region(123, region)

        user = await self.db.get_user(123)
        self.assertEqual(user["region_id"], 82)
        self.assertEqual(user["notifications_enabled"], 1)

        await self.db.set_notifications(123, False)
        user = await self.db.get_user(123)
        self.assertEqual(user["notifications_enabled"], 0)

        await self.db.set_notification_mode(123, "important")
        user = await self.db.get_user(123)
        self.assertEqual(user["notifications_enabled"], 1)
        self.assertEqual(user["notification_mode"], "important")

        await self.db.set_notification_scope(123, "all")
        user = await self.db.get_user(123)
        self.assertEqual(user["notification_scope"], "all")

    async def test_event_and_delivery_are_idempotent(self) -> None:
        await self.db.register_user(123)
        await self.db.set_region(
            123, Region(82, "Рязанская область", "ryazanskaya-oblast", 0, "")
        )
        event = Event(
            id=27953,
            published_at=datetime(2026, 6, 23, 17, 10, tzinfo=UTC),
            url="https://example.test/event",
            title="Опасность БПЛА",
            description="Описание",
            region_ids=(82,),
            region_names=("Рязанская область",),
        )

        self.assertTrue(await self.db.add_event(event))
        self.assertFalse(await self.db.add_event(event))
        await self.db.queue_delivery(event.id, 123)
        await self.db.queue_delivery(event.id, 123)

        deliveries = await self.db.pending_deliveries()
        self.assertEqual(len(deliveries), 1)
        self.assertEqual(deliveries[0]["event"], event)

        await self.db.mark_delivery_sent(event.id, 123)
        self.assertEqual(await self.db.pending_deliveries(), [])


class DatabaseMigrationTests(unittest.IsolatedAsyncioTestCase):
    async def test_adds_notification_mode_to_existing_database(self) -> None:
        temp_dir = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        path = Path(temp_dir.name) / "old.sqlite3"
        connection = sqlite3.connect(path)
        connection.execute(
            """
            CREATE TABLE users (
                telegram_id INTEGER PRIMARY KEY,
                username TEXT NOT NULL DEFAULT '',
                full_name TEXT NOT NULL DEFAULT '',
                region_id INTEGER,
                region_name TEXT NOT NULL DEFAULT '',
                region_slug TEXT NOT NULL DEFAULT '',
                notifications_enabled INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        connection.commit()
        connection.close()

        db = Database(path)
        await db.connect()
        try:
            await db.register_user(321)
            user = await db.get_user(321)
            self.assertEqual(user["notification_mode"], "all")
            self.assertEqual(user["notification_scope"], "region")
        finally:
            await db.close()
            temp_dir.cleanup()


if __name__ == "__main__":
    unittest.main()
