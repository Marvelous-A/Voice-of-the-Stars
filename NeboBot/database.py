from __future__ import annotations

import asyncio
import json
from datetime import datetime
from pathlib import Path
from typing import Any

import aiosqlite

from models import Event, Region


class Database:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.connection: aiosqlite.Connection | None = None
        self._lock = asyncio.Lock()

    async def connect(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = await aiosqlite.connect(self.path)
        self.connection.row_factory = aiosqlite.Row
        await self.connection.execute("PRAGMA journal_mode=WAL")
        await self.connection.execute("PRAGMA foreign_keys=ON")
        await self.connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS users (
                telegram_id INTEGER PRIMARY KEY,
                username TEXT NOT NULL DEFAULT '',
                full_name TEXT NOT NULL DEFAULT '',
                region_id INTEGER,
                region_name TEXT NOT NULL DEFAULT '',
                region_slug TEXT NOT NULL DEFAULT '',
                notifications_enabled INTEGER NOT NULL DEFAULT 1,
                notification_mode TEXT NOT NULL DEFAULT 'all',
                notification_scope TEXT NOT NULL DEFAULT 'region',
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS events (
                event_id INTEGER PRIMARY KEY,
                published_at TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                seeded INTEGER NOT NULL DEFAULT 0,
                discovered_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS deliveries (
                event_id INTEGER NOT NULL REFERENCES events(event_id) ON DELETE CASCADE,
                telegram_id INTEGER NOT NULL REFERENCES users(telegram_id) ON DELETE CASCADE,
                status TEXT NOT NULL DEFAULT 'pending',
                attempts INTEGER NOT NULL DEFAULT 0,
                last_error TEXT NOT NULL DEFAULT '',
                delivered_at TEXT,
                PRIMARY KEY (event_id, telegram_id)
            );

            CREATE TABLE IF NOT EXISTS metadata (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_users_region
                ON users(region_id, notifications_enabled);
            CREATE INDEX IF NOT EXISTS idx_deliveries_pending
                ON deliveries(status, attempts);
            """
        )
        columns_cursor = await self.connection.execute("PRAGMA table_info(users)")
        columns = {row["name"] for row in await columns_cursor.fetchall()}
        await columns_cursor.close()
        if "notification_mode" not in columns:
            await self.connection.execute(
                "ALTER TABLE users ADD COLUMN notification_mode TEXT NOT NULL DEFAULT 'all'"
            )
        if "notification_scope" not in columns:
            await self.connection.execute(
                "ALTER TABLE users ADD COLUMN notification_scope TEXT NOT NULL DEFAULT 'region'"
            )
        await self.connection.commit()

    async def close(self) -> None:
        if self.connection is not None:
            await self.connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            await self.connection.commit()
            await self.connection.close()
            self.connection = None

    def _db(self) -> aiosqlite.Connection:
        if self.connection is None:
            raise RuntimeError("База данных не подключена")
        return self.connection

    async def register_user(
        self, telegram_id: int, username: str = "", full_name: str = ""
    ) -> bool:
        async with self._lock:
            cursor = await self._db().execute(
                """
                INSERT OR IGNORE INTO users (telegram_id, username, full_name)
                VALUES (?, ?, ?)
                """,
                (telegram_id, username, full_name),
            )
            created = cursor.rowcount > 0
            await self._db().execute(
                """
                UPDATE users
                SET username=?, full_name=?, updated_at=CURRENT_TIMESTAMP
                WHERE telegram_id=?
                """,
                (username, full_name, telegram_id),
            )
            await self._db().commit()
            return created

    async def count_users(self) -> int:
        cursor = await self._db().execute("SELECT COUNT(*) FROM users")
        return int((await cursor.fetchone())[0])

    async def get_user(self, telegram_id: int) -> dict[str, Any] | None:
        cursor = await self._db().execute(
            "SELECT * FROM users WHERE telegram_id=?", (telegram_id,)
        )
        row = await cursor.fetchone()
        return dict(row) if row else None

    async def set_region(self, telegram_id: int, region: Region) -> None:
        async with self._lock:
            await self._db().execute(
                """
                UPDATE users
                SET region_id=?, region_name=?, region_slug=?,
                    notifications_enabled=1, updated_at=CURRENT_TIMESTAMP
                WHERE telegram_id=?
                """,
                (region.id, region.name, region.slug, telegram_id),
            )
            await self._db().commit()

    async def set_notifications(self, telegram_id: int, enabled: bool) -> None:
        async with self._lock:
            await self._db().execute(
                """
                UPDATE users SET notifications_enabled=?, updated_at=CURRENT_TIMESTAMP
                WHERE telegram_id=?
                """,
                (int(enabled), telegram_id),
            )
            await self._db().commit()

    async def set_notification_mode(self, telegram_id: int, mode: str) -> None:
        if mode not in {"all", "important", "off"}:
            raise ValueError(f"Неизвестный режим уведомлений: {mode}")
        stored_mode = "all" if mode == "off" else mode
        async with self._lock:
            await self._db().execute(
                """
                UPDATE users
                SET notifications_enabled=?, notification_mode=?,
                    updated_at=CURRENT_TIMESTAMP
                WHERE telegram_id=?
                """,
                (int(mode != "off"), stored_mode, telegram_id),
            )
            await self._db().commit()

    async def set_notification_scope(self, telegram_id: int, scope: str) -> None:
        if scope not in {"region", "all"}:
            raise ValueError(f"Неизвестная география уведомлений: {scope}")
        async with self._lock:
            await self._db().execute(
                """
                UPDATE users
                SET notification_scope=?, updated_at=CURRENT_TIMESTAMP
                WHERE telegram_id=?
                """,
                (scope, telegram_id),
            )
            await self._db().commit()

    async def delete_user(self, telegram_id: int) -> None:
        async with self._lock:
            await self._db().execute(
                "DELETE FROM users WHERE telegram_id=?", (telegram_id,)
            )
            await self._db().commit()

    async def list_subscribers(self) -> list[dict[str, Any]]:
        cursor = await self._db().execute(
            """
            SELECT * FROM users
            WHERE notifications_enabled=1 AND region_id IS NOT NULL
            """
        )
        return [dict(row) for row in await cursor.fetchall()]

    async def add_event(self, event: Event, *, seeded: bool = False) -> bool:
        async with self._lock:
            cursor = await self._db().execute(
                """
                INSERT OR IGNORE INTO events
                    (event_id, published_at, payload_json, seeded)
                VALUES (?, ?, ?, ?)
                """,
                (
                    event.id,
                    event.published_at.isoformat(),
                    json.dumps(event.to_payload(), ensure_ascii=False),
                    int(seeded),
                ),
            )
            await self._db().commit()
            return cursor.rowcount > 0

    async def latest_event_datetime(self) -> datetime | None:
        cursor = await self._db().execute(
            "SELECT published_at FROM events ORDER BY published_at DESC LIMIT 1"
        )
        row = await cursor.fetchone()
        return datetime.fromisoformat(row["published_at"]) if row else None

    async def queue_delivery(self, event_id: int, telegram_id: int) -> None:
        async with self._lock:
            await self._db().execute(
                """
                INSERT OR IGNORE INTO deliveries (event_id, telegram_id)
                VALUES (?, ?)
                """,
                (event_id, telegram_id),
            )
            await self._db().commit()

    async def pending_deliveries(self, limit: int = 100) -> list[dict[str, Any]]:
        cursor = await self._db().execute(
            """
            SELECT d.event_id, d.telegram_id, d.attempts, e.payload_json
            FROM deliveries AS d
            JOIN events AS e ON e.event_id=d.event_id
            WHERE d.status='pending' AND d.attempts < 5
            ORDER BY e.published_at ASC
            LIMIT ?
            """,
            (limit,),
        )
        result = []
        for row in await cursor.fetchall():
            item = dict(row)
            item["event"] = Event.from_payload(json.loads(item.pop("payload_json")))
            result.append(item)
        return result

    async def mark_delivery_sent(self, event_id: int, telegram_id: int) -> None:
        async with self._lock:
            await self._db().execute(
                """
                UPDATE deliveries
                SET status='delivered', attempts=attempts+1,
                    delivered_at=CURRENT_TIMESTAMP, last_error=''
                WHERE event_id=? AND telegram_id=?
                """,
                (event_id, telegram_id),
            )
            await self._db().commit()

    async def mark_delivery_failed(
        self, event_id: int, telegram_id: int, error: str, *, permanent: bool = False
    ) -> None:
        async with self._lock:
            await self._db().execute(
                """
                UPDATE deliveries
                SET status=?, attempts=attempts+1, last_error=?
                WHERE event_id=? AND telegram_id=?
                """,
                (
                    "failed" if permanent else "pending",
                    error[:500],
                    event_id,
                    telegram_id,
                ),
            )
            await self._db().commit()

    async def get_metadata(self, key: str) -> str | None:
        cursor = await self._db().execute(
            "SELECT value FROM metadata WHERE key=?", (key,)
        )
        row = await cursor.fetchone()
        return str(row["value"]) if row else None

    async def set_metadata(self, key: str, value: str) -> None:
        async with self._lock:
            await self._db().execute(
                """
                INSERT INTO metadata (key, value) VALUES (?, ?)
                ON CONFLICT(key) DO UPDATE SET value=excluded.value
                """,
                (key, value),
            )
            await self._db().commit()
