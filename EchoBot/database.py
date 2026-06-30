from __future__ import annotations

import asyncio
import json
import secrets
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import aiosqlite

from subscriptions import get_plan


@dataclass(frozen=True, slots=True)
class Match:
    dialog_id: int
    partner_id: int


@dataclass(frozen=True, slots=True)
class ActiveDialog:
    dialog_id: int
    user_id: int
    partner_id: int


class DailyDialogLimitReached(Exception):
    def __init__(self, used: int, limit: int) -> None:
        self.used = used
        self.limit = limit
        super().__init__(f"Daily dialog limit reached: {used}/{limit}")


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
        await self.connection.execute("PRAGMA busy_timeout=5000")
        await self._create_schema()

    async def close(self) -> None:
        if self.connection is not None:
            await self.connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            await self.connection.commit()
            await self.connection.close()
            self.connection = None

    def _db(self) -> aiosqlite.Connection:
        if self.connection is None:
            raise RuntimeError("Database.connect() должен быть вызван до работы с базой.")
        return self.connection

    async def _create_schema(self) -> None:
        db = self._db()
        await db.executescript(
            """
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                first_name TEXT NOT NULL DEFAULT '',
                is_adult INTEGER NOT NULL DEFAULT 0,
                gender TEXT CHECK (gender IN ('male', 'female') OR gender IS NULL),
                preferred_gender TEXT NOT NULL DEFAULT 'any'
                    CHECK (preferred_gender IN ('any', 'male', 'female')),
                blur_media INTEGER NOT NULL DEFAULT 1,
                premium_until TEXT,
                vip_until TEXT,
                is_banned INTEGER NOT NULL DEFAULT 0,
                can_receive INTEGER NOT NULL DEFAULT 1,
                last_partner_id INTEGER,
                last_dialog_id INTEGER,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                last_seen_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS user_interests (
                user_id INTEGER NOT NULL,
                interest TEXT NOT NULL,
                PRIMARY KEY (user_id, interest),
                FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS queue (
                user_id INTEGER PRIMARY KEY,
                enqueued_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS dialogs (
                dialog_id INTEGER PRIMARY KEY AUTOINCREMENT,
                user1_id INTEGER NOT NULL,
                user2_id INTEGER NOT NULL,
                started_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                ended_at TEXT,
                ended_by INTEGER,
                end_reason TEXT,
                call_room TEXT
            );

            CREATE TABLE IF NOT EXISTS active_chats (
                user_id INTEGER PRIMARY KEY,
                partner_id INTEGER NOT NULL,
                dialog_id INTEGER NOT NULL,
                FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE,
                FOREIGN KEY (dialog_id) REFERENCES dialogs(dialog_id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS relayed_messages (
                dialog_id INTEGER NOT NULL,
                sender_id INTEGER NOT NULL,
                sender_message_id INTEGER NOT NULL,
                recipient_id INTEGER NOT NULL,
                recipient_message_id INTEGER NOT NULL,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (dialog_id, sender_id, sender_message_id),
                UNIQUE (dialog_id, recipient_id, recipient_message_id),
                FOREIGN KEY (dialog_id) REFERENCES dialogs(dialog_id) ON DELETE CASCADE,
                FOREIGN KEY (sender_id) REFERENCES users(user_id) ON DELETE CASCADE,
                FOREIGN KEY (recipient_id) REFERENCES users(user_id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS ratings (
                dialog_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                value INTEGER NOT NULL CHECK (value IN (-1, 1)),
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (dialog_id, user_id),
                FOREIGN KEY (dialog_id) REFERENCES dialogs(dialog_id) ON DELETE CASCADE,
                FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS reports (
                report_id INTEGER PRIMARY KEY AUTOINCREMENT,
                dialog_id INTEGER NOT NULL,
                reporter_id INTEGER NOT NULL,
                reported_id INTEGER NOT NULL,
                reason TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                UNIQUE (dialog_id, reporter_id),
                FOREIGN KEY (dialog_id) REFERENCES dialogs(dialog_id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS blocks (
                user_id INTEGER NOT NULL,
                blocked_user_id INTEGER NOT NULL,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (user_id, blocked_user_id),
                FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS start_sources (
                user_id INTEGER PRIMARY KEY,
                source TEXT NOT NULL CHECK (length(source) BETWEEN 1 AND 64),
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS payment_orders (
                order_id TEXT PRIMARY KEY,
                user_id INTEGER NOT NULL,
                plan_code TEXT NOT NULL,
                amount_kopeks INTEGER NOT NULL CHECK (amount_kopeks > 0),
                invoice_url TEXT NOT NULL,
                expires_at TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'created',
                credited INTEGER NOT NULL DEFAULT 0,
                payment_json TEXT,
                receipt TEXT,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                paid_at TEXT,
                credited_at TEXT
            );

            CREATE TABLE IF NOT EXISTS processed_payments (
                payment_key TEXT PRIMARY KEY,
                order_id TEXT,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS scheduled_jobs (
                job_name TEXT PRIMARY KEY,
                next_run_at TEXT NOT NULL,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE INDEX IF NOT EXISTS idx_queue_enqueued_at ON queue(enqueued_at);
            CREATE INDEX IF NOT EXISTS idx_dialogs_users ON dialogs(user1_id, user2_id);
            CREATE INDEX IF NOT EXISTS idx_reports_reported ON reports(reported_id);
            CREATE INDEX IF NOT EXISTS idx_active_partner ON active_chats(partner_id);
            CREATE INDEX IF NOT EXISTS idx_start_sources_source ON start_sources(source);
            CREATE INDEX IF NOT EXISTS idx_payment_orders_user
                ON payment_orders(user_id, created_at DESC);
            CREATE INDEX IF NOT EXISTS idx_payment_orders_status
                ON payment_orders(status, credited);
            """
        )
        cursor = await db.execute("PRAGMA table_info(users)")
        user_columns = {row["name"] for row in await cursor.fetchall()}
        if "premium_until" not in user_columns:
            await db.execute("ALTER TABLE users ADD COLUMN premium_until TEXT")
        if "vip_until" not in user_columns:
            await db.execute("ALTER TABLE users ADD COLUMN vip_until TEXT")
        cursor = await db.execute("PRAGMA table_info(dialogs)")
        dialog_columns = {row["name"] for row in await cursor.fetchall()}
        if "call_room" not in dialog_columns:
            await db.execute("ALTER TABLE dialogs ADD COLUMN call_room TEXT")
        await db.commit()

    async def upsert_user(self, user_id: int, username: str | None, first_name: str) -> None:
        async with self._lock:
            db = self._db()
            await db.execute(
                """
                INSERT INTO users (user_id, username, first_name)
                VALUES (?, ?, ?)
                ON CONFLICT(user_id) DO UPDATE SET
                    username = excluded.username,
                    first_name = excluded.first_name,
                    can_receive = 1,
                    updated_at = CURRENT_TIMESTAMP,
                    last_seen_at = CURRENT_TIMESTAMP
                """,
                (user_id, username, first_name),
            )
            await db.commit()

    async def get_user(self, user_id: int) -> dict[str, Any] | None:
        async with self._lock:
            cursor = await self._db().execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
            row = await cursor.fetchone()
            return dict(row) if row else None

    async def get_membership(self, user_id: int) -> dict[str, Any]:
        async with self._lock:
            cursor = await self._db().execute(
                "SELECT premium_until, vip_until FROM users WHERE user_id = ?",
                (user_id,),
            )
            row = await cursor.fetchone()
            return _membership_from_row(row)

    async def has_premium(self, user_id: int) -> bool:
        return bool((await self.get_membership(user_id))["has_premium"])

    async def is_vip(self, user_id: int) -> bool:
        return bool((await self.get_membership(user_id))["is_vip"])

    async def grant_membership(
        self, user_id: int, tier: str, duration_days: int
    ) -> dict[str, Any]:
        if tier not in {"premium", "vip"} or duration_days <= 0:
            raise ValueError("Некорректный уровень или срок подписки.")
        async with self._lock:
            db = self._db()
            cursor = await db.execute(
                "SELECT premium_until, vip_until FROM users WHERE user_id = ?",
                (user_id,),
            )
            row = await cursor.fetchone()
            if not row:
                raise ValueError("Пользователь не найден.")
            current_expiry = row[f"{tier}_until"]
            if tier == "premium":
                current_expiry = _latest_expiry(current_expiry, row["vip_until"])
            expires_at = _extended_expiry(current_expiry, duration_days)
            await db.execute(
                f"UPDATE users SET {tier}_until = ?, updated_at = CURRENT_TIMESTAMP "
                "WHERE user_id = ?",
                (_db_datetime(expires_at), user_id),
            )
            await db.commit()
            cursor = await db.execute(
                "SELECT premium_until, vip_until FROM users WHERE user_id = ?",
                (user_id,),
            )
            return _membership_from_row(await cursor.fetchone())

    async def daily_dialog_usage(self, user_id: int) -> int:
        since = _moscow_day_start_utc()
        async with self._lock:
            cursor = await self._db().execute(
                """
                SELECT COUNT(*) FROM dialogs
                WHERE started_at >= ? AND (user1_id = ? OR user2_id = ?)
                """,
                (_db_datetime(since), user_id, user_id),
            )
            row = await cursor.fetchone()
            return int(row[0])

    async def dialog_count(self, user_id: int) -> int:
        async with self._lock:
            cursor = await self._db().execute(
                "SELECT COUNT(*) FROM dialogs WHERE user1_id = ? OR user2_id = ?",
                (user_id, user_id),
            )
            row = await cursor.fetchone()
            return int(row[0])

    async def get_user_ids_by_usernames(self, usernames: frozenset[str]) -> set[int]:
        if not usernames:
            return set()
        async with self._lock:
            placeholders = ",".join("?" for _ in usernames)
            cursor = await self._db().execute(
                f"SELECT user_id FROM users WHERE lower(username) IN ({placeholders})",
                tuple(usernames),
            )
            return {int(row["user_id"]) for row in await cursor.fetchall()}

    async def notification_recipient_ids(self) -> list[int]:
        """Return registered adults who can still receive bot messages."""
        return [user_id for user_id, _gender in await self.notification_recipients()]

    async def notification_recipients(self) -> list[tuple[int, str | None]]:
        """Return eligible notification recipients together with their gender."""
        async with self._lock:
            cursor = await self._db().execute(
                """
                SELECT user_id, gender FROM users
                WHERE is_adult = 1 AND is_banned = 0 AND can_receive = 1
                ORDER BY user_id
                """
            )
            return [
                (int(row["user_id"]), row["gender"])
                for row in await cursor.fetchall()
            ]

    async def get_or_create_scheduled_job(
        self, job_name: str, first_run_at: datetime
    ) -> datetime:
        async with self._lock:
            db = self._db()
            await db.execute(
                """
                INSERT OR IGNORE INTO scheduled_jobs (job_name, next_run_at)
                VALUES (?, ?)
                """,
                (job_name, _db_datetime(first_run_at)),
            )
            cursor = await db.execute(
                "SELECT next_run_at FROM scheduled_jobs WHERE job_name = ?",
                (job_name,),
            )
            row = await cursor.fetchone()
            await db.commit()
            scheduled_at = _parse_db_datetime(row["next_run_at"] if row else None)
            if scheduled_at is None:
                raise RuntimeError(f"Не удалось прочитать расписание {job_name!r}.")
            return scheduled_at

    async def advance_scheduled_job(
        self,
        job_name: str,
        expected_run_at: datetime,
        next_run_at: datetime,
    ) -> bool:
        """Atomically claim a run and move the job to its next scheduled time."""
        async with self._lock:
            db = self._db()
            cursor = await db.execute(
                """
                UPDATE scheduled_jobs
                SET next_run_at = ?, updated_at = CURRENT_TIMESTAMP
                WHERE job_name = ? AND next_run_at = ?
                """,
                (
                    _db_datetime(next_run_at),
                    job_name,
                    _db_datetime(expected_run_at),
                ),
            )
            await db.commit()
            return cursor.rowcount > 0

    async def record_start_source(self, user_id: int, source: str) -> tuple[bool, int]:
        """Сохраняет первый рекламный источник пользователя и возвращает его охват."""
        async with self._lock:
            db = self._db()
            cursor = await db.execute(
                "INSERT OR IGNORE INTO start_sources (user_id, source) VALUES (?, ?)",
                (user_id, source),
            )
            created = cursor.rowcount > 0
            count_cursor = await db.execute(
                "SELECT COUNT(*) FROM start_sources WHERE source = ?",
                (source,),
            )
            count_row = await count_cursor.fetchone()
            await db.commit()
            return created, int(count_row[0])

    async def source_stats(self, limit: int = 10) -> list[tuple[str, int]]:
        async with self._lock:
            cursor = await self._db().execute(
                """
                SELECT source, COUNT(*) AS users
                FROM start_sources
                GROUP BY source
                ORDER BY users DESC, source ASC
                LIMIT ?
                """,
                (limit,),
            )
            return [(str(row["source"]), int(row["users"])) for row in await cursor.fetchall()]

    async def find_active_payment_order(
        self, user_id: int, plan_code: str
    ) -> dict[str, Any] | None:
        async with self._lock:
            cursor = await self._db().execute(
                """
                SELECT * FROM payment_orders
                WHERE user_id = ? AND plan_code = ? AND status = 'created'
                  AND expires_at > CURRENT_TIMESTAMP
                ORDER BY created_at DESC
                LIMIT 1
                """,
                (user_id, plan_code),
            )
            row = await cursor.fetchone()
            return dict(row) if row else None

    async def create_payment_order(
        self,
        *,
        order_id: str,
        user_id: int,
        plan_code: str,
        amount_kopeks: int,
        invoice_url: str,
        expires_at: datetime,
    ) -> dict[str, Any]:
        if not get_plan(plan_code):
            raise ValueError("Неизвестный тариф.")
        async with self._lock:
            db = self._db()
            await db.execute(
                """
                INSERT INTO payment_orders
                    (order_id, user_id, plan_code, amount_kopeks, invoice_url, expires_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    str(order_id),
                    user_id,
                    plan_code,
                    amount_kopeks,
                    invoice_url,
                    _db_datetime(expires_at),
                ),
            )
            await db.commit()
            cursor = await db.execute(
                "SELECT * FROM payment_orders WHERE order_id = ?", (str(order_id),)
            )
            return dict(await cursor.fetchone())

    async def get_payment_order(self, order_id: str) -> dict[str, Any] | None:
        async with self._lock:
            cursor = await self._db().execute(
                "SELECT * FROM payment_orders WHERE order_id = ?", (str(order_id),)
            )
            row = await cursor.fetchone()
            return dict(row) if row else None

    async def mark_ckassa_payment_seen(
        self, *, payment_key: str, order_id: str
    ) -> bool:
        async with self._lock:
            cursor = await self._db().execute(
                "INSERT OR IGNORE INTO processed_payments (payment_key, order_id) "
                "VALUES (?, ?)",
                (payment_key, str(order_id)),
            )
            await self._db().commit()
            return cursor.rowcount > 0

    async def apply_ckassa_payment(
        self,
        *,
        payment_key: str,
        order_id: str | None,
        state: str,
        payment: dict[str, Any],
    ) -> dict[str, Any] | None:
        normalized_state = (state or "unknown").upper()
        async with self._lock:
            db = self._db()
            await db.execute("BEGIN IMMEDIATE")
            try:
                if not order_id:
                    await db.commit()
                    return None
                cursor = await db.execute(
                    "SELECT * FROM payment_orders WHERE order_id = ?", (order_id,)
                )
                order = await cursor.fetchone()
                if not order:
                    # Do not consume or remember events owned by another bot.
                    await db.commit()
                    return None

                cursor = await db.execute(
                    "INSERT OR IGNORE INTO processed_payments (payment_key, order_id) "
                    "VALUES (?, ?)",
                    (payment_key, order_id),
                )
                if cursor.rowcount == 0:
                    await db.rollback()
                    return None
                payment_json = json.dumps(payment, ensure_ascii=False)
                if normalized_state != "PAYED":
                    terminal_states = {
                        "CANCELED",
                        "CANCELLED",
                        "DECLINED",
                        "ERROR",
                        "FAILED",
                        "REFUNDED",
                        "REJECTED",
                    }
                    status = (
                        normalized_state.lower()
                        if normalized_state in terminal_states
                        else str(order["status"])
                    )
                    await db.execute(
                        """
                        UPDATE payment_orders
                        SET status = ?, payment_json = ?, updated_at = CURRENT_TIMESTAMP
                        WHERE order_id = ?
                        """,
                        (status, payment_json, order_id),
                    )
                    await db.commit()
                    return None

                if order["credited"]:
                    await db.commit()
                    return None
                plan = get_plan(str(order["plan_code"]))
                if not plan or int(order["amount_kopeks"]) != plan.price_kopeks:
                    await db.rollback()
                    raise ValueError("Заказ содержит неизвестный или изменённый тариф.")

                cursor = await db.execute(
                    "SELECT premium_until, vip_until FROM users WHERE user_id = ?",
                    (int(order["user_id"]),),
                )
                user = await cursor.fetchone()
                if not user:
                    await db.execute(
                        """
                        UPDATE payment_orders SET status = 'payed', payment_json = ?,
                            paid_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
                        WHERE order_id = ?
                        """,
                        (payment_json, order_id),
                    )
                    await db.commit()
                    return None

                field = f"{plan.tier}_until"
                current_expiry = user[field]
                if plan.tier == "premium":
                    current_expiry = _latest_expiry(current_expiry, user["vip_until"])
                expires_at = _extended_expiry(current_expiry, plan.duration_days)
                await db.execute(
                    f"UPDATE users SET {field} = ?, updated_at = CURRENT_TIMESTAMP "
                    "WHERE user_id = ?",
                    (_db_datetime(expires_at), int(order["user_id"])),
                )
                receipt = str(payment.get("receipt") or "")
                await db.execute(
                    """
                    UPDATE payment_orders
                    SET status = 'payed', credited = 1, payment_json = ?, receipt = ?,
                        paid_at = CURRENT_TIMESTAMP, credited_at = CURRENT_TIMESTAMP,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE order_id = ?
                    """,
                    (payment_json, receipt, order_id),
                )
                await db.commit()
                return {
                    "order_id": order_id,
                    "user_id": int(order["user_id"]),
                    "plan_code": plan.code,
                    "plan_title": plan.title,
                    "amount_kopeks": int(order["amount_kopeks"]),
                    "tier": plan.tier,
                    "expires_at": _db_datetime(expires_at),
                    "expires_display": _display_moscow_datetime(expires_at),
                    "receipt": receipt,
                }
            except Exception:
                await db.rollback()
                raise

    async def confirm_adult(self, user_id: int) -> None:
        await self._update_user(user_id, "is_adult = 1")

    async def set_gender(self, user_id: int, gender: str | None) -> None:
        if gender not in {None, "male", "female"}:
            raise ValueError("Недопустимое значение пола.")
        async with self._lock:
            db = self._db()
            await db.execute(
                "UPDATE users SET gender = ?, updated_at = CURRENT_TIMESTAMP WHERE user_id = ?",
                (gender, user_id),
            )
            await db.commit()

    async def set_preferred_gender(self, user_id: int, gender: str) -> None:
        if gender not in {"any", "male", "female"}:
            raise ValueError("Недопустимое значение фильтра пола.")
        async with self._lock:
            db = self._db()
            await db.execute(
                """
                UPDATE users
                SET preferred_gender = ?, updated_at = CURRENT_TIMESTAMP
                WHERE user_id = ?
                """,
                (gender, user_id),
            )
            await db.commit()

    async def toggle_blur_media(self, user_id: int) -> bool:
        async with self._lock:
            db = self._db()
            await db.execute(
                """
                UPDATE users
                SET blur_media = CASE blur_media WHEN 1 THEN 0 ELSE 1 END,
                    updated_at = CURRENT_TIMESTAMP
                WHERE user_id = ?
                """,
                (user_id,),
            )
            await db.commit()
            cursor = await db.execute("SELECT blur_media FROM users WHERE user_id = ?", (user_id,))
            row = await cursor.fetchone()
            return bool(row["blur_media"])

    async def _update_user(self, user_id: int, assignment: str) -> None:
        async with self._lock:
            db = self._db()
            await db.execute(
                f"UPDATE users SET {assignment}, updated_at = CURRENT_TIMESTAMP WHERE user_id = ?",
                (user_id,),
            )
            await db.commit()

    async def get_interests(self, user_id: int) -> set[str]:
        async with self._lock:
            cursor = await self._db().execute(
                "SELECT interest FROM user_interests WHERE user_id = ?", (user_id,)
            )
            rows = await cursor.fetchall()
            return {row["interest"] for row in rows}

    async def toggle_interest(self, user_id: int, interest: str) -> bool:
        async with self._lock:
            db = self._db()
            cursor = await db.execute(
                "SELECT 1 FROM user_interests WHERE user_id = ? AND interest = ?",
                (user_id, interest),
            )
            exists = await cursor.fetchone()
            if exists:
                await db.execute(
                    "DELETE FROM user_interests WHERE user_id = ? AND interest = ?",
                    (user_id, interest),
                )
                selected = False
            else:
                await db.execute(
                    "INSERT INTO user_interests (user_id, interest) VALUES (?, ?)",
                    (user_id, interest),
                )
                selected = True
            await db.commit()
            return selected

    async def clear_interests(self, user_id: int) -> None:
        async with self._lock:
            db = self._db()
            await db.execute("DELETE FROM user_interests WHERE user_id = ?", (user_id,))
            await db.commit()

    async def get_active(self, user_id: int) -> ActiveDialog | None:
        async with self._lock:
            cursor = await self._db().execute(
                "SELECT partner_id, dialog_id FROM active_chats WHERE user_id = ?", (user_id,)
            )
            row = await cursor.fetchone()
            if not row:
                return None
            return ActiveDialog(row["dialog_id"], user_id, row["partner_id"])

    async def record_relayed_message(
        self,
        dialog_id: int,
        sender_id: int,
        sender_message_id: int,
        recipient_id: int,
        recipient_message_id: int,
    ) -> None:
        async with self._lock:
            db = self._db()
            await db.execute(
                """
                INSERT INTO relayed_messages (
                    dialog_id,
                    sender_id,
                    sender_message_id,
                    recipient_id,
                    recipient_message_id
                )
                SELECT ?, ?, ?, ?, ?
                WHERE EXISTS (
                    SELECT 1
                    FROM active_chats
                    WHERE user_id = ? AND partner_id = ? AND dialog_id = ?
                )
                ON CONFLICT (dialog_id, sender_id, sender_message_id) DO UPDATE SET
                    recipient_id = excluded.recipient_id,
                    recipient_message_id = excluded.recipient_message_id
                """,
                (
                    dialog_id,
                    sender_id,
                    sender_message_id,
                    recipient_id,
                    recipient_message_id,
                    sender_id,
                    recipient_id,
                    dialog_id,
                ),
            )
            await db.commit()

    async def get_relayed_reply_target(
        self,
        dialog_id: int,
        user_id: int,
        message_id: int,
        partner_id: int,
    ) -> int | None:
        async with self._lock:
            cursor = await self._db().execute(
                """
                SELECT CASE
                           WHEN sender_id = ? THEN recipient_message_id
                           ELSE sender_message_id
                       END AS target_message_id
                FROM relayed_messages
                WHERE dialog_id = ?
                  AND (
                      (sender_id = ? AND sender_message_id = ? AND recipient_id = ?)
                      OR
                      (recipient_id = ? AND recipient_message_id = ? AND sender_id = ?)
                  )
                LIMIT 1
                """,
                (
                    user_id,
                    dialog_id,
                    user_id,
                    message_id,
                    partner_id,
                    user_id,
                    message_id,
                    partner_id,
                ),
            )
            row = await cursor.fetchone()
            return int(row["target_message_id"]) if row else None

    async def is_searching(self, user_id: int) -> bool:
        async with self._lock:
            cursor = await self._db().execute("SELECT 1 FROM queue WHERE user_id = ?", (user_id,))
            return await cursor.fetchone() is not None

    async def enqueue_or_match(
        self, user_id: int, free_daily_limit: int = 15
    ) -> Match | None:
        async with self._lock:
            db = self._db()
            await db.execute("BEGIN IMMEDIATE")
            try:
                cursor = await db.execute(
                    """
                    SELECT gender, preferred_gender, is_adult, is_banned,
                           premium_until, vip_until
                    FROM users WHERE user_id = ?
                    """,
                    (user_id,),
                )
                current = await cursor.fetchone()
                if not current or not current["is_adult"] or current["is_banned"]:
                    await db.rollback()
                    return None

                membership = _membership_from_row(current)
                if not membership["has_premium"]:
                    cursor = await db.execute(
                        """
                        SELECT COUNT(*) FROM dialogs
                        WHERE started_at >= ? AND (user1_id = ? OR user2_id = ?)
                        """,
                        (_db_datetime(_moscow_day_start_utc()), user_id, user_id),
                    )
                    used = int((await cursor.fetchone())[0])
                    if used >= free_daily_limit:
                        await db.rollback()
                        raise DailyDialogLimitReached(used, free_daily_limit)

                effective_preference = (
                    str(current["preferred_gender"])
                    if membership["has_premium"]
                    else "any"
                )
                cursor = await db.execute(
                    "SELECT 1 FROM active_chats WHERE user_id = ?", (user_id,)
                )
                if await cursor.fetchone():
                    await db.rollback()
                    return None

                cursor = await db.execute(
                    """
                    SELECT q.user_id,
                           (
                               SELECT COUNT(*)
                               FROM user_interests mine
                               JOIN user_interests theirs
                                 ON theirs.interest = mine.interest
                               WHERE mine.user_id = ?
                                 AND theirs.user_id = q.user_id
                           ) AS common_interests
                    FROM queue q
                    JOIN users candidate ON candidate.user_id = q.user_id
                    WHERE q.user_id <> ?
                      AND candidate.is_adult = 1
                      AND candidate.is_banned = 0
                      AND candidate.can_receive = 1
                      AND NOT EXISTS (
                          SELECT 1 FROM active_chats ac WHERE ac.user_id = q.user_id
                      )
                      AND (? = 'any' OR candidate.gender = ?)
                      AND (
                          CASE
                              WHEN candidate.premium_until > CURRENT_TIMESTAMP
                                OR candidate.vip_until > CURRENT_TIMESTAMP
                              THEN candidate.preferred_gender
                              ELSE 'any'
                          END = 'any'
                          OR (
                              CASE
                                  WHEN candidate.premium_until > CURRENT_TIMESTAMP
                                    OR candidate.vip_until > CURRENT_TIMESTAMP
                                  THEN candidate.preferred_gender
                                  ELSE 'any'
                              END = ?
                          )
                      )
                      AND NOT EXISTS (
                          SELECT 1 FROM blocks b
                          WHERE (b.user_id = ? AND b.blocked_user_id = q.user_id)
                             OR (b.user_id = q.user_id AND b.blocked_user_id = ?)
                      )
                    ORDER BY
                        CASE WHEN candidate.vip_until > CURRENT_TIMESTAMP THEN 0 ELSE 1 END,
                        common_interests DESC,
                        q.enqueued_at ASC
                    LIMIT 1
                    """,
                    (
                        user_id,
                        user_id,
                        effective_preference,
                        effective_preference,
                        current["gender"],
                        user_id,
                        user_id,
                    ),
                )
                candidate = await cursor.fetchone()
                if not candidate:
                    await db.execute(
                        """
                        INSERT INTO queue (user_id, enqueued_at)
                        VALUES (?, CURRENT_TIMESTAMP)
                        ON CONFLICT(user_id) DO NOTHING
                        """,
                        (user_id,),
                    )
                    await db.commit()
                    return None

                partner_id = int(candidate["user_id"])
                await db.execute("DELETE FROM queue WHERE user_id IN (?, ?)", (user_id, partner_id))
                cursor = await db.execute(
                    "INSERT INTO dialogs (user1_id, user2_id) VALUES (?, ?)",
                    (user_id, partner_id),
                )
                dialog_id = int(cursor.lastrowid)
                await db.executemany(
                    "INSERT INTO active_chats (user_id, partner_id, dialog_id) VALUES (?, ?, ?)",
                    (
                        (user_id, partner_id, dialog_id),
                        (partner_id, user_id, dialog_id),
                    ),
                )
                await db.commit()
                return Match(dialog_id=dialog_id, partner_id=partner_id)
            except Exception:
                await db.rollback()
                raise

    async def cancel_search(self, user_id: int) -> bool:
        async with self._lock:
            db = self._db()
            cursor = await db.execute("DELETE FROM queue WHERE user_id = ?", (user_id,))
            await db.commit()
            return cursor.rowcount > 0

    async def end_dialog(self, user_id: int, reason: str) -> ActiveDialog | None:
        async with self._lock:
            db = self._db()
            await db.execute("BEGIN IMMEDIATE")
            try:
                cursor = await db.execute(
                    "SELECT partner_id, dialog_id FROM active_chats WHERE user_id = ?",
                    (user_id,),
                )
                row = await cursor.fetchone()
                if not row:
                    await db.rollback()
                    return None

                partner_id = int(row["partner_id"])
                dialog_id = int(row["dialog_id"])
                await db.execute(
                    "DELETE FROM active_chats WHERE user_id IN (?, ?)", (user_id, partner_id)
                )
                await db.execute(
                    "DELETE FROM relayed_messages WHERE dialog_id = ?", (dialog_id,)
                )
                await db.execute(
                    """
                    UPDATE dialogs
                    SET ended_at = COALESCE(ended_at, CURRENT_TIMESTAMP),
                        ended_by = COALESCE(ended_by, ?),
                        end_reason = COALESCE(end_reason, ?)
                    WHERE dialog_id = ?
                    """,
                    (user_id, reason, dialog_id),
                )
                await db.executemany(
                    """
                    UPDATE users
                    SET last_partner_id = ?, last_dialog_id = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE user_id = ?
                    """,
                    (
                        (partner_id, dialog_id, user_id),
                        (user_id, dialog_id, partner_id),
                    ),
                )
                await db.commit()
                return ActiveDialog(dialog_id, user_id, partner_id)
            except Exception:
                await db.rollback()
                raise

    async def is_dialog_member(self, user_id: int, dialog_id: int) -> bool:
        async with self._lock:
            cursor = await self._db().execute(
                """
                SELECT 1 FROM dialogs
                WHERE dialog_id = ? AND (user1_id = ? OR user2_id = ?)
                """,
                (dialog_id, user_id, user_id),
            )
            return await cursor.fetchone() is not None

    async def get_or_create_call_room(self, dialog_id: int) -> str:
        async with self._lock:
            db = self._db()
            cursor = await db.execute(
                "SELECT call_room FROM dialogs WHERE dialog_id = ?", (dialog_id,)
            )
            row = await cursor.fetchone()
            if not row:
                raise ValueError("Диалог не найден.")
            if row["call_room"]:
                return str(row["call_room"])
            room = f"EchoDialog-{secrets.token_urlsafe(24)}"
            await db.execute(
                "UPDATE dialogs SET call_room = ? WHERE dialog_id = ?",
                (room, dialog_id),
            )
            await db.commit()
            return room

    async def rate_dialog(self, user_id: int, dialog_id: int, value: int) -> bool:
        if value not in {-1, 1}:
            raise ValueError("Оценка должна быть -1 или 1.")
        async with self._lock:
            db = self._db()
            cursor = await db.execute(
                """
                SELECT 1 FROM dialogs
                WHERE dialog_id = ? AND (user1_id = ? OR user2_id = ?)
                """,
                (dialog_id, user_id, user_id),
            )
            if not await cursor.fetchone():
                return False
            await db.execute(
                """
                INSERT INTO ratings (dialog_id, user_id, value)
                VALUES (?, ?, ?)
                ON CONFLICT(dialog_id, user_id) DO UPDATE SET value = excluded.value
                """,
                (dialog_id, user_id, value),
            )
            await db.commit()
            return True

    async def create_report(
        self, reporter_id: int, dialog_id: int, reason: str
    ) -> tuple[int | None, bool]:
        async with self._lock:
            db = self._db()
            cursor = await db.execute(
                "SELECT user1_id, user2_id FROM dialogs WHERE dialog_id = ?",
                (dialog_id,),
            )
            dialog = await cursor.fetchone()
            if not dialog or reporter_id not in {dialog["user1_id"], dialog["user2_id"]}:
                return None, False
            reported_id = (
                int(dialog["user2_id"])
                if dialog["user1_id"] == reporter_id
                else int(dialog["user1_id"])
            )
            cursor = await db.execute(
                """
                INSERT OR IGNORE INTO reports
                    (dialog_id, reporter_id, reported_id, reason)
                VALUES (?, ?, ?, ?)
                """,
                (dialog_id, reporter_id, reported_id, reason),
            )
            created = cursor.rowcount > 0
            await db.execute(
                """
                INSERT OR IGNORE INTO blocks (user_id, blocked_user_id)
                VALUES (?, ?)
                """,
                (reporter_id, reported_id),
            )
            await db.commit()
            return reported_id, created

    async def mark_unreachable(self, user_id: int) -> None:
        await self._update_user(user_id, "can_receive = 0")

    async def set_banned(self, user_id: int, banned: bool) -> ActiveDialog | None:
        async with self._lock:
            db = self._db()
            await db.execute("BEGIN IMMEDIATE")
            try:
                await db.execute(
                    "UPDATE users SET is_banned = ?, updated_at = CURRENT_TIMESTAMP WHERE user_id = ?",
                    (int(banned), user_id),
                )
                await db.execute("DELETE FROM queue WHERE user_id = ?", (user_id,))
                cursor = await db.execute(
                    "SELECT partner_id, dialog_id FROM active_chats WHERE user_id = ?", (user_id,)
                )
                row = await cursor.fetchone()
                active = None
                if row:
                    partner_id = int(row["partner_id"])
                    dialog_id = int(row["dialog_id"])
                    active = ActiveDialog(dialog_id, user_id, partner_id)
                    await db.execute(
                        "DELETE FROM active_chats WHERE user_id IN (?, ?)",
                        (user_id, partner_id),
                    )
                    await db.execute(
                        "DELETE FROM relayed_messages WHERE dialog_id = ?", (dialog_id,)
                    )
                    await db.execute(
                        """
                        UPDATE dialogs SET ended_at = CURRENT_TIMESTAMP,
                            ended_by = ?, end_reason = 'moderation'
                        WHERE dialog_id = ? AND ended_at IS NULL
                        """,
                        (user_id, dialog_id),
                    )
                await db.commit()
                return active
            except Exception:
                await db.rollback()
                raise

    async def delete_user(self, user_id: int) -> ActiveDialog | None:
        async with self._lock:
            db = self._db()
            await db.execute("BEGIN IMMEDIATE")
            try:
                cursor = await db.execute(
                    "SELECT partner_id, dialog_id FROM active_chats WHERE user_id = ?", (user_id,)
                )
                row = await cursor.fetchone()
                active = None
                if row:
                    active = ActiveDialog(int(row["dialog_id"]), user_id, int(row["partner_id"]))
                    await db.execute(
                        "DELETE FROM active_chats WHERE user_id IN (?, ?)",
                        (user_id, active.partner_id),
                    )
                    await db.execute(
                        """
                        UPDATE dialogs SET ended_at = CURRENT_TIMESTAMP,
                            ended_by = ?, end_reason = 'account_deleted'
                        WHERE dialog_id = ? AND ended_at IS NULL
                        """,
                        (user_id, active.dialog_id),
                    )

                cursor = await db.execute(
                    "SELECT dialog_id FROM dialogs WHERE user1_id = ? OR user2_id = ?",
                    (user_id, user_id),
                )
                dialog_ids = [row["dialog_id"] for row in await cursor.fetchall()]
                if dialog_ids:
                    placeholders = ",".join("?" for _ in dialog_ids)
                    await db.execute(
                        f"DELETE FROM ratings WHERE dialog_id IN ({placeholders})", dialog_ids
                    )
                    await db.execute(
                        f"DELETE FROM reports WHERE dialog_id IN ({placeholders})", dialog_ids
                    )
                    await db.execute(
                        f"DELETE FROM dialogs WHERE dialog_id IN ({placeholders})", dialog_ids
                    )

                await db.execute(
                    "DELETE FROM reports WHERE reporter_id = ? OR reported_id = ?",
                    (user_id, user_id),
                )
                await db.execute(
                    "DELETE FROM blocks WHERE user_id = ? OR blocked_user_id = ?",
                    (user_id, user_id),
                )
                await db.execute(
                    """
                    UPDATE users SET last_partner_id = NULL, last_dialog_id = NULL
                    WHERE last_partner_id = ?
                    """,
                    (user_id,),
                )
                cursor = await db.execute(
                    "SELECT order_id FROM payment_orders WHERE user_id = ?", (user_id,)
                )
                order_ids = [str(row["order_id"]) for row in await cursor.fetchall()]
                if order_ids:
                    placeholders = ",".join("?" for _ in order_ids)
                    await db.execute(
                        f"DELETE FROM processed_payments WHERE order_id IN ({placeholders})",
                        order_ids,
                    )
                await db.execute("DELETE FROM payment_orders WHERE user_id = ?", (user_id,))
                await db.execute("DELETE FROM users WHERE user_id = ?", (user_id,))
                await db.commit()
                return active
            except Exception:
                await db.rollback()
                raise

    async def stats(self) -> dict[str, int]:
        async with self._lock:
            db = self._db()
            result: dict[str, int] = {}
            queries = {
                "users": "SELECT COUNT(*) FROM users",
                "adults": "SELECT COUNT(*) FROM users WHERE is_adult = 1",
                "queue": "SELECT COUNT(*) FROM queue",
                "active_dialogs": "SELECT COUNT(*) / 2 FROM active_chats",
                "dialogs": "SELECT COUNT(*) FROM dialogs",
                "reports": "SELECT COUNT(*) FROM reports",
                "banned": "SELECT COUNT(*) FROM users WHERE is_banned = 1",
                "premium": """
                    SELECT COUNT(*) FROM users
                    WHERE premium_until > CURRENT_TIMESTAMP
                      AND NOT (vip_until > CURRENT_TIMESTAMP)
                """,
                "vip": "SELECT COUNT(*) FROM users WHERE vip_until > CURRENT_TIMESTAMP",
                "payments": "SELECT COUNT(*) FROM payment_orders WHERE credited = 1",
                "revenue_kopeks": """
                    SELECT COALESCE(SUM(amount_kopeks), 0)
                    FROM payment_orders WHERE credited = 1
                """,
            }
            for key, query in queries.items():
                cursor = await db.execute(query)
                row = await cursor.fetchone()
                result[key] = int(row[0])
            return result


def _parse_db_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _db_datetime(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _extended_expiry(current: Any, duration_days: int) -> datetime:
    now = datetime.now(timezone.utc)
    current_datetime = _parse_db_datetime(current)
    starts_at = current_datetime if current_datetime and current_datetime > now else now
    return starts_at + timedelta(days=duration_days)


def _latest_expiry(first: Any, second: Any) -> datetime | None:
    values = [
        value
        for value in (_parse_db_datetime(first), _parse_db_datetime(second))
        if value
    ]
    return max(values) if values else None


def _membership_from_row(row: Any) -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    premium_until = _parse_db_datetime(row["premium_until"]) if row else None
    vip_until = _parse_db_datetime(row["vip_until"]) if row else None
    is_vip = bool(vip_until and vip_until > now)
    premium_active = bool(premium_until and premium_until > now)
    has_premium = is_vip or premium_active
    if is_vip:
        tier = "vip"
        expires_at = vip_until
    elif premium_active:
        tier = "premium"
        expires_at = premium_until
    else:
        tier = "free"
        expires_at = None
    return {
        "tier": tier,
        "has_premium": has_premium,
        "is_vip": is_vip,
        "expires_at": expires_at,
        "premium_until": premium_until,
        "vip_until": vip_until,
    }


def _moscow_day_start_utc() -> datetime:
    moscow = timezone(timedelta(hours=3))
    now = datetime.now(moscow)
    return now.replace(hour=0, minute=0, second=0, microsecond=0).astimezone(
        timezone.utc
    )


def _display_moscow_datetime(value: datetime) -> str:
    moscow = timezone(timedelta(hours=3))
    return value.astimezone(moscow).strftime("%d.%m.%Y %H:%M МСК")
