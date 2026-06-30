"""Project-specific data adapters for the shared administration bot."""

from __future__ import annotations

import html
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any


class EchoStatsError(RuntimeError):
    """Raised when the ECHO statistics database cannot be read."""


class NeboStatsError(RuntimeError):
    """Raised when the Nebo Alert statistics database cannot be read."""


def load_echo_stats(database_path: str | Path) -> dict[str, Any]:
    """Read an ECHO statistics snapshot without modifying its SQLite database."""
    path = Path(database_path).expanduser()
    if not path.is_file():
        raise EchoStatsError(f"База данных не найдена: {path}")

    try:
        connection = sqlite3.connect(
            f"file:{path.resolve().as_posix()}?mode=ro",
            uri=True,
            timeout=5,
        )
        connection.execute("PRAGMA query_only=ON")
        queries = {
            "users": "SELECT COUNT(*) FROM users",
            "adults": "SELECT COUNT(*) FROM users WHERE is_adult = 1",
            "queue": "SELECT COUNT(*) FROM queue",
            "active_dialogs": "SELECT COUNT(*) / 2 FROM active_chats",
            "dialogs": "SELECT COUNT(*) FROM dialogs",
            "reports": "SELECT COUNT(*) FROM reports",
            "banned": "SELECT COUNT(*) FROM users WHERE is_banned = 1",
        }
        stats = {
            key: int(connection.execute(query).fetchone()[0])
            for key, query in queries.items()
        }
        has_start_sources = connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'start_sources'"
        ).fetchone()
        stats["sources"] = (
            [
                (str(source), int(users))
                for source, users in connection.execute(
                    """
                    SELECT source, COUNT(*) AS users
                    FROM start_sources
                    GROUP BY source
                    ORDER BY users DESC, source ASC
                    LIMIT 10
                    """
                ).fetchall()
            ]
            if has_start_sources
            else []
        )
        return stats
    except sqlite3.Error as error:
        raise EchoStatsError(f"Не удалось прочитать базу данных: {error}") from error
    finally:
        if "connection" in locals():
            connection.close()


def render_echo_stats(stats: dict[str, Any], *, now: datetime | None = None) -> str:
    """Render an ECHO statistics snapshot as Telegram-safe HTML."""
    current_time = now or datetime.now()
    sources = stats.get("sources") or []
    source_lines = (
        "\n\n<b>📣 Рекламные источники</b>\n"
        + "\n".join(
            f"  <code>{html.escape(str(source))}</code>: <b>{int(users)}</b>"
            for source, users in sources
        )
        if sources
        else ""
    )
    return (
        "📊 <b>Статистика бота «ЭХО»</b>\n"
        f"<i>{current_time.strftime('%d.%m.%Y %H:%M')}</i>\n\n"
        "<b>👥 Пользователи</b>\n"
        f"  Всего: <b>{int(stats['users'])}</b>\n"
        f"  Подтвердили 18+: <b>{int(stats['adults'])}</b>\n"
        f"  Заблокированы: <b>{int(stats['banned'])}</b>\n\n"
        "<b>💬 Общение</b>\n"
        f"  Сейчас в очереди: <b>{int(stats['queue'])}</b>\n"
        f"  Активные связи: <b>{int(stats['active_dialogs'])}</b>\n"
        f"  Всего диалогов: <b>{int(stats['dialogs'])}</b>\n"
        f"  Жалобы: <b>{int(stats['reports'])}</b>"
        + source_lines
    )


def load_nebo_stats(database_path: str | Path) -> dict[str, Any]:
    """Read a Nebo Alert statistics snapshot without modifying its database."""
    path = Path(database_path).expanduser()
    if not path.is_file():
        raise NeboStatsError(f"База данных не найдена: {path}")

    try:
        connection = sqlite3.connect(
            f"file:{path.resolve().as_posix()}?mode=ro",
            uri=True,
            timeout=5,
        )
        connection.execute("PRAGMA query_only=ON")
        connection.row_factory = sqlite3.Row

        tables = {
            str(row[0])
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        if "users" not in tables:
            raise NeboStatsError("В базе нет таблицы пользователей.")

        user_columns = {
            str(row[1]) for row in connection.execute("PRAGMA table_info(users)")
        }

        def count(query: str, parameters: tuple[Any, ...] = ()) -> int:
            return int(connection.execute(query, parameters).fetchone()[0])

        stats: dict[str, Any] = {
            "users": count("SELECT COUNT(*) FROM users"),
            "users_24h": (
                count(
                    "SELECT COUNT(*) FROM users "
                    "WHERE created_at >= datetime('now', '-1 day')"
                )
                if "created_at" in user_columns
                else 0
            ),
            "notifications_enabled": (
                count("SELECT COUNT(*) FROM users WHERE notifications_enabled=1")
                if "notifications_enabled" in user_columns
                else 0
            ),
            "regions_selected": (
                count("SELECT COUNT(*) FROM users WHERE region_id IS NOT NULL")
                if "region_id" in user_columns
                else 0
            ),
            "scope_all": (
                count(
                    "SELECT COUNT(*) FROM users "
                    "WHERE notifications_enabled=1 AND notification_scope='all'"
                )
                if {"notifications_enabled", "notification_scope"} <= user_columns
                else 0
            ),
            "scope_region": (
                count(
                    "SELECT COUNT(*) FROM users "
                    "WHERE notifications_enabled=1 AND notification_scope='region'"
                )
                if {"notifications_enabled", "notification_scope"} <= user_columns
                else 0
            ),
            "mode_all": (
                count(
                    "SELECT COUNT(*) FROM users "
                    "WHERE notifications_enabled=1 AND notification_mode='all'"
                )
                if {"notifications_enabled", "notification_mode"} <= user_columns
                else 0
            ),
            "mode_important": (
                count(
                    "SELECT COUNT(*) FROM users "
                    "WHERE notifications_enabled=1 AND notification_mode='important'"
                )
                if {"notifications_enabled", "notification_mode"} <= user_columns
                else 0
            ),
            "events": count("SELECT COUNT(*) FROM events") if "events" in tables else 0,
            "new_events": (
                count("SELECT COUNT(*) FROM events WHERE seeded=0")
                if "events" in tables
                else 0
            ),
            "delivered": (
                count("SELECT COUNT(*) FROM deliveries WHERE status='delivered'")
                if "deliveries" in tables
                else 0
            ),
            "pending": (
                count("SELECT COUNT(*) FROM deliveries WHERE status='pending'")
                if "deliveries" in tables
                else 0
            ),
            "failed": (
                count("SELECT COUNT(*) FROM deliveries WHERE status='failed'")
                if "deliveries" in tables
                else 0
            ),
        }

        stats["top_regions"] = (
            [
                (str(row[0]), int(row[1]))
                for row in connection.execute(
                    """
                    SELECT region_name, COUNT(*) AS users
                    FROM users
                    WHERE region_name <> ''
                    GROUP BY region_name
                    ORDER BY users DESC, region_name ASC
                    LIMIT 10
                    """
                ).fetchall()
            ]
            if "region_name" in user_columns
            else []
        )

        latest_columns = {"telegram_id", "username", "full_name", "created_at"}
        latest = (
            connection.execute(
                """
                SELECT telegram_id, username, full_name,
                       datetime(created_at, '+3 hours') AS created_at_msk
                FROM users
                ORDER BY created_at DESC, telegram_id DESC
                LIMIT 1
                """
            ).fetchone()
            if latest_columns <= user_columns
            else None
        )
        stats["latest_user"] = dict(latest) if latest else None
        return stats
    except NeboStatsError:
        raise
    except sqlite3.Error as error:
        raise NeboStatsError(f"Не удалось прочитать базу данных: {error}") from error
    finally:
        if "connection" in locals():
            connection.close()


def render_nebo_stats(stats: dict[str, Any], *, now: datetime | None = None) -> str:
    """Render a Nebo Alert statistics snapshot as Telegram-safe HTML."""
    current_time = now or datetime.now()
    top_regions = stats.get("top_regions") or []
    region_lines = (
        "\n\n<b>📍 Популярные регионы</b>\n"
        + "\n".join(
            f"  {html.escape(str(region))}: <b>{int(users)}</b>"
            for region, users in top_regions
        )
        if top_regions
        else ""
    )

    latest = stats.get("latest_user") or {}
    username = str(latest.get("username") or "").lstrip("@")
    display_name = str(latest.get("full_name") or "Без имени")
    latest_line = ""
    if latest:
        username_line = f" (@{html.escape(username)})" if username else ""
        latest_line = (
            "\n\n<b>🆕 Последний пользователь</b>\n"
            f"  {html.escape(display_name)}{username_line}\n"
            f"  ID: <code>{int(latest['telegram_id'])}</code> · "
            f"{html.escape(str(latest.get('created_at_msk') or ''))} МСК"
        )

    return (
        "📊 <b>Статистика бота «Небо рядом»</b>\n"
        f"<i>{current_time.strftime('%d.%m.%Y %H:%M')}</i>\n\n"
        "<b>👥 Пользователи</b>\n"
        f"  Всего: <b>{int(stats['users'])}</b>\n"
        f"  Новых за 24 часа: <b>{int(stats['users_24h'])}</b>\n"
        f"  Выбрали регион: <b>{int(stats['regions_selected'])}</b>\n"
        f"  Уведомления включены: <b>{int(stats['notifications_enabled'])}</b>\n\n"
        "<b>🔔 Подписки</b>\n"
        f"  Вся Россия: <b>{int(stats['scope_all'])}</b>\n"
        f"  Только свой регион: <b>{int(stats['scope_region'])}</b>\n"
        f"  Все события: <b>{int(stats['mode_all'])}</b>\n"
        f"  Тревоги и отбои: <b>{int(stats['mode_important'])}</b>\n\n"
        "<b>📡 Работа рассылки</b>\n"
        f"  Событий в базе: <b>{int(stats['events'])}</b>\n"
        f"  Новых событий после запуска: <b>{int(stats['new_events'])}</b>\n"
        f"  Доставлено уведомлений: <b>{int(stats['delivered'])}</b>\n"
        f"  Ожидают отправки: <b>{int(stats['pending'])}</b>\n"
        f"  Ошибок доставки: <b>{int(stats['failed'])}</b>"
        + region_lines
        + latest_line
    )
