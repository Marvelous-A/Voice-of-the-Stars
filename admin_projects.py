"""Project-specific data adapters for the shared administration bot."""

from __future__ import annotations

import html
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any


class EchoStatsError(RuntimeError):
    """Raised when the ECHO statistics database cannot be read."""


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
