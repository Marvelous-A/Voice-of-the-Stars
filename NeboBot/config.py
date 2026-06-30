from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


BASE_DIR = Path(__file__).resolve().parent


def _positive_int(name: str, default: int, minimum: int = 1) -> int:
    raw = os.getenv(name, "").strip()
    try:
        value = int(raw) if raw else default
    except ValueError:
        value = default
    return max(minimum, value)


@dataclass(frozen=True, slots=True)
class Settings:
    bot_token: str
    admin_bot_token: str
    admin_id: int
    database_path: Path
    source_base_url: str
    poll_interval_seconds: int
    request_timeout_seconds: int
    regions_cache_ttl_seconds: int
    stats_cache_ttl_seconds: int
    max_history_pages: int
    telegram_proxy_url: str
    log_level: str


def load_settings() -> Settings:
    load_dotenv(BASE_DIR / ".env")

    bot_token = os.getenv("BOT_TOKEN", "").strip()
    if not bot_token:
        raise RuntimeError(
            "BOT_TOKEN не задан. Скопируйте .env.example в .env и добавьте токен от @BotFather."
        )

    database_path = Path(
        os.getenv("DATABASE_PATH", "data/bpla_region_bot.sqlite3").strip()
    )
    if not database_path.is_absolute():
        database_path = BASE_DIR / database_path

    return Settings(
        bot_token=bot_token,
        admin_bot_token=os.getenv("ADMIN_BOT_TOKEN", "").strip(),
        admin_id=_positive_int("ADMIN_ID", 0, minimum=0),
        database_path=database_path,
        source_base_url=(
            os.getenv("SOURCE_BASE_URL", "https://bplarussia.ru").strip().rstrip("/")
        ),
        poll_interval_seconds=_positive_int(
            "POLL_INTERVAL_SECONDS", 60, minimum=30
        ),
        request_timeout_seconds=_positive_int(
            "REQUEST_TIMEOUT_SECONDS", 20, minimum=5
        ),
        regions_cache_ttl_seconds=_positive_int(
            "REGIONS_CACHE_TTL_SECONDS", 600, minimum=60
        ),
        stats_cache_ttl_seconds=_positive_int(
            "STATS_CACHE_TTL_SECONDS", 30, minimum=10
        ),
        max_history_pages=_positive_int("MAX_HISTORY_PAGES", 50, minimum=1),
        telegram_proxy_url=os.getenv("TELEGRAM_PROXY_URL", "").strip(),
        log_level=os.getenv("LOG_LEVEL", "INFO").strip().upper() or "INFO",
    )
