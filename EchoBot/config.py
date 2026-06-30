from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


BASE_DIR = Path(__file__).resolve().parent


@dataclass(frozen=True, slots=True)
class Settings:
    bot_token: str
    database_path: Path
    call_base_url: str
    admin_ids: frozenset[int]
    log_level: str
    admin_bot_token: str = ""
    admin_usernames: frozenset[str] = frozenset()
    free_daily_dialog_limit: int = 15
    ckassa_poll_interval_sec: int = 30
    ad_text: str = ""
    ad_url: str = ""
    ad_button_text: str = "Узнать больше"
    ad_dialog_interval: int = 3


def _positive_int_env(name: str, default: int, minimum: int = 1) -> int:
    raw = os.getenv(name, "").strip()
    try:
        value = int(raw) if raw else default
    except ValueError:
        value = default
    return max(minimum, value)


def load_settings() -> Settings:
    load_dotenv(BASE_DIR / ".env")

    token = os.getenv("BOT_TOKEN", "").strip()
    if not token:
        raise RuntimeError("BOT_TOKEN не задан. Скопируйте .env.example в .env и добавьте токен.")

    raw_admin_ids = os.getenv(
        "ADMIN_IDS",
        os.getenv("DEPLOY_NOTIFY_CHAT_IDS", ""),
    )
    try:
        admin_ids = frozenset(
            int(value.strip()) for value in raw_admin_ids.split(",") if value.strip()
        )
    except ValueError as error:
        raise RuntimeError("ADMIN_IDS должен содержать Telegram ID через запятую.") from error

    raw_database_path = os.getenv("DATABASE_PATH", "echo.db").strip()
    database_path = Path(raw_database_path)
    if not database_path.is_absolute():
        database_path = BASE_DIR / database_path

    raw_admin_usernames = os.getenv(
        "ADMIN_USERNAMES",
        os.getenv("DEPLOY_NOTIFY_USERNAMES", "bimbim2bambam"),
    )
    admin_usernames = frozenset(
        value.strip().lower().removeprefix("@")
        for value in raw_admin_usernames.split(",")
        if value.strip()
    )

    return Settings(
        bot_token=token,
        database_path=database_path,
        call_base_url=os.getenv("CALL_BASE_URL", "https://meet.jit.si").strip().rstrip("/"),
        admin_ids=admin_ids,
        log_level=os.getenv("LOG_LEVEL", "INFO").upper(),
        admin_bot_token=os.getenv("ADMIN_BOT_TOKEN", "").strip(),
        admin_usernames=admin_usernames,
        free_daily_dialog_limit=_positive_int_env("FREE_DAILY_DIALOG_LIMIT", 15),
        ckassa_poll_interval_sec=_positive_int_env(
            "CKASSA_POLL_INTERVAL_SEC", 30, minimum=15
        ),
        ad_text=os.getenv("ECHO_AD_TEXT", "").strip(),
        ad_url=os.getenv("ECHO_AD_URL", "").strip(),
        ad_button_text=(
            os.getenv("ECHO_AD_BUTTON_TEXT", "Узнать больше").strip()
            or "Узнать больше"
        ),
        ad_dialog_interval=_positive_int_env("ECHO_AD_DIALOG_INTERVAL", 3),
    )
