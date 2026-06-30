"""Explicit links from AdminBot to the managed bot projects."""

from __future__ import annotations

import importlib.util
import os
import sys
from contextlib import contextmanager
from pathlib import Path
from types import ModuleType

from dotenv import load_dotenv


BASE_DIR = Path(__file__).resolve().parent
WORKSPACE_DIR = BASE_DIR.parent

# AdminBot may have its own environment file. Existing Voice installations remain
# compatible because their .env is loaded as a fallback below.
load_dotenv(BASE_DIR / ".env")


def _directory_from_env(name: str, default: Path) -> Path:
    raw_value = os.getenv(name, "").strip()
    return Path(raw_value).expanduser().resolve() if raw_value else default.resolve()


def _default_voice_app_dir() -> Path:
    if (WORKSPACE_DIR / "main.py").is_file():
        return WORKSPACE_DIR
    return WORKSPACE_DIR / "Voice of the Stars"


VOICE_APP_DIR = _directory_from_env("VOICE_APP_DIR", _default_voice_app_dir())
VOICE_DATA_DIR = _directory_from_env("VOICE_DATA_DIR", VOICE_APP_DIR)

load_dotenv(VOICE_APP_DIR / ".env")
if VOICE_DATA_DIR != VOICE_APP_DIR:
    load_dotenv(VOICE_DATA_DIR / ".env")

ECHO_DATABASE_PATH = Path(
    os.getenv(
        "ECHO_DATABASE_PATH",
        str(WORKSPACE_DIR / "ЭХО — разговор без имён" / "echo.db"),
    )
).expanduser().resolve()

NEBO_DATABASE_PATH = Path(
    os.getenv(
        "NEBO_DATABASE_PATH",
        str(WORKSPACE_DIR / "BPLA Region Bot" / "data" / "bpla_region_bot.sqlite3"),
    )
).expanduser().resolve()

# Shared Voice modules remain single-source: AdminBot imports their implementation
# from the Voice project instead of keeping payment and promo-code copies.
voice_app_path = str(VOICE_APP_DIR)
if voice_app_path not in sys.path:
    sys.path.insert(0, voice_app_path)


@contextmanager
def voice_working_directory():
    """Run legacy Voice operations against their own runtime files."""
    previous = Path.cwd()
    os.chdir(VOICE_DATA_DIR)
    try:
        yield
    finally:
        os.chdir(previous)


_voice_main: ModuleType | None = None


def load_voice_main() -> ModuleType:
    """Load the Voice application lazily for channel-publication commands."""
    global _voice_main
    if _voice_main is not None:
        return _voice_main

    main_path = VOICE_APP_DIR / "main.py"
    if not main_path.is_file():
        raise RuntimeError(f"Не найден основной модуль Voice: {main_path}")

    spec = importlib.util.spec_from_file_location("adminbot_voice_main", main_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Не удалось загрузить основной модуль Voice: {main_path}")

    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    try:
        with voice_working_directory():
            spec.loader.exec_module(module)
    except Exception:
        sys.modules.pop(spec.name, None)
        raise

    _voice_main = module
    return module
