from __future__ import annotations

import asyncio
import logging
import secrets
from collections.abc import Callable
from datetime import datetime, time, timedelta, timezone

from aiogram import Bot
from aiogram.exceptions import (
    TelegramAPIError,
    TelegramBadRequest,
    TelegramForbiddenError,
    TelegramRetryAfter,
)

from database import Database


logger = logging.getLogger(__name__)

MOSCOW_TIMEZONE = timezone(timedelta(hours=3))
REMINDER_JOB_NAME = "engagement-reminder"
REMINDER_INTERVAL = timedelta(days=3)
REMINDER_LOCAL_TIME = time(hour=21)
REMINDER_SEND_DELAY_SECONDS = 0.05

FEMALE_NAMES = (
    "Алина",
    "Анна",
    "Виктория",
    "Дарья",
    "Екатерина",
    "Ксения",
    "Мария",
    "Полина",
    "София",
    "Юлия",
)
MALE_NAMES = (
    "Александр",
    "Алексей",
    "Артём",
    "Даниил",
    "Иван",
    "Максим",
    "Михаил",
    "Никита",
    "Роман",
    "Сергей",
)

REMINDER_TEXT = (
    "<b>Тебя уже ждут в «ЭХО».</b>\n\n"
    "Кто-то хочет поговорить именно с тобой — без имён, анкет и лишнего шума.\n\n"
    "📡 Подай сигнал. Твой разговор уже рядом."
)


def reminder_text(gender: str | None) -> str:
    """Build an assertive reminder with a name of the opposite gender."""
    if gender == "male":
        name = secrets.choice(FEMALE_NAMES)
        pronoun = "Она"
    elif gender == "female":
        name = secrets.choice(MALE_NAMES)
        pronoun = "Он"
    else:
        return REMINDER_TEXT

    return (
        f"<b>{name} ждёт тебя в «ЭХО».</b>\n\n"
        f"{pronoun} хочет поговорить именно с тобой — без имён, анкет и лишнего шума.\n\n"
        f"📡 Подай сигнал. {name} уже ждёт."
    )


def first_reminder_at(now: datetime) -> datetime:
    """Return the next 21:00 Moscow occurrence as an aware UTC datetime."""
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    local_now = now.astimezone(MOSCOW_TIMEZONE)
    candidate = datetime.combine(
        local_now.date(), REMINDER_LOCAL_TIME, tzinfo=MOSCOW_TIMEZONE
    )
    if candidate <= local_now:
        candidate += timedelta(days=1)
    return candidate.astimezone(timezone.utc)


def following_reminder_at(scheduled_at: datetime, now: datetime) -> datetime:
    """Keep the three-day cadence while skipping missed historical runs."""
    if scheduled_at.tzinfo is None:
        scheduled_at = scheduled_at.replace(tzinfo=timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    candidate = scheduled_at.astimezone(timezone.utc) + REMINDER_INTERVAL
    current = now.astimezone(timezone.utc)
    while candidate <= current:
        candidate += REMINDER_INTERVAL
    return candidate


async def _deliver_reminder(
    bot: Bot, database: Database, user_id: int, gender: str | None
) -> bool:
    for attempt in range(2):
        try:
            # Не меняем reply-клавиатуру: пользователь может уже быть в диалоге.
            await bot.send_message(user_id, reminder_text(gender))
            return True
        except TelegramRetryAfter as error:
            if attempt == 0:
                await asyncio.sleep(float(error.retry_after) + 0.1)
                continue
            logger.warning(
                "Повторная отправка напоминания пользователю %s не удалась: %s",
                user_id,
                error,
            )
        except (TelegramForbiddenError, TelegramBadRequest) as error:
            await database.mark_unreachable(user_id)
            logger.info("Напоминание недоступно пользователю %s: %s", user_id, error)
        except TelegramAPIError as error:
            logger.warning(
                "Не удалось отправить напоминание пользователю %s: %s",
                user_id,
                error,
            )
        return False
    return False


async def broadcast_reminder(bot: Bot, database: Database) -> tuple[int, int]:
    recipients = await database.notification_recipients()
    delivered = 0
    for user_id, gender in recipients:
        if await _deliver_reminder(bot, database, user_id, gender):
            delivered += 1
        await asyncio.sleep(REMINDER_SEND_DELAY_SECONDS)
    return delivered, len(recipients)


async def run_reminder_scheduler(
    bot: Bot,
    database: Database,
    *,
    now: Callable[[], datetime] | None = None,
) -> None:
    now = now or (lambda: datetime.now(timezone.utc))
    scheduled_at = await database.get_or_create_scheduled_job(
        REMINDER_JOB_NAME, first_reminder_at(now())
    )

    while True:
        delay = (scheduled_at - now().astimezone(timezone.utc)).total_seconds()
        if delay > 0:
            await asyncio.sleep(delay)

        current = now().astimezone(timezone.utc)
        next_run_at = following_reminder_at(scheduled_at, current)
        claimed = await database.advance_scheduled_job(
            REMINDER_JOB_NAME, scheduled_at, next_run_at
        )
        if not claimed:
            scheduled_at = await database.get_or_create_scheduled_job(
                REMINDER_JOB_NAME, first_reminder_at(current)
            )
            continue

        delivered, total = await broadcast_reminder(bot, database)
        logger.info(
            "Рассылка «ЭХО» завершена: доставлено %s из %s; следующая — %s",
            delivered,
            total,
            next_run_at.astimezone(MOSCOW_TIMEZONE).strftime("%d.%m.%Y %H:%M МСК"),
        )
        scheduled_at = next_run_at
