from __future__ import annotations

import asyncio
import html
import logging
from datetime import UTC, datetime, timedelta, timezone

from aiogram import Bot
from aiogram.exceptions import (
    TelegramAPIError,
    TelegramForbiddenError,
    TelegramRetryAfter,
)

from database import Database
from keyboards import event_keyboard
from models import Event, Region, event_matches_region, is_clear_event, is_important_event
from source import BplaRussiaClient


logger = logging.getLogger(__name__)
MSK = timezone(timedelta(hours=3))


def render_event_notification(event: Event) -> str:
    searchable = f"{event.incident_type} {event.title}".casefold()
    if "отбой" in searchable:
        icon = "🟢"
    elif any(marker in searchable for marker in ("опасност", "тревог", "угроз")):
        icon = "🚨"
    else:
        icon = "🟡"

    region_names = ", ".join(event.region_names) or "Регион не указан"
    published = event.published_at.astimezone(MSK).strftime("%d.%m.%Y %H:%M МСК")
    lines = [
        f"{icon} <b>Новое сообщение</b>",
        f"📍 {html.escape(region_names)}",
        "",
        f"<b>{html.escape(event.title)}</b>",
    ]
    if event.description:
        lines.extend(["", html.escape(event.description)])
    details = []
    if event.incident_type:
        details.append(f"Тип: {html.escape(event.incident_type)}")
    if event.threat_level:
        details.append(f"Уровень: {html.escape(event.threat_level)}")
    details.append(f"Время: {published}")
    lines.extend(["", *details, "", "<i>Источник: bplarussia.ru</i>"])
    return "\n".join(lines)


class EventMonitor:
    def __init__(
        self,
        bot: Bot,
        db: Database,
        source: BplaRussiaClient,
        poll_interval_seconds: int,
    ) -> None:
        self.bot = bot
        self.db = db
        self.source = source
        self.poll_interval_seconds = poll_interval_seconds

    async def _deliver_pending(self) -> None:
        for delivery in await self.db.pending_deliveries(limit=100):
            event: Event = delivery["event"]
            chat_id = int(delivery["telegram_id"])
            try:
                await self.bot.send_message(
                    chat_id,
                    render_event_notification(event),
                    reply_markup=event_keyboard(event.url),
                )
            except TelegramForbiddenError as error:
                await self.db.set_notifications(chat_id, False)
                await self.db.mark_delivery_failed(
                    event.id, chat_id, str(error), permanent=True
                )
            except TelegramRetryAfter as error:
                await self.db.mark_delivery_failed(event.id, chat_id, str(error))
                await asyncio.sleep(min(float(error.retry_after), 30.0))
            except TelegramAPIError as error:
                logger.warning("Не доставлено событие %s пользователю %s: %s", event.id, chat_id, error)
                await self.db.mark_delivery_failed(event.id, chat_id, str(error))
            except Exception as error:
                logger.exception("Ошибка доставки события %s пользователю %s", event.id, chat_id)
                await self.db.mark_delivery_failed(event.id, chat_id, str(error))
            else:
                await self.db.mark_delivery_sent(event.id, chat_id)

    async def _discover(self) -> None:
        initialized = await self.db.get_metadata("source_seeded") == "1"
        latest = await self.db.latest_event_datetime()

        if not initialized:
            events = await self.source.get_latest_events(limit=100)
            for event in events:
                await self.db.add_event(event, seeded=True)
            await self.db.set_metadata("source_seeded", "1")
            logger.info("Начальная лента сохранена: %s событий без рассылки", len(events))
            return

        after = (latest - timedelta(minutes=5)) if latest else (datetime.now(UTC) - timedelta(minutes=5))
        events = await self.source.get_events_since(after)
        subscribers = await self.db.list_subscribers()

        for event in sorted(events, key=lambda item: (item.published_at, item.id)):
            if not await self.db.add_event(event):
                continue
            for subscriber in subscribers:
                region = Region(
                    id=int(subscriber["region_id"]),
                    name=str(subscriber["region_name"]),
                    slug=str(subscriber["region_slug"]),
                    incidents_total=0,
                    url="",
                )
                mode = str(subscriber.get("notification_mode") or "all")
                scope = str(subscriber.get("notification_scope") or "region")
                allowed_by_mode = (
                    mode == "all"
                    or is_important_event(event)
                    or is_clear_event(event)
                )
                allowed_by_scope = scope == "all" or event_matches_region(event, region)
                if allowed_by_mode and allowed_by_scope:
                    await self.db.queue_delivery(event.id, int(subscriber["telegram_id"]))

    async def run_once(self) -> None:
        await self._deliver_pending()
        await self._discover()
        await self._deliver_pending()

    async def run(self) -> None:
        while True:
            try:
                await self.run_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Ошибка фонового мониторинга")
            await asyncio.sleep(self.poll_interval_seconds)
