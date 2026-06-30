from __future__ import annotations

import asyncio
import logging
from contextlib import suppress

from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramAPIError
from aiogram.types import BotCommand

from config import load_settings
from database import Database
from handlers import build_router
from notifier import EventMonitor
from source import BplaRussiaClient


logger = logging.getLogger(__name__)


async def configure_bot(bot: Bot) -> None:
    commands = [
        BotCommand(command="start", description="Открыть бота"),
        BotCommand(command="region", description="Выбрать регион"),
        BotCommand(command="stats", description="Что происходит сейчас"),
        BotCommand(command="overview", description="Обстановка по России"),
        BotCommand(command="alerts", description="Настроить уведомления"),
        BotCommand(command="help", description="Как работает бот"),
        BotCommand(command="delete_me", description="Удалить мои данные"),
    ]
    try:
        await bot.set_my_commands(commands)
    except TelegramAPIError:
        logger.warning("Не удалось установить команды бота", exc_info=True)


async def run() -> None:
    settings = load_settings()
    logging.basicConfig(
        level=getattr(logging, settings.log_level, logging.INFO),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

    db = Database(settings.database_path)
    await db.connect()
    source = BplaRussiaClient(
        settings.source_base_url,
        timeout_seconds=settings.request_timeout_seconds,
        regions_cache_ttl_seconds=settings.regions_cache_ttl_seconds,
        stats_cache_ttl_seconds=settings.stats_cache_ttl_seconds,
        max_history_pages=settings.max_history_pages,
    )
    telegram_session = (
        AiohttpSession(proxy=settings.telegram_proxy_url)
        if settings.telegram_proxy_url
        else AiohttpSession()
    )
    bot = Bot(
        token=settings.bot_token,
        session=telegram_session,
        default=DefaultBotProperties(
            parse_mode=ParseMode.HTML,
            link_preview_is_disabled=True,
        ),
    )
    admin_session = None
    admin_bot = None
    if settings.admin_bot_token and settings.admin_id:
        admin_session = (
            AiohttpSession(proxy=settings.telegram_proxy_url)
            if settings.telegram_proxy_url
            else AiohttpSession()
        )
        admin_bot = Bot(
            token=settings.admin_bot_token,
            session=admin_session,
            default=DefaultBotProperties(parse_mode=ParseMode.HTML),
        )
    else:
        logger.warning(
            "ADMIN_BOT_TOKEN или ADMIN_ID не заданы: уведомления о новых пользователях выключены"
        )
    dispatcher = Dispatcher()
    dispatcher.include_router(
        build_router(db, source, admin_bot=admin_bot, admin_id=settings.admin_id)
    )
    monitor = EventMonitor(
        bot, db, source, poll_interval_seconds=settings.poll_interval_seconds
    )
    monitor_task: asyncio.Task[None] | None = None

    try:
        await bot.delete_webhook(drop_pending_updates=False)
        await configure_bot(bot)
        info = await bot.get_me()
        logger.info("Запущен @%s (%s)", info.username, info.id)
        monitor_task = asyncio.create_task(monitor.run(), name="bpla-event-monitor")
        await dispatcher.start_polling(
            bot,
            allowed_updates=dispatcher.resolve_used_update_types(),
        )
    finally:
        if monitor_task:
            monitor_task.cancel()
            with suppress(asyncio.CancelledError):
                await monitor_task
        await source.close()
        await db.close()
        await bot.session.close()
        if admin_bot is not None:
            await admin_bot.session.close()


if __name__ == "__main__":
    try:
        asyncio.run(run())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Бот остановлен")
