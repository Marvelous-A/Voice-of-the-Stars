from __future__ import annotations

import asyncio
import logging
from contextlib import suppress

from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramAPIError
from aiogram.types import BotCommand

from config import load_settings
from database import Database
from handlers import build_router
from payments import SubscriptionPayments
from reminders import run_reminder_scheduler


logger = logging.getLogger(__name__)


async def configure_bot(bot: Bot) -> None:
    commands = [
        BotCommand(command="start", description="Открыть ЭХО"),
        BotCommand(command="search", description="Подать сигнал"),
        BotCommand(command="next", description="Найти другого собеседника"),
        BotCommand(command="stop", description="Завершить связь или поиск"),
        BotCommand(command="interests", description="Выбрать интересы"),
        BotCommand(command="settings", description="Настройки"),
        BotCommand(command="link", description="Открыть профиль собеседнику"),
        BotCommand(command="call", description="Анонимный аудио- или видеозвонок"),
        BotCommand(command="premium", description="Premium и VIP"),
        BotCommand(command="rules", description="Правила и приватность"),
        BotCommand(command="help", description="Помощь"),
        BotCommand(command="delete_me", description="Удалить мои данные"),
    ]
    try:
        await bot.set_my_name(name="ЭХО | Анонимный чат")
        await bot.set_my_short_description(
            short_description="Анонимные разговоры один на один. Отправь мысль — получи отклик."
        )
        await bot.set_my_description(
            description=(
                "ЭХО соединяет двух незнакомцев для анонимного разговора. "
                "Профили скрыты, сообщения не сохраняются. Выбирай интересы, "
                "подавай сигнал и находи человека, который тоже хочет поговорить. Только 18+."
            )
        )
        await bot.set_my_commands(commands)
    except TelegramAPIError:
        logger.warning("Не удалось обновить имя, описание или команды бота", exc_info=True)


async def run() -> None:
    settings = load_settings()
    logging.basicConfig(
        level=getattr(logging, settings.log_level, logging.INFO),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

    database = Database(settings.database_path)
    await database.connect()
    payments = SubscriptionPayments(database, settings)
    bot_defaults = DefaultBotProperties(parse_mode=ParseMode.HTML)
    bot = Bot(token=settings.bot_token, default=bot_defaults)
    admin_bot = (
        Bot(token=settings.admin_bot_token, default=bot_defaults)
        if settings.admin_bot_token
        else None
    )
    dispatcher = Dispatcher()
    dispatcher.include_router(
        build_router(database, settings, payments, admin_bot=admin_bot)
    )
    payment_watcher: asyncio.Task[None] | None = None
    reminder_scheduler: asyncio.Task[None] | None = None

    try:
        await bot.delete_webhook(drop_pending_updates=False)
        await configure_bot(bot)
        info = await bot.get_me()
        logger.info("Запущен @%s (%s)", info.username, info.id)
        payment_watcher = asyncio.create_task(
            payments.watch(bot), name="ckassa-payment-watcher"
        )
        reminder_scheduler = asyncio.create_task(
            run_reminder_scheduler(bot, database), name="engagement-reminder-scheduler"
        )
        await dispatcher.start_polling(
            bot,
            allowed_updates=dispatcher.resolve_used_update_types(),
        )
    finally:
        for task in (payment_watcher, reminder_scheduler):
            if task:
                task.cancel()
                with suppress(asyncio.CancelledError):
                    await task
        await database.close()
        await bot.session.close()
        if admin_bot:
            await admin_bot.session.close()


if __name__ == "__main__":
    try:
        asyncio.run(run())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Бот остановлен")
