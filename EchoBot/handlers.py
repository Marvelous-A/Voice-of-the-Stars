from __future__ import annotations

import html
import logging
import re
from collections.abc import Awaitable, Callable
from datetime import timedelta, timezone
from typing import Any

from aiogram import BaseMiddleware, Bot, F, Router
from aiogram.enums import ChatType
from aiogram.exceptions import TelegramAPIError, TelegramBadRequest, TelegramForbiddenError
from aiogram.filters import Command, CommandObject, CommandStart
from aiogram.types import (
    CallbackQuery,
    Message,
    ReplyKeyboardRemove,
    ReplyParameters,
    TelegramObject,
    User,
)

from config import Settings
from ckassa_payments import (
    CkassaPaymentAccessDenied,
    CkassaPaymentConfigError,
    CkassaPaymentError,
    CkassaProviderNotFound,
)
from database import ActiveDialog, DailyDialogLimitReached, Database
from keyboards import (
    CANCEL_SEARCH_BUTTON,
    CALL_BUTTON,
    INTERESTS_BUTTON,
    NEXT_BUTTON,
    PREMIUM_BUTTON,
    REPORT_BUTTON,
    SEARCH_BUTTON,
    SETTINGS_BUTTON,
    SHARE_BUTTON,
    STOP_BUTTON,
    after_chat_keyboard,
    ad_keyboard,
    age_keyboard,
    chat_menu,
    call_keyboard,
    delete_confirmation_keyboard,
    gender_keyboard,
    interests_keyboard,
    main_menu,
    payment_keyboard,
    preferred_gender_keyboard,
    registration_gender_keyboard,
    report_reasons_keyboard,
    search_menu,
    settings_keyboard,
    share_confirmation_keyboard,
    subscription_keyboard,
)
from payments import SubscriptionPayments
from subscriptions import get_plan
from texts import (
    AGE_PROMPT,
    HELP,
    INTERESTS,
    MATCH_FOUND,
    NO_ACTIVE_CHAT,
    REPORT_REASONS,
    RULES,
    SEARCHING,
    SUPPORTED_CONTENT,
    SUBSCRIPTION_OFFER,
    WELCOME,
)


logger = logging.getLogger(__name__)

START_SOURCE_PATTERN = re.compile(r"^[A-Za-z0-9_-]{1,64}$")


class UserActivityMiddleware(BaseMiddleware):
    def __init__(self, database: Database) -> None:
        self.database = database

    async def __call__(
        self,
        handler: Callable[[TelegramObject, dict[str, Any]], Awaitable[Any]],
        event: TelegramObject,
        data: dict[str, Any],
    ) -> Any:
        user: User | None = data.get("event_from_user")
        if user and not user.is_bot:
            await self.database.upsert_user(user.id, user.username, user.first_name or "")
        return await handler(event, data)


def build_router(
    database: Database,
    settings: Settings,
    payments: SubscriptionPayments,
    admin_bot: Bot | None = None,
) -> Router:
    router = Router(name="echo")
    router.message.filter(F.chat.type == ChatType.PRIVATE)
    router.callback_query.filter(F.message.chat.type == ChatType.PRIVATE)
    router.message.outer_middleware(UserActivityMiddleware(database))
    router.callback_query.outer_middleware(UserActivityMiddleware(database))

    async def get_admin_chat_ids() -> set[int]:
        chat_ids = set(settings.admin_ids)
        chat_ids.update(
            await database.get_user_ids_by_usernames(settings.admin_usernames)
        )
        return chat_ids

    async def is_admin(user: User) -> bool:
        return user.id in await get_admin_chat_ids()

    async def require_adult(message: Message) -> dict[str, Any] | None:
        user = await database.get_user(message.from_user.id)
        if not user or not user["is_adult"]:
            await message.answer(AGE_PROMPT, reply_markup=age_keyboard())
            return None
        if user["is_banned"]:
            await message.answer(
                "Доступ к «ЭХО» ограничен модерацией. Если это ошибка, обратитесь в поддержку."
            )
            return None
        return user

    async def safe_send(
        bot: Bot,
        chat_id: int,
        text: str,
        **kwargs: Any,
    ) -> bool:
        try:
            await bot.send_message(chat_id, text, **kwargs)
            return True
        except TelegramForbiddenError:
            await database.mark_unreachable(chat_id)
            return False

    async def notify_new_user_admins(text: str) -> None:
        """Send acquisition notifications only through the separate admin bot."""
        if admin_bot is None:
            logger.error(
                "Уведомление о новом пользователе пропущено: ADMIN_BOT_TOKEN не задан"
            )
            return

        for admin_id in await get_admin_chat_ids():
            try:
                await admin_bot.send_message(admin_id, text)
            except TelegramAPIError:
                logger.warning(
                    "Админ-бот не смог отправить уведомление пользователю %s",
                    admin_id,
                    exc_info=True,
                )

    async def subscription_text(user_id: int, intro: str = "") -> str:
        membership = await database.get_membership(user_id)
        if membership["tier"] == "vip":
            label = "👑 <b>VIP</b>"
        elif membership["tier"] == "premium":
            label = "💎 <b>Premium</b>"
        else:
            label = "бесплатный"
        if membership["expires_at"]:
            moscow = timezone(timedelta(hours=3))
            expires = membership["expires_at"].astimezone(moscow).strftime(
                "%d.%m.%Y %H:%M МСК"
            )
            status = f"Твой статус: {label} до <b>{expires}</b>."
        else:
            status = f"Твой статус: {label}."
        prefix = f"{intro}\n\n" if intro else ""
        return f"{prefix}{status}\n\n{SUBSCRIPTION_OFFER}"

    async def send_subscription_offer(message: Message, intro: str = "") -> None:
        await message.answer(
            await subscription_text(message.from_user.id, intro),
            reply_markup=subscription_keyboard(),
        )

    async def maybe_send_ad(bot: Bot, user_id: int) -> None:
        if not settings.ad_text or await database.has_premium(user_id):
            return
        dialogs = await database.dialog_count(user_id)
        if dialogs == 0 or dialogs % settings.ad_dialog_interval:
            return
        await safe_send(
            bot,
            user_id,
            html.escape(settings.ad_text),
            reply_markup=ad_keyboard(settings.ad_url, settings.ad_button_text),
            disable_web_page_preview=True,
        )

    async def match_found_text(partner_id: int) -> str:
        if await database.is_vip(partner_id):
            return f"{MATCH_FOUND}\n\n👑 У собеседника статус <b>VIP</b>."
        return MATCH_FOUND

    async def send_rating(bot: Bot, chat_id: int, dialog_id: int) -> None:
        await safe_send(
            bot,
            chat_id,
            "Вы снова стали незнакомцами. Каким был разговор?",
            reply_markup=after_chat_keyboard(dialog_id),
        )

    async def notify_partner_ended(
        bot: Bot,
        active: ActiveDialog,
        text: str = "<b>Связь затихла.</b> Собеседник завершил разговор.",
    ) -> None:
        delivered = await safe_send(
            bot,
            active.partner_id,
            text,
            reply_markup=main_menu(),
        )
        if delivered:
            await send_rating(bot, active.partner_id, active.dialog_id)
            await maybe_send_ad(bot, active.partner_id)

    async def begin_search(message: Message, bot: Bot) -> None:
        user = await require_adult(message)
        if not user:
            return
        if await database.get_active(message.from_user.id):
            await message.answer("Связь уже установлена.", reply_markup=chat_menu())
            return

        try:
            # Несколько попыток нужны, если ожидающий пользователь успел заблокировать бота.
            for _ in range(3):
                match = await database.enqueue_or_match(
                    message.from_user.id, settings.free_daily_dialog_limit
                )
                if match is None:
                    await message.answer(SEARCHING, reply_markup=search_menu())
                    return
                delivered = await safe_send(
                    bot,
                    match.partner_id,
                    await match_found_text(message.from_user.id),
                    reply_markup=chat_menu(),
                )
                if delivered:
                    await message.answer(
                        await match_found_text(match.partner_id),
                        reply_markup=chat_menu(),
                    )
                    return
                await database.end_dialog(message.from_user.id, "partner_unreachable")

            # Редкий случай: три устаревших аккаунта подряд. Возвращаем пользователя в очередь.
            await database.enqueue_or_match(
                message.from_user.id, settings.free_daily_dialog_limit
            )
            await message.answer(SEARCHING, reply_markup=search_menu())
        except DailyDialogLimitReached as error:
            await database.cancel_search(message.from_user.id)
            await send_subscription_offer(
                message,
                "На сегодня бесплатные диалоги закончились "
                f"({error.used} из {error.limit}). С Premium лимита нет.",
            )

    async def finish_dialog(message: Message, bot: Bot, search_again: bool) -> None:
        active = await database.end_dialog(
            message.from_user.id, "next" if search_again else "stopped"
        )
        if not active:
            cancelled = await database.cancel_search(message.from_user.id)
            if cancelled:
                await message.answer("Сигнал отозван.", reply_markup=main_menu())
            else:
                await message.answer(NO_ACTIVE_CHAT, reply_markup=main_menu())
            return

        await notify_partner_ended(bot, active)
        if search_again:
            await message.answer("<b>Связь затихла.</b> Ищем другое эхо…")
            await maybe_send_ad(bot, message.from_user.id)
            await begin_search(message, bot)
            return

        await message.answer("<b>Связь затихла.</b>", reply_markup=main_menu())
        await send_rating(bot, message.from_user.id, active.dialog_id)
        await maybe_send_ad(bot, message.from_user.id)

    async def show_interests(message: Message) -> None:
        if not await require_adult(message):
            return
        selected = await database.get_interests(message.from_user.id)
        labels = [INTERESTS[code] for code in INTERESTS if code in selected]
        summary = ", ".join(labels) if labels else "пока ничего"
        await message.answer(
            f"<b>Мои интересы</b>\n\nВыбрано: {summary}.\n"
            "Люди с общими интересами получают приоритет при поиске.",
            reply_markup=interests_keyboard(selected),
        )

    async def show_settings(message: Message) -> None:
        user = await require_adult(message)
        if not user:
            return
        membership = await database.get_membership(message.from_user.id)
        await message.answer(
            "<b>Настройки</b>\n\n"
            "Свой пол указывается бесплатно. Фильтр пола собеседника доступен "
            "с Premium и учитывается взаимно.",
            reply_markup=settings_keyboard(user, membership["has_premium"]),
        )

    async def ask_to_share_profile(message: Message) -> None:
        if not await require_adult(message):
            return
        active = await database.get_active(message.from_user.id)
        if not active:
            await message.answer(NO_ACTIVE_CHAT)
            return
        await message.answer(
            "<b>Открыть профиль?</b>\n\nСобеседник увидит ссылку на твой Telegram-профиль. "
            "Это действие нельзя отменить.",
            reply_markup=share_confirmation_keyboard(active.dialog_id),
        )

    async def ask_for_report(message: Message, bot: Bot) -> None:
        if not await require_adult(message):
            return
        active = await database.end_dialog(message.from_user.id, "report_started")
        if not active:
            await message.answer("Жалобу можно отправить после состоявшегося разговора.")
            return
        await notify_partner_ended(bot, active)
        await message.answer(
            "Связь завершена. Что произошло?",
            reply_markup=main_menu(),
        )
        await message.answer(
            "<b>Причина жалобы</b>",
            reply_markup=report_reasons_keyboard(active.dialog_id),
        )

    async def notify_admins(
        bot: Bot, reporter_id: int, reported_id: int, dialog_id: int, reason: str
    ) -> None:
        text = (
            "<b>Новая жалоба в «ЭХО»</b>\n"
            f"Диалог: <code>{dialog_id}</code>\n"
            f"Отправитель: <code>{reporter_id}</code>\n"
            f"На пользователя: <code>{reported_id}</code>\n"
            f"Причина: {html.escape(reason)}"
        )
        for admin_id in await get_admin_chat_ids():
            await safe_send(bot, admin_id, text)

    @router.message(CommandStart())
    async def start_handler(
        message: Message,
        command: CommandObject,
        bot: Bot,
    ) -> None:
        source = (command.args or "").strip()
        if START_SOURCE_PATTERN.fullmatch(source):
            created, source_users = await database.record_start_source(
                message.from_user.id,
                source,
            )
            if created:
                username = (
                    f"@{html.escape(message.from_user.username)}"
                    if message.from_user.username
                    else "не указан"
                )
                notification = (
                    "<b>Новый переход по рекламной ссылке</b>\n"
                    f"Источник: <code>{html.escape(source)}</code>\n"
                    f"Пользователь: <code>{message.from_user.id}</code>\n"
                    f"Username: {username}\n"
                    f"Пользователей из этого источника: <b>{source_users}</b>"
                )
                await notify_new_user_admins(notification)
        user = await database.get_user(message.from_user.id)
        if user and user["is_banned"]:
            await message.answer("Доступ к «ЭХО» ограничен модерацией.")
            return
        if not user or not user["is_adult"]:
            await message.answer(WELCOME)
            await message.answer(AGE_PROMPT, reply_markup=age_keyboard())
            return
        if await database.get_active(message.from_user.id):
            await message.answer("<b>Ты в «ЭХО».</b> Связь установлена.", reply_markup=chat_menu())
            return
        if await database.is_searching(message.from_user.id):
            await message.answer(SEARCHING, reply_markup=search_menu())
            return
        await message.answer(WELCOME, reply_markup=main_menu())

    @router.callback_query(F.data == "age:accept")
    async def age_accept_handler(callback: CallbackQuery) -> None:
        await database.confirm_adult(callback.from_user.id)
        await callback.answer("Возраст подтверждён")
        await callback.message.edit_text(
            "Укажи свой пол — это поможет взаимным фильтрам поиска. Можно не отвечать.",
            reply_markup=registration_gender_keyboard(),
        )

    @router.callback_query(F.data == "age:decline")
    async def age_decline_handler(callback: CallbackQuery) -> None:
        await database.cancel_search(callback.from_user.id)
        await callback.answer()
        await callback.message.edit_text(
            "«ЭХО» предназначено только для совершеннолетних. Возвращайся после 18 лет."
        )

    @router.callback_query(F.data.startswith("reg_gender:"))
    async def registration_gender_handler(callback: CallbackQuery) -> None:
        value = callback.data.split(":", 1)[1]
        await database.set_gender(callback.from_user.id, None if value == "unknown" else value)
        await callback.answer("Сохранено")
        await callback.message.edit_text("Настройка завершена. Добро пожаловать в «ЭХО».")
        await callback.message.answer(WELCOME, reply_markup=main_menu())

    @router.message(Command("help"))
    async def help_handler(message: Message) -> None:
        await message.answer(HELP)

    @router.message(Command("rules"))
    async def rules_handler(message: Message) -> None:
        await message.answer(RULES)

    @router.message(Command("search"))
    @router.message(F.text == SEARCH_BUTTON)
    async def search_handler(message: Message, bot: Bot) -> None:
        await begin_search(message, bot)

    @router.message(Command("next"))
    @router.message(F.text == NEXT_BUTTON)
    async def next_handler(message: Message, bot: Bot) -> None:
        if not await require_adult(message):
            return
        await finish_dialog(message, bot, search_again=True)

    @router.message(Command("stop", "cancel"))
    @router.message(F.text.in_({STOP_BUTTON, CANCEL_SEARCH_BUTTON}))
    async def stop_handler(message: Message, bot: Bot) -> None:
        if not await require_adult(message):
            return
        await finish_dialog(message, bot, search_again=False)

    @router.message(Command("interests"))
    @router.message(F.text == INTERESTS_BUTTON)
    async def interests_handler(message: Message) -> None:
        await show_interests(message)

    @router.callback_query(F.data.startswith("interest:"))
    async def interest_toggle_handler(callback: CallbackQuery) -> None:
        code = callback.data.split(":", 1)[1]
        if code not in INTERESTS:
            await callback.answer("Неизвестный интерес", show_alert=True)
            return
        await database.toggle_interest(callback.from_user.id, code)
        selected = await database.get_interests(callback.from_user.id)
        labels = [INTERESTS[item] for item in INTERESTS if item in selected]
        summary = ", ".join(labels) if labels else "пока ничего"
        await callback.message.edit_text(
            f"<b>Мои интересы</b>\n\nВыбрано: {summary}.\n"
            "Люди с общими интересами получают приоритет при поиске.",
            reply_markup=interests_keyboard(selected),
        )
        await callback.answer()

    @router.callback_query(F.data == "interests:clear")
    async def interests_clear_handler(callback: CallbackQuery) -> None:
        await database.clear_interests(callback.from_user.id)
        await callback.message.edit_text(
            "<b>Мои интересы</b>\n\nВыбрано: пока ничего.\n"
            "Люди с общими интересами получают приоритет при поиске.",
            reply_markup=interests_keyboard(set()),
        )
        await callback.answer("Интересы сброшены")

    @router.callback_query(F.data == "interests:done")
    async def interests_done_handler(callback: CallbackQuery) -> None:
        await callback.message.edit_text("Интересы сохранены.")
        await callback.answer()

    @router.message(Command("settings"))
    @router.message(F.text == SETTINGS_BUTTON)
    async def settings_handler(message: Message) -> None:
        await show_settings(message)

    @router.callback_query(F.data == "settings:gender")
    async def settings_gender_handler(callback: CallbackQuery) -> None:
        await callback.message.edit_text("Какой у тебя пол?", reply_markup=gender_keyboard())
        await callback.answer()

    @router.callback_query(F.data == "settings:preferred_gender")
    async def settings_preferred_handler(callback: CallbackQuery) -> None:
        if not await database.has_premium(callback.from_user.id):
            await database.set_preferred_gender(callback.from_user.id, "any")
            await callback.message.edit_text(
                await subscription_text(
                    callback.from_user.id,
                    "Фильтр пола собеседника доступен с Premium. "
                    "Свой пол по-прежнему можно указать бесплатно.",
                ),
                reply_markup=subscription_keyboard(),
            )
            await callback.answer("Нужен Premium", show_alert=True)
            return
        await callback.message.edit_text(
            "Собеседника какого пола искать?", reply_markup=preferred_gender_keyboard()
        )
        await callback.answer()

    @router.callback_query(F.data == "settings:blur")
    async def settings_blur_handler(callback: CallbackQuery) -> None:
        enabled = await database.toggle_blur_media(callback.from_user.id)
        user = await database.get_user(callback.from_user.id)
        membership = await database.get_membership(callback.from_user.id)
        await callback.message.edit_text(
            "<b>Настройки</b>\n\n"
            "Фото, видео и GIF со спойлером открываются только после нажатия.",
            reply_markup=settings_keyboard(user, membership["has_premium"]),
        )
        await callback.answer("Скрытие медиа включено" if enabled else "Скрытие медиа выключено")

    @router.callback_query(F.data.startswith("set_gender:"))
    async def set_gender_handler(callback: CallbackQuery) -> None:
        value = callback.data.split(":", 1)[1]
        await database.set_gender(callback.from_user.id, None if value == "unknown" else value)
        user = await database.get_user(callback.from_user.id)
        membership = await database.get_membership(callback.from_user.id)
        await callback.message.edit_text(
            "<b>Настройки</b>",
            reply_markup=settings_keyboard(user, membership["has_premium"]),
        )
        await callback.answer("Сохранено")

    @router.callback_query(F.data.startswith("set_preferred:"))
    async def set_preferred_handler(callback: CallbackQuery) -> None:
        value = callback.data.split(":", 1)[1]
        if value != "any" and not await database.has_premium(callback.from_user.id):
            await database.set_preferred_gender(callback.from_user.id, "any")
            await callback.message.edit_text(
                await subscription_text(
                    callback.from_user.id,
                    "Фильтр пола собеседника доступен с Premium.",
                ),
                reply_markup=subscription_keyboard(),
            )
            await callback.answer("Нужен Premium", show_alert=True)
            return
        await database.set_preferred_gender(callback.from_user.id, value)
        user = await database.get_user(callback.from_user.id)
        membership = await database.get_membership(callback.from_user.id)
        await callback.message.edit_text(
            "<b>Настройки</b>",
            reply_markup=settings_keyboard(user, membership["has_premium"]),
        )
        await callback.answer("Фильтр сохранён")

    @router.callback_query(F.data == "settings:back")
    async def settings_back_handler(callback: CallbackQuery) -> None:
        user = await database.get_user(callback.from_user.id)
        membership = await database.get_membership(callback.from_user.id)
        await callback.message.edit_text(
            "<b>Настройки</b>",
            reply_markup=settings_keyboard(user, membership["has_premium"]),
        )
        await callback.answer()

    @router.callback_query(F.data == "settings:close")
    async def settings_close_handler(callback: CallbackQuery) -> None:
        await callback.message.edit_text("Настройки сохранены.")
        await callback.answer()

    @router.message(Command("premium", "vip"))
    @router.message(F.text == PREMIUM_BUTTON)
    async def subscription_handler(message: Message) -> None:
        if not await require_adult(message):
            return
        await send_subscription_offer(message)

    @router.callback_query(F.data == "subscription:show")
    async def subscription_show_handler(callback: CallbackQuery) -> None:
        await callback.message.edit_text(
            await subscription_text(callback.from_user.id),
            reply_markup=subscription_keyboard(),
        )
        await callback.answer()

    @router.callback_query(F.data == "subscription:close")
    async def subscription_close_handler(callback: CallbackQuery) -> None:
        await callback.message.edit_text("Меню Premium и VIP закрыто.")
        await callback.answer()

    @router.callback_query(F.data.startswith("subscription:buy:"))
    async def subscription_buy_handler(callback: CallbackQuery, bot: Bot) -> None:
        plan_code = callback.data.rsplit(":", 1)[-1]
        plan = get_plan(plan_code)
        if not plan:
            await callback.answer("Тариф не найден", show_alert=True)
            return
        user = await database.get_user(callback.from_user.id)
        if not user or not user["is_adult"] or user["is_banned"]:
            await callback.answer("Покупка сейчас недоступна", show_alert=True)
            return
        await callback.answer("Создаю безопасную ссылку CKassa…")
        try:
            order, reused = await payments.create_or_reuse_order(
                callback.from_user.id, plan
            )
        except (CkassaProviderNotFound, CkassaPaymentAccessDenied) as error:
            logger.error("CKassa отклонила создание счёта: %s", error)
            await callback.message.edit_text(
                "CKassa временно не принимает оплату. Администратор уже уведомлён.",
                reply_markup=subscription_keyboard(),
            )
            for admin_id in await get_admin_chat_ids():
                await safe_send(bot, admin_id, f"[CKassa ЭХО] {error}")
            return
        except CkassaPaymentConfigError as error:
            logger.error("CKassa не настроена: %s", error)
            await callback.message.edit_text(
                "Оплата пока не настроена. Администратор уже получил причину.",
                reply_markup=subscription_keyboard(),
            )
            for admin_id in await get_admin_chat_ids():
                await safe_send(bot, admin_id, f"[CKassa ЭХО] Ошибка настройки: {error}")
            return
        except CkassaPaymentError:
            logger.exception("Не удалось создать счёт CKassa")
            await callback.message.edit_text(
                "Не получилось создать ссылку на оплату. Попробуй чуть позже.",
                reply_markup=subscription_keyboard(),
            )
            return
        except Exception:
            logger.exception("Внутренняя ошибка при создании счёта CKassa")
            await callback.message.edit_text(
                "Не получилось подготовить оплату. Попробуй чуть позже.",
                reply_markup=subscription_keyboard(),
            )
            return

        prefix = "Активная ссылка уже была создана." if reused else "Ссылка готова."
        await callback.message.edit_text(
            f"💳 <b>{html.escape(plan.title)}</b>\n\n"
            f"{prefix} Сумма: <b>{plan.price_rubles} ₽</b>.\n"
            "После оплаты нажми «Проверить оплату». Бот также проверяет платежи "
            "автоматически.",
            reply_markup=payment_keyboard(
                order["invoice_url"], str(order["order_id"]), plan.price_rubles
            ),
            disable_web_page_preview=True,
        )

    @router.callback_query(F.data.startswith("subscription:check:"))
    async def subscription_check_handler(callback: CallbackQuery, bot: Bot) -> None:
        order_id = callback.data.rsplit(":", 1)[-1]
        order = await database.get_payment_order(order_id)
        if not order or int(order["user_id"]) != callback.from_user.id:
            await callback.answer("Счёт не найден", show_alert=True)
            return
        await callback.answer("Проверяю оплату…")
        try:
            await payments.process_updates(bot)
        except CkassaPaymentError:
            logger.exception("Ручная проверка платежа CKassa не удалась")
            await callback.message.answer(
                "CKassa пока не ответила. Попробуй проверить оплату чуть позже."
            )
            return
        except Exception:
            logger.exception("Внутренняя ошибка при проверке платежа CKassa")
            await callback.message.answer(
                "Не получилось проверить платёж. Попробуй чуть позже."
            )
            return
        order = await database.get_payment_order(order_id)
        if order and order["credited"]:
            await callback.message.edit_text(
                "✅ Оплата получена, подписка уже подключена."
            )
            return
        await callback.message.answer(
            "Платёж пока не найден. Если ты только что оплатил, подожди минуту и "
            "нажми «Проверить оплату» ещё раз."
        )

    @router.message(Command("link"))
    @router.message(F.text == SHARE_BUTTON)
    async def share_prompt_handler(message: Message) -> None:
        await ask_to_share_profile(message)

    @router.message(Command("call"))
    @router.message(F.text == CALL_BUTTON)
    async def call_handler(message: Message, bot: Bot) -> None:
        if not await require_adult(message):
            return
        active = await database.get_active(message.from_user.id)
        if not active:
            await message.answer(NO_ACTIVE_CHAT)
            return
        if not await database.has_premium(message.from_user.id):
            await send_subscription_offer(
                message,
                "Аудио- и видеозвонки доступны с Premium.",
            )
            return
        if not settings.call_base_url:
            await message.answer("Звонки пока не настроены администратором.")
            return
        room = await database.get_or_create_call_room(active.dialog_id)
        url = f"{settings.call_base_url}/{room}"
        call_text = (
            "<b>Комната для анонимного звонка готова.</b>\n\n"
            "Ссылка действует для текущего разговора. Звонок проходит через внешний "
            "WebRTC-сервис: не называй личные данные, если не хочешь раскрывать себя."
        )
        delivered = await safe_send(
            bot,
            active.partner_id,
            call_text,
            reply_markup=call_keyboard(url),
        )
        if not delivered:
            ended = await database.end_dialog(message.from_user.id, "partner_unreachable")
            await message.answer(
                "Собеседник уже недоступен. Связь завершена.", reply_markup=main_menu()
            )
            if ended:
                await send_rating(bot, message.from_user.id, ended.dialog_id)
            return
        await message.answer(call_text, reply_markup=call_keyboard(url))

    @router.callback_query(F.data.startswith("share:"))
    async def share_handler(callback: CallbackQuery, bot: Bot) -> None:
        value = callback.data.split(":", 1)[1]
        if value == "cancel":
            await callback.message.edit_text("Профиль остался скрытым.")
            await callback.answer()
            return
        try:
            dialog_id = int(value)
        except ValueError:
            await callback.answer("Некорректная ссылка", show_alert=True)
            return
        active = await database.get_active(callback.from_user.id)
        if not active or active.dialog_id != dialog_id:
            await callback.answer("Эта связь уже завершена", show_alert=True)
            return
        user = callback.from_user
        if user.username:
            profile_url = f"https://t.me/{user.username}"
            label = f"@{html.escape(user.username)}"
        else:
            profile_url = f"tg://user?id={user.id}"
            label = html.escape(user.first_name or "Открыть профиль")
        delivered = await safe_send(
            bot,
            active.partner_id,
            "Собеседник решил открыть профиль:\n"
            f'<a href="{profile_url}">{label}</a>',
        )
        if delivered:
            await callback.message.edit_text("Профиль открыт собеседнику.")
            await callback.answer("Отправлено")
        else:
            ended = await database.end_dialog(callback.from_user.id, "partner_unreachable")
            await callback.message.edit_text("Собеседник уже недоступен. Связь завершена.")
            if ended:
                await send_rating(bot, callback.from_user.id, ended.dialog_id)
            await callback.answer()

    @router.message(F.text == REPORT_BUTTON)
    async def report_button_handler(message: Message, bot: Bot) -> None:
        await ask_for_report(message, bot)

    @router.callback_query(F.data.startswith("report:"))
    async def report_prompt_handler(callback: CallbackQuery) -> None:
        value = callback.data.split(":", 1)[1]
        if value == "cancel":
            await callback.message.edit_text("Жалоба отменена.")
            await callback.answer()
            return
        try:
            dialog_id = int(value)
        except ValueError:
            await callback.answer("Некорректный диалог", show_alert=True)
            return
        if not await database.is_dialog_member(callback.from_user.id, dialog_id):
            await callback.answer("Диалог не найден", show_alert=True)
            return
        await callback.message.edit_text(
            "<b>Причина жалобы</b>",
            reply_markup=report_reasons_keyboard(dialog_id),
        )
        await callback.answer()

    @router.callback_query(F.data.startswith("report_reason:"))
    async def report_reason_handler(callback: CallbackQuery, bot: Bot) -> None:
        parts = callback.data.split(":", 2)
        if len(parts) != 3 or not parts[1].isdigit() or parts[2] not in REPORT_REASONS:
            await callback.answer("Некорректная жалоба", show_alert=True)
            return
        dialog_id = int(parts[1])
        reason_code = parts[2]
        reported_id, created = await database.create_report(
            callback.from_user.id, dialog_id, reason_code
        )
        if reported_id is None:
            await callback.answer("Диалог не найден", show_alert=True)
            return
        if created:
            await notify_admins(
                bot,
                callback.from_user.id,
                reported_id,
                dialog_id,
                REPORT_REASONS[reason_code],
            )
            await callback.message.edit_text(
                "Жалоба отправлена. Этот пользователь больше не попадётся тебе в поиске."
            )
            await callback.answer("Спасибо. Мы учтём жалобу.")
        else:
            await callback.message.edit_text("Ты уже отправлял жалобу на этот разговор.")
            await callback.answer()

    @router.callback_query(F.data.startswith("rate:"))
    async def rating_handler(callback: CallbackQuery) -> None:
        parts = callback.data.split(":", 2)
        if len(parts) != 3 or not parts[1].isdigit() or parts[2] not in {"up", "down"}:
            await callback.answer("Некорректная оценка", show_alert=True)
            return
        value = 1 if parts[2] == "up" else -1
        saved = await database.rate_dialog(callback.from_user.id, int(parts[1]), value)
        if not saved:
            await callback.answer("Диалог не найден", show_alert=True)
            return
        await callback.message.edit_text("Спасибо за оценку.")
        await callback.answer()

    @router.message(Command("delete_me"))
    async def delete_me_handler(message: Message) -> None:
        await message.answer(
            "<b>Удалить все данные?</b>\n\nБудут удалены профиль, интересы, история связей, "
            "оценки и жалобы. Действие необратимо.",
            reply_markup=delete_confirmation_keyboard(),
        )

    @router.callback_query(F.data == "settings:delete")
    async def settings_delete_handler(callback: CallbackQuery) -> None:
        await callback.message.edit_text(
            "<b>Удалить все данные?</b>\n\nДействие необратимо.",
            reply_markup=delete_confirmation_keyboard(),
        )
        await callback.answer()

    @router.callback_query(F.data == "delete:cancel")
    async def delete_cancel_handler(callback: CallbackQuery) -> None:
        await callback.message.edit_text("Удаление отменено.")
        await callback.answer()

    @router.callback_query(F.data == "delete:confirm")
    async def delete_confirm_handler(callback: CallbackQuery, bot: Bot) -> None:
        active = await database.delete_user(callback.from_user.id)
        if active:
            await safe_send(
                bot,
                active.partner_id,
                "<b>Связь затихла.</b> Собеседник удалил профиль.",
                reply_markup=main_menu(),
            )
        await callback.message.edit_text("Твои данные удалены.")
        await callback.message.answer(
            "Чтобы вернуться, отправь /start.", reply_markup=ReplyKeyboardRemove()
        )
        await callback.answer("Удалено")

    @router.message(Command("stats"))
    async def admin_stats_handler(message: Message) -> None:
        if not await is_admin(message.from_user):
            await message.answer("Команда недоступна.")
            return
        stats = await database.stats()
        sources = await database.source_stats()
        source_lines = (
            "\n\n<b>Источники:</b>\n"
            + "\n".join(
                f"<code>{html.escape(source)}</code>: {users}"
                for source, users in sources
            )
            if sources
            else ""
        )
        await message.answer(
            "<b>Статистика «ЭХО»</b>\n"
            f"Пользователи: {stats['users']}\n"
            f"18+: {stats['adults']}\n"
            f"В очереди: {stats['queue']}\n"
            f"Активные связи: {stats['active_dialogs']}\n"
            f"Всего диалогов: {stats['dialogs']}\n"
            f"Жалобы: {stats['reports']}\n"
            f"Заблокированы: {stats['banned']}\n"
            f"Premium: {stats['premium']}\n"
            f"VIP: {stats['vip']}\n"
            f"Оплачено подписок: {stats['payments']}\n"
            f"Выручка: {stats['revenue_kopeks'] // 100} ₽"
            + source_lines
        )

    @router.message(Command("ban", "unban"))
    async def admin_ban_handler(
        message: Message, command: CommandObject, bot: Bot
    ) -> None:
        if not await is_admin(message.from_user):
            await message.answer("Команда недоступна.")
            return
        if not command.args or not command.args.strip().isdigit():
            await message.answer("Формат: /ban 123456789 или /unban 123456789")
            return
        target_id = int(command.args.strip())
        banned = command.command == "ban"
        active = await database.set_banned(target_id, banned)
        if active:
            await safe_send(
                bot,
                active.partner_id,
                "<b>Связь затихла.</b>",
                reply_markup=main_menu(),
            )
        await message.answer(
            f"Пользователь <code>{target_id}</code> "
            + ("заблокирован." if banned else "разблокирован.")
        )

    @router.message(F.text.startswith("/"))
    async def unknown_command_handler(message: Message) -> None:
        await message.answer("Не знаю такой команды. Список команд: /help")

    @router.message()
    async def relay_handler(message: Message, bot: Bot) -> None:
        if not await require_adult(message):
            return
        active = await database.get_active(message.from_user.id)
        if not active:
            if await database.is_searching(message.from_user.id):
                await message.answer("Сигнал уже отправлен. Осталось дождаться отклика.")
            else:
                await message.answer(NO_ACTIVE_CHAT, reply_markup=main_menu())
            return
        if message.content_type not in SUPPORTED_CONTENT:
            await message.answer(
                "Такой тип сообщения пока не поддерживается. Можно отправить текст, фото, "
                "видео, GIF, документ, стикер, голосовое, видеосообщение или аудио."
            )
            return

        partner = await database.get_user(active.partner_id)
        blur_media = bool(partner and partner["blur_media"])
        reply_parameters: ReplyParameters | None = None
        if message.reply_to_message:
            reply_target = await database.get_relayed_reply_target(
                active.dialog_id,
                message.from_user.id,
                message.reply_to_message.message_id,
                active.partner_id,
            )
            if reply_target is not None:
                reply_parameters = ReplyParameters(
                    message_id=reply_target,
                    allow_sending_without_reply=True,
                )
        try:
            if message.photo:
                sent_message = await bot.send_photo(
                    active.partner_id,
                    message.photo[-1].file_id,
                    caption=message.caption,
                    caption_entities=message.caption_entities,
                    has_spoiler=blur_media,
                    protect_content=True,
                    parse_mode=None,
                    reply_parameters=reply_parameters,
                )
            elif message.video:
                sent_message = await bot.send_video(
                    active.partner_id,
                    message.video.file_id,
                    caption=message.caption,
                    caption_entities=message.caption_entities,
                    has_spoiler=blur_media,
                    protect_content=True,
                    parse_mode=None,
                    reply_parameters=reply_parameters,
                )
            elif message.animation:
                sent_message = await bot.send_animation(
                    active.partner_id,
                    message.animation.file_id,
                    caption=message.caption,
                    caption_entities=message.caption_entities,
                    has_spoiler=blur_media,
                    protect_content=True,
                    parse_mode=None,
                    reply_parameters=reply_parameters,
                )
            else:
                sent_message = await bot.copy_message(
                    chat_id=active.partner_id,
                    from_chat_id=message.chat.id,
                    message_id=message.message_id,
                    protect_content=True,
                    reply_parameters=reply_parameters,
                )
            await database.record_relayed_message(
                active.dialog_id,
                message.from_user.id,
                message.message_id,
                active.partner_id,
                sent_message.message_id,
            )
        except TelegramForbiddenError:
            await database.mark_unreachable(active.partner_id)
            ended = await database.end_dialog(message.from_user.id, "partner_unreachable")
            await message.answer(
                "<b>Связь оборвалась.</b> Собеседник стал недоступен.",
                reply_markup=main_menu(),
            )
            if ended:
                await send_rating(bot, message.from_user.id, ended.dialog_id)
        except TelegramBadRequest:
            logger.exception(
                "Telegram отклонил пересылку типа %s без сохранения содержимого",
                message.content_type,
            )
            await message.answer("Не получилось передать это сообщение. Попробуй другой формат.")

    return router
