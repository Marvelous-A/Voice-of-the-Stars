from __future__ import annotations

import html
import logging
from difflib import SequenceMatcher
from datetime import timedelta, timezone

from aiogram import Bot, F, Router
from aiogram.exceptions import TelegramAPIError, TelegramBadRequest
from aiogram.filters import Command, CommandStart
from aiogram.types import CallbackQuery, Message, User

from database import Database
from keyboards import (
    BTN_NOTIFICATIONS,
    BTN_OVERVIEW,
    BTN_REGION,
    BTN_SAFETY,
    BTN_SOURCES,
    BTN_STATS,
    delete_confirmation_keyboard,
    main_keyboard,
    notifications_keyboard,
    overview_keyboard,
    region_search_keyboard,
    regions_keyboard,
    safety_keyboard,
    sources_keyboard,
    stats_keyboard,
)
from models import NationalOverview, Region, RegionStats, normalize_region_name
from source import BplaRussiaClient, SourceError


logger = logging.getLogger(__name__)
MSK = timezone(timedelta(hours=3))


def _escape(value: object) -> str:
    return html.escape(str(value or ""))


def _region_from_user(user: dict) -> Region | None:
    if user.get("region_id") is None:
        return None
    return Region(
        id=int(user["region_id"]),
        name=str(user.get("region_name", "")),
        slug=str(user.get("region_slug", "")),
        incidents_total=0,
        url=f"https://bplarussia.ru/region/{user.get('region_slug', '')}/",
    )


def _search_regions(regions: tuple[Region, ...], query: str) -> list[Region]:
    needle = normalize_region_name(query)
    if not needle:
        return []
    aliases = {
        "москва": "московская область",
        "подмосковье": "московская область",
        "питер": "санкт петербург",
        "спб": "санкт петербург",
    }
    needle = aliases.get(needle, needle)
    tokens = needle.split()

    def score(region: Region) -> tuple[int, str]:
        name = normalize_region_name(region.name)
        if name == needle:
            rank = 0
        elif name.startswith(needle):
            rank = 1
        elif needle in name:
            rank = 2
        elif all(token in name for token in tokens):
            rank = 3
        elif max(
            (SequenceMatcher(None, token, part).ratio() for token in tokens for part in name.split()),
            default=0.0,
        ) >= 0.78:
            rank = 4
        else:
            rank = 99
        return rank, name

    matches = [region for region in regions if score(region)[0] < 99]
    return sorted(matches, key=score)[:12]


def _last_event_status(stats: RegionStats) -> str:
    if not stats.recent_events:
        return "⚪ За последние 24 часа записей нет"
    latest = stats.recent_events[0]
    searchable = f"{latest.incident_type} {latest.title}".casefold()
    if "отбой" in searchable:
        return "🟢 Последняя запись — отбой опасности"
    if any(marker in searchable for marker in ("опасност", "тревог", "угроз", "обнаруж")):
        return "🔴 Последняя запись сообщает об угрозе"
    return "🟡 Есть новое информационное сообщение"


def render_stats(stats: RegionStats) -> str:
    risk_icons = {"НИЗКИЙ": "🟢", "СРЕДНИЙ": "🟡", "ВЫСОКИЙ": "🔴"}
    updated = stats.updated_at.astimezone(MSK).strftime("%H:%M МСК")
    lines = [
        f"<b>📍 {_escape(stats.region.name)}</b>",
        _last_event_status(stats),
        "",
        f"<b>Уровень риска:</b> {risk_icons.get(stats.risk_level, '⚪')} {stats.risk_level}",
        f"<b>Всего инцидентов:</b> {stats.region.incidents_total}",
        f"<b>Сработок ПВО (по публикациям):</b> {stats.air_defence_total}",
        "",
        "<b>За последние 24 часа:</b>",
        f"• публикаций: <b>{stats.incidents_24h}</b>",
        f"• сообщений об угрозе: <b>{stats.active_alerts_24h}</b>",
        f"• сообщений об обнаружении БПЛА: <b>{stats.detections_24h}</b>",
        f"• упоминаний работы ПВО: <b>{stats.air_defence_mentions_24h}</b>",
    ]

    if stats.breakdown:
        lines.extend(["", "<b>Типы сообщений:</b>"])
        lines.extend(
            f"• {_escape(label)} — {count}" for label, count in stats.breakdown
        )

    if stats.recent_events:
        lines.extend(["", "<b>Последние записи:</b>"])
        for event in stats.recent_events[:5]:
            timestamp = event.published_at.astimezone(MSK).strftime("%d.%m %H:%M")
            lines.append(f"• {timestamp} — {_escape(event.title)}")

    if stats.history_truncated:
        lines.extend(["", "Показана часть очень большой суточной ленты."])
    if stats.is_stale:
        lines.extend(["", "⚠️ Источник временно недоступен — показан последний сохранённый расчёт."])
    lines.extend(
        [
            "",
            f"Обновлено: {updated}",
            "<i>Это статистика публикаций bplarussia.ru, а не данные радара. "
            "«Сработки ПВО» — число записей, содержащих упоминание ПВО. "
            "Уровень риска — историческая шкала сайта по объёму архива. "
            "Число сообщений не равно числу беспилотников.</i>",
        ]
    )
    return "\n".join(lines)


def render_national_overview(overview: NationalOverview) -> str:
    updated = overview.updated_at.astimezone(MSK).strftime("%H:%M МСК")
    lines = [
        "<b>🗺 Обстановка по России</b>",
        "",
        f"<b>Всего инцидентов в базе:</b> {overview.incidents_total}",
        f"<b>Публикаций за 24 часа:</b> {overview.incidents_24h}",
        f"<b>Упоминаний ПВО за 24 часа:</b> {overview.air_defence_mentions_24h}",
        f"<b>Регионов с последним активным сигналом:</b> {len(overview.active_regions)}",
    ]
    if overview.active_regions:
        shown = overview.active_regions[:12]
        lines.extend(["", "<b>Активные сигналы:</b>", "• " + "\n• ".join(map(_escape, shown))])
        if len(overview.active_regions) > len(shown):
            lines.append(f"…и ещё {len(overview.active_regions) - len(shown)}")
    if overview.top_regions_24h:
        lines.extend(["", "<b>Больше всего сообщений за сутки:</b>"])
        lines.extend(
            f"• {_escape(name)} — {count}"
            for name, count in overview.top_regions_24h
        )
    if overview.is_stale:
        lines.extend(["", "⚠️ Показан последний сохранённый расчёт."])
    if overview.history_truncated:
        lines.extend(["", "Показана часть очень большой суточной ленты."])
    lines.extend(
        [
            "",
            f"Обновлено: {updated}",
            "<i>Сводка построена по публикациям bplarussia.ru и не заменяет "
            "официальные сигналы оповещения.</i>",
        ]
    )
    return "\n".join(lines)


def render_notification_settings(user: dict) -> tuple[str, str, str]:
    enabled = bool(user.get("notifications_enabled"))
    mode = str(user.get("notification_mode") or "all") if enabled else "off"
    scope = str(user.get("notification_scope") or "region")
    descriptions = {
        "all": "все новые публикации, включая информационные",
        "important": "только тревоги, обнаружения, ПВО и отбои",
        "off": "уведомления выключены",
    }
    geography = (
        "вся Россия"
        if scope == "all"
        else f"только {_escape(user.get('region_name'))}"
    )
    text = (
        "<b>Уведомления</b>\n\n"
        f"Домашний регион: <b>{_escape(user.get('region_name'))}</b>\n"
        f"География рассылки: <b>{geography}</b>\n"
        f"События: {descriptions[mode]}.\n\n"
        "Режим «Тревоги и отбои» уменьшает шум, но не скрывает сообщения "
        "об окончании опасности. Для всей России поток сообщений может быть большим."
    )
    return text, mode, scope


def format_new_user_notification(user: User, total_users: int) -> str:
    username = f"@{_escape(user.username)}" if user.username else "не указан"
    return (
        "🆕 <b>Новый пользователь «Небо рядом»</b>\n\n"
        f"Имя: <b>{_escape(user.full_name)}</b>\n"
        f"Username: {username}\n"
        f"ID: <code>{user.id}</code>\n\n"
        f"Всего пользователей: <b>{total_users}</b>"
    )


async def _notify_new_user(
    admin_bot: Bot | None, admin_id: int, user: User, total_users: int
) -> None:
    if admin_bot is None or admin_id <= 0:
        return
    try:
        await admin_bot.send_message(
            admin_id,
            format_new_user_notification(user, total_users),
        )
    except TelegramAPIError:
        logger.exception(
            "Не удалось отправить администратору уведомление о пользователе %s",
            user.id,
        )


async def _register(
    db: Database,
    message: Message,
    admin_bot: Bot | None = None,
    admin_id: int = 0,
) -> dict:
    user = message.from_user
    created = await db.register_user(
        user.id,
        username=user.username or "",
        full_name=user.full_name,
    )
    if created:
        await _notify_new_user(admin_bot, admin_id, user, await db.count_users())
    return (await db.get_user(user.id)) or {}


def build_router(
    db: Database,
    source: BplaRussiaClient,
    *,
    admin_bot: Bot | None = None,
    admin_id: int = 0,
) -> Router:
    router = Router(name="bpla-region-bot")

    async def send_region_picker(message: Message, page: int = 0) -> None:
        try:
            regions = await source.get_primary_regions()
        except SourceError:
            logger.exception("Не удалось загрузить регионы")
            await message.answer(
                "Сайт-источник сейчас не отвечает. Попробуйте выбрать регион чуть позже."
            )
            return
        await message.answer(
            "<b>Выберите свой регион</b>\n\n"
            "Листайте список кнопками ← → или просто напишите название, например: "
            "<code>Рязанская область</code>. Составные категории сайта скрыты, "
            "чтобы список был короче.",
            reply_markup=regions_keyboard(regions, page),
        )

    async def send_stats(
        message: Message, telegram_id: int, *, force: bool = False
    ) -> None:
        user = await db.get_user(telegram_id)
        region = _region_from_user(user or {})
        if region is None:
            await message.answer("Сначала выберите регион.")
            await send_region_picker(message)
            return
        wait_message = await message.answer("Собираю свежую сводку…")
        try:
            stats = await source.get_region_stats(region, force=force)
        except SourceError:
            logger.exception("Не удалось получить статистику для %s", region.name)
            await wait_message.edit_text(
                "Не удалось получить свежие данные с сайта. Попробуйте ещё раз через минуту."
            )
            return
        await wait_message.edit_text(
            render_stats(stats), reply_markup=stats_keyboard(stats.region)
        )

    async def send_overview(message: Message, *, force: bool = False) -> None:
        wait_message = await message.answer("Собираю общую обстановку…")
        try:
            overview = await source.get_national_overview(force=force)
        except SourceError:
            logger.exception("Не удалось получить общую сводку")
            await wait_message.edit_text(
                "Не удалось получить общую сводку. Попробуйте ещё раз через минуту."
            )
            return
        await wait_message.edit_text(
            render_national_overview(overview), reply_markup=overview_keyboard()
        )

    @router.message(CommandStart())
    async def start(message: Message) -> None:
        user = await _register(db, message, admin_bot, admin_id)
        region = _region_from_user(user)
        if region is None:
            await message.answer(
                "<b>Привет! Я буду следить за сообщениями о БПЛА в вашем регионе.</b>\n\n"
                "При появлении новой записи на bplarussia.ru пришлю уведомление. "
                "Также здесь можно посмотреть свой регион, обстановку по России, "
                "источники и памятку безопасности.\n\n"
                "⚠️ Бот не является официальной системой оповещения. В экстренной ситуации "
                "ориентируйтесь на сообщения властей и номер 112."
            )
            await send_region_picker(message)
            return

        await message.answer(
            f"С возвращением! Сейчас выбран регион: <b>{_escape(region.name)}</b>.\n"
            "Уведомления можно проверить или выключить в меню.",
            reply_markup=main_keyboard(),
        )

    @router.message(Command("region"))
    @router.message(F.text == BTN_REGION)
    async def choose_region(message: Message) -> None:
        await _register(db, message, admin_bot, admin_id)
        await send_region_picker(message)

    @router.callback_query(F.data.startswith("region:page:"))
    async def region_page(callback: CallbackQuery) -> None:
        page = int((callback.data or "0").rsplit(":", 1)[-1])
        try:
            regions = await source.get_primary_regions()
            await callback.message.edit_reply_markup(
                reply_markup=regions_keyboard(regions, page)
            )
        except (SourceError, TelegramBadRequest):
            logger.exception("Не удалось перелистнуть регионы")
        await callback.answer()

    @router.callback_query(F.data == "region:noop")
    async def region_noop(callback: CallbackQuery) -> None:
        await callback.answer()

    @router.callback_query(F.data.startswith("region:pick:"))
    async def region_pick(callback: CallbackQuery) -> None:
        region_id = int((callback.data or "0").rsplit(":", 1)[-1])
        created = await db.register_user(
            callback.from_user.id,
            callback.from_user.username or "",
            callback.from_user.full_name,
        )
        if created:
            await _notify_new_user(
                admin_bot,
                admin_id,
                callback.from_user,
                await db.count_users(),
            )
        try:
            region = await source.get_region(region_id)
        except SourceError:
            region = None
        if region is None:
            await callback.answer("Регион не найден. Обновите список.", show_alert=True)
            return
        await db.set_region(callback.from_user.id, region)
        await callback.answer("Регион сохранён")
        await callback.message.answer(
            f"✅ Выбран регион: <b>{_escape(region.name)}</b>\n"
            "Уведомления о новых публикациях включены.",
            reply_markup=main_keyboard(),
        )
        await send_stats(callback.message, callback.from_user.id)

    @router.message(Command("stats"))
    @router.message(F.text == BTN_STATS)
    async def stats(message: Message) -> None:
        await _register(db, message, admin_bot, admin_id)
        await send_stats(message, message.from_user.id)

    @router.callback_query(F.data == "stats:refresh")
    async def refresh_stats(callback: CallbackQuery) -> None:
        await callback.answer("Обновляю")
        await send_stats(callback.message, callback.from_user.id, force=True)

    @router.message(Command("overview"))
    @router.message(F.text == BTN_OVERVIEW)
    async def overview(message: Message) -> None:
        await _register(db, message, admin_bot, admin_id)
        await send_overview(message)

    @router.callback_query(F.data == "overview:refresh")
    async def refresh_overview(callback: CallbackQuery) -> None:
        await callback.answer("Обновляю")
        await send_overview(callback.message, force=True)

    @router.message(Command("alerts"))
    @router.message(F.text == BTN_NOTIFICATIONS)
    async def notification_settings(message: Message) -> None:
        user = await _register(db, message, admin_bot, admin_id)
        if user.get("region_id") is None:
            await message.answer("Сначала выберите регион.")
            await send_region_picker(message)
            return
        text, mode, scope = render_notification_settings(user)
        await message.answer(
            text,
            reply_markup=notifications_keyboard(mode, scope),
        )

    @router.callback_query(
        F.data.in_(
            {
                "notifications:all",
                "notifications:important",
                "notifications:off",
            }
        )
    )
    async def toggle_notifications(callback: CallbackQuery) -> None:
        mode = (callback.data or "notifications:off").split(":", 1)[1]
        await db.set_notification_mode(callback.from_user.id, mode)
        user = (await db.get_user(callback.from_user.id)) or {}
        text, actual_mode, scope = render_notification_settings(user)
        await callback.message.edit_text(
            text,
            reply_markup=notifications_keyboard(actual_mode, scope),
        )
        labels = {
            "all": "Все события",
            "important": "Тревоги и отбои",
            "off": "Уведомления выключены",
        }
        await callback.answer(labels[mode])

    @router.callback_query(
        F.data.in_({"notification_scope:region", "notification_scope:all"})
    )
    async def toggle_notification_scope(callback: CallbackQuery) -> None:
        scope = (callback.data or "notification_scope:region").split(":", 1)[1]
        await db.set_notification_scope(callback.from_user.id, scope)
        user = (await db.get_user(callback.from_user.id)) or {}
        text, mode, actual_scope = render_notification_settings(user)
        await callback.message.edit_text(
            text,
            reply_markup=notifications_keyboard(mode, actual_scope),
        )
        if scope == "all":
            await callback.answer(
                "Выбрана вся Россия. Сообщений может быть много.", show_alert=True
            )
        else:
            await callback.answer("Выбран домашний регион")

    @router.message(F.text == BTN_SAFETY)
    async def safety(message: Message) -> None:
        await _register(db, message, admin_bot, admin_id)
        await message.answer(
            "<b>🛡 Если объявлена опасность</b>\n\n"
            "• следуйте сообщениям региональных властей и экстренных служб;\n"
            "• при непосредственной угрозе звоните <b>112</b>;\n"
            "• не подходите к обломкам и подозрительным предметам;\n"
            "• не публикуйте фото и координаты работы ПВО;\n"
            "• установите официальное приложение МЧС для экстренных сообщений.\n\n"
            "Указания вашего региона всегда важнее общей памятки.",
            reply_markup=safety_keyboard(),
        )

    @router.message(F.text == BTN_SOURCES)
    async def sources(message: Message) -> None:
        await _register(db, message, admin_bot, admin_id)
        await message.answer(
            "<b>📡 Источники и проверка информации</b>\n\n"
            "<b>БПЛА Россия</b> — основной источник истории и уведомлений этого бота.\n"
            "<b>RadarMap</b> — неофициальная живая карта на основе публичной ленты Радар ВРВ.\n"
            "<b>Воздушная обстановка</b> — агрегатор нескольких публичных каналов с "
            "районами и статусом Крымского моста.\n"
            "<b>Радар ВРВ</b> — публичная лента, лежащая в основе части агрегаторов.\n\n"
            "Эти сервисы могут запаздывать, ошибаться и дублировать друг друга. "
            "Для действий используйте официальные оповещения региона и МЧС.",
            reply_markup=sources_keyboard(),
        )

    @router.message(Command("help"))
    async def help_message(message: Message) -> None:
        await _register(db, message, admin_bot, admin_id)
        await message.answer(
            "<b>Как работает бот</b>\n\n"
            "• получает свежие записи через открытый интерфейс bplarussia.ru;\n"
            "• присылает новые сообщения по выбранному региону;\n"
            "• показывает риск, общее число инцидентов, публикации о ПВО и последние записи;\n"
            "• строит общую сводку по России и позволяет выбирать уровень шума уведомлений.\n\n"
            "Бот не видит радары и не может достоверно назвать число аппаратов. "
            "Он пересказывает публикации источника, поэтому возможны задержки и ошибки.\n\n"
            "Команды: /region, /stats, /overview, /alerts, /delete_me.",
            reply_markup=main_keyboard(),
        )

    @router.message(Command("delete_me"))
    async def delete_me(message: Message) -> None:
        await message.answer(
            "Удалить выбранный регион, настройки и очередь уведомлений?",
            reply_markup=delete_confirmation_keyboard(),
        )

    @router.callback_query(F.data == "delete:no")
    async def cancel_delete(callback: CallbackQuery) -> None:
        await callback.message.edit_text("Удаление отменено.")
        await callback.answer()

    @router.callback_query(F.data == "delete:yes")
    async def confirm_delete(callback: CallbackQuery) -> None:
        await db.delete_user(callback.from_user.id)
        await callback.message.edit_text("Ваши данные и подписка удалены.")
        await callback.answer()

    @router.message(F.text & ~F.text.startswith("/"))
    async def region_search(message: Message) -> None:
        await _register(db, message, admin_bot, admin_id)
        query = (message.text or "").strip()
        if len(query) < 2:
            await message.answer("Напишите хотя бы две буквы названия региона.")
            return
        try:
            regions = await source.get_primary_regions()
        except SourceError:
            await message.answer("Список регионов временно недоступен. Попробуйте позже.")
            return
        matches = _search_regions(regions, query)
        if not matches:
            await message.answer(
                "Не нашёл такой регион. Попробуйте написать без сокращений или откройте весь список.",
                reply_markup=regions_keyboard(regions, 0),
            )
            return
        await message.answer(
            "Нашёл подходящие варианты:",
            reply_markup=region_search_keyboard(matches),
        )

    return router
