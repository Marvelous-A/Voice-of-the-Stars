"""
Админ-бот «Голос Звёзд» — отдельный Telegram-бот для администратора.

Здесь живут:
  • все служебные уведомления (новая регистрация, перезапуск, ежедневный отчёт)
  • команды /users, /user, /stats — оформлены как кнопки, а не слэш-команды
  • быстрый доступ к статусу сервиса

Отправку уведомлений делает основной бот (main.py) через notify_admin(),
используя токен из переменной ADMIN_BOT_TOKEN.
"""

import asyncio
import json
import os
import re
from datetime import datetime, timedelta
from os import getenv

from aiogram import Bot, Dispatcher, F
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.filters import CommandStart
from aiogram.types import (BufferedInputFile, CallbackQuery, FSInputFile,
                           InlineKeyboardButton, InlineKeyboardMarkup,
                           KeyboardButton, Message, ReplyKeyboardMarkup)
from dotenv import load_dotenv

load_dotenv()

ADMIN_BOT_TOKEN = getenv("ADMIN_BOT_TOKEN", "")
ADMIN_ID = int(getenv("ADMIN_ID", "0"))
PROXY_URL = getenv("PROXY_URL", "")

# Для закреплённого сообщения со ссылками
MAIN_BOT_TOKEN = getenv("BOT_TOKEN", "")
CHANNEL_ID = getenv("CHANNEL_ID", "")  # напр. "@VoiceOfTheStarsInfo"

if not ADMIN_BOT_TOKEN:
    raise SystemExit("ADMIN_BOT_TOKEN не задан в .env — создайте бота в @BotFather и пропишите токен.")
if not ADMIN_ID:
    raise SystemExit("ADMIN_ID не задан в .env.")

USERS_FILE = "users.json"
REVIEWS_FILE = "reviews.json"
PENDING_REVIEWS_FILE = "pending_reviews.json"
CONSULTATION_REQUESTS_FILE = "consultation_requests.json"
TAROT_HISTORY_FILE = "tarot_history.json"
ASTRO_HISTORY_FILE = "astro_history.json"
WAITING_FEEDBACK_FILE = "waiting_feedback.json"

DIALOGS_PER_PAGE = 8
MESSAGES_PER_PAGE = 6
MSG_PREVIEW_MAX = 600
FEEDBACK_PER_PAGE = 6

session = AiohttpSession(proxy=PROXY_URL) if PROXY_URL else AiohttpSession()
bot = Bot(token=ADMIN_BOT_TOKEN, session=session)
dp = Dispatcher()

# Отдельный клиент к основному боту — им шлём сообщения пользователям от имени платформы
_main_bot_session = AiohttpSession(proxy=PROXY_URL) if PROXY_URL else AiohttpSession()
main_bot = Bot(token=MAIN_BOT_TOKEN, session=_main_bot_session) if MAIN_BOT_TOKEN else None

# Строгий фильтр: бот реагирует ТОЛЬКО на админа. Все остальные апдейты отбрасываются
# на уровне диспетчера — ни один хэндлер их не увидит.
dp.message.filter(F.from_user.id == ADMIN_ID)
dp.callback_query.filter(F.from_user.id == ADMIN_ID)

# Память: {admin_id: "user_query"} — ждём ввод ID/@username после кнопки «Найти пользователя»
PENDING_INPUT: dict[int, str] = {}

# {admin_id: review_id} — администратор редактирует текст отзыва
WAITING_REVIEW_EDIT: dict[int, str] = {}

# Состояние просмотра переписок: фильтр по пользователю и поисковый запрос
DIALOG_USER_SCOPE: dict[int, str] = {}
DIALOG_SEARCH_STATE: dict[int, str] = {}
# Кэш id специалиста -> имя (строится из consultation_requests.json)
_SPECIALIST_NAME_CACHE: dict[tuple[str, str], str] = {}

# Кэш username основного бота — получаем через getMe при старте
MAIN_BOT_USERNAME: str = ""

# ====== КНОПКИ =======
BTN_STATS = "📊 Статистика"
BTN_USERS = "👥 Все пользователи"
BTN_FIND = "🔍 Найти пользователя"
BTN_REQUESTS = "📩 Консультации"
BTN_DIALOGS = "💬 Переписки"
BTN_PENDING = "⭐ Отзывы на модерации"
BTN_FEEDBACK = "💌 Запросить отзыв"
BTN_STATUS = "ℹ️ Статус"
BTN_REFRESH = "🔄 Обновить меню"


def get_admin_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=BTN_STATS)],
            [KeyboardButton(text=BTN_REQUESTS), KeyboardButton(text=BTN_DIALOGS)],
            [KeyboardButton(text=BTN_USERS), KeyboardButton(text=BTN_FIND)],
            [KeyboardButton(text=BTN_PENDING), KeyboardButton(text=BTN_FEEDBACK)],
            [KeyboardButton(text=BTN_STATUS), KeyboardButton(text=BTN_REFRESH)],
        ],
        resize_keyboard=True,
    )


def build_links_markup() -> InlineKeyboardMarkup | None:
    """Инлайн-кнопки со ссылками на основного бота и канал."""
    rows = []
    if MAIN_BOT_USERNAME:
        rows.append([InlineKeyboardButton(
            text="🌟 Открыть основного бота",
            url=f"https://t.me/{MAIN_BOT_USERNAME}",
        )])
    if CHANNEL_ID:
        clean = CHANNEL_ID.lstrip("@")
        rows.append([InlineKeyboardButton(
            text="📢 Открыть канал",
            url=f"https://t.me/{clean}",
        )])
    return InlineKeyboardMarkup(inline_keyboard=rows) if rows else None


async def pin_links_message(chat_id: int):
    """Публикует сообщение со ссылками и закрепляет его (без уведомления)."""
    markup = build_links_markup()
    if not markup:
        return
    try:
        sent = await bot.send_message(
            chat_id,
            "🔗 *Быстрые ссылки*",
            parse_mode="Markdown",
            reply_markup=markup,
        )
        try:
            await bot.unpin_all_chat_messages(chat_id)
        except Exception:
            pass
        await bot.pin_chat_message(chat_id, sent.message_id, disable_notification=True)
    except Exception as e:
        print(f"[pin_links] {e}")


# ====== ЗАГРУЗКА ДАННЫХ ======
def load_users() -> dict:
    try:
        with open(USERS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def load_reviews() -> list:
    try:
        with open(REVIEWS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return []


def load_pending_reviews() -> dict:
    try:
        with open(PENDING_REVIEWS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def load_consultation_requests() -> list:
    try:
        with open(CONSULTATION_REQUESTS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return []


def save_reviews(reviews: list) -> None:
    with open(REVIEWS_FILE, "w", encoding="utf-8") as f:
        json.dump(reviews, f, ensure_ascii=False, indent=2)


def save_pending_reviews(pending: dict) -> None:
    with open(PENDING_REVIEWS_FILE, "w", encoding="utf-8") as f:
        json.dump(pending, f, ensure_ascii=False, indent=2)


def save_users(users: dict) -> None:
    with open(USERS_FILE, "w", encoding="utf-8") as f:
        json.dump(users, f, ensure_ascii=False, indent=2)


def save_consultation_requests(requests: list) -> None:
    with open(CONSULTATION_REQUESTS_FILE, "w", encoding="utf-8") as f:
        json.dump(requests, f, ensure_ascii=False, indent=2)


def _author_username(author: str) -> str:
    """Из строки `@username (Имя)` / `@username` достаёт голый username в нижнем регистре."""
    author = (author or "").strip()
    if not author.startswith("@"):
        return ""
    return author[1:].split(" ", 1)[0].lower()


def purge_user(uid: str) -> dict:
    """Удаляет пользователя и связанные с ним записи. Возвращает сводку по удалённому."""
    stats = {"users": 0, "requests": 0, "pending_reviews": 0, "reviews": 0,
             "tarot_dialogs": 0, "astro_dialogs": 0, "username": ""}
    users = load_users()
    user_data = users.pop(uid, None)
    if user_data is None:
        return stats
    save_users(users)
    stats["users"] = 1
    username = (user_data.get("username") or "").lstrip("@").lower()
    stats["username"] = username

    try:
        requests = load_consultation_requests()
        filtered = [r for r in requests if str(r.get("user_id", "")) != uid]
        removed = len(requests) - len(filtered)
        if removed:
            save_consultation_requests(filtered)
            stats["requests"] = removed
    except Exception as e:
        print(f"[purge_user] consultation_requests: {e}")

    try:
        tarot_h = load_tarot_history()
        if uid in tarot_h:
            stats["tarot_dialogs"] = len(tarot_h[uid])
            tarot_h.pop(uid, None)
            save_tarot_history(tarot_h)
    except Exception as e:
        print(f"[purge_user] tarot_history: {e}")

    try:
        astro_h = load_astro_history()
        if uid in astro_h:
            stats["astro_dialogs"] = len(astro_h[uid])
            astro_h.pop(uid, None)
            save_astro_history(astro_h)
    except Exception as e:
        print(f"[purge_user] astro_history: {e}")

    if username:
        try:
            pending = load_pending_reviews()
            to_remove = [rid for rid, r in pending.items()
                         if _author_username(r.get("author", "")) == username]
            for rid in to_remove:
                pending.pop(rid, None)
            if to_remove:
                save_pending_reviews(pending)
                stats["pending_reviews"] = len(to_remove)
        except Exception as e:
            print(f"[purge_user] pending_reviews: {e}")

        try:
            reviews = load_reviews()
            before = len(reviews)
            reviews = [r for r in reviews
                       if _author_username(r.get("author", "")) != username]
            removed = before - len(reviews)
            if removed:
                save_reviews(reviews)
                stats["reviews"] = removed
        except Exception as e:
            print(f"[purge_user] reviews: {e}")

    return stats


def publish_review(review_id: str) -> bool:
    """Переносит отзыв из pending в reviews.json. Возвращает True если успешно."""
    pending = load_pending_reviews()
    if review_id not in pending:
        return False
    review = pending.pop(review_id)
    save_pending_reviews(pending)
    reviews = load_reviews()
    reviews.append({
        "author": review["author"],
        "tag": review["tag"],
        "text": review["text"],
        "published_at": datetime.now().isoformat(),
    })
    save_reviews(reviews)
    return True


def moderation_keyboard(review_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Опубликовать", callback_data=f"radmin_ok_{review_id}"),
            InlineKeyboardButton(text="❌ Отклонить",   callback_data=f"radmin_no_{review_id}"),
        ],
        [InlineKeyboardButton(text="✏️ Редактировать", callback_data=f"radmin_edit_{review_id}")],
    ])


def render_pending_review(review_id: str, review: dict) -> str:
    return (
        f"⭐ *Отзыв на модерацию*\n\n"
        f"🆔 `{review_id}`\n"
        f"👤 *Автор:* {review['author']}\n"
        f"🏷 *Тема:* {review['tag']}\n\n"
        f"💬 *Текст:*\n{review['text']}"
    )


# ====== РЕНДЕРЫ ======
def render_stats() -> str:
    users = load_users()
    now = datetime.now()
    today = now.date()
    week_ago = now - timedelta(days=7)

    total_users = len(users)
    users_with_sign = sum(1 for u in users.values() if "sign" in u)

    new_today = sum(
        1 for u in users.values()
        if u.get("joined_at") and datetime.fromisoformat(u["joined_at"]).date() == today
    )
    new_week = sum(
        1 for u in users.values()
        if u.get("joined_at") and datetime.fromisoformat(u["joined_at"]) >= week_ago
    )

    active_today = sum(
        1 for u in users.values()
        if u.get("activity", {}).get("last_seen")
        and datetime.fromisoformat(u["activity"]["last_seen"]).date() == today
    )
    active_week = sum(
        1 for u in users.values()
        if u.get("activity", {}).get("last_seen")
        and datetime.fromisoformat(u["activity"]["last_seen"]) >= week_ago
    )

    totals = {"forecast": 0, "about_me": 0, "tarot": 0, "astro": 0, "review": 0}
    for u in users.values():
        for key in totals:
            totals[key] += u.get("activity", {}).get(key, 0)

    top = sorted(
        [(uid, u.get("activity", {}).get("total", 0)) for uid, u in users.items()],
        key=lambda x: x[1], reverse=True
    )[:5]
    top_lines = "\n".join(f"  `{uid}` — {cnt} действий" for uid, cnt in top if cnt > 0) or "  нет данных"

    published_reviews = len(load_reviews())
    pending_reviews = len(load_pending_reviews())

    total_referrals = sum(u.get("referrals_total", 0) for u in users.values())
    users_with_referrals = sum(1 for u in users.values() if u.get("referrals_total", 0) > 0)
    total_bonus_remaining = sum(u.get("bonus_sessions", 0) for u in users.values())
    came_by_referral = sum(1 for u in users.values() if u.get("referred_by"))

    return (
        f"📊 *Статистика бота «Голос Звёзд»*\n"
        f"_{now.strftime('%d.%m.%Y %H:%M')}_\n\n"
        f"👥 *Пользователи*\n"
        f"  Всего зарегистрировано: *{total_users}*\n"
        f"  Выбрали знак зодиака: *{users_with_sign}*\n"
        f"  Новых сегодня: *{new_today}*\n"
        f"  Новых за 7 дней: *{new_week}*\n\n"
        f"⚡ *Активность*\n"
        f"  Активны сегодня: *{active_today}*\n"
        f"  Активны за 7 дней: *{active_week}*\n\n"
        f"🔢 *Использование услуг (всего)*\n"
        f"  🔮 Прогнозы: *{totals['forecast']}*\n"
        f"  📖 Читать о себе: *{totals['about_me']}*\n"
        f"  🎴 Консультации таролога: *{totals['tarot']}*\n"
        f"  ⭐ Консультации астролога: *{totals['astro']}*\n"
        f"  ✍️ Отправили отзыв: *{totals['review']}*\n\n"
        f"🎁 *Реферальная система*\n"
        f"  Всего приглашений: *{total_referrals}*\n"
        f"  Пришли по реферальной ссылке: *{came_by_referral}*\n"
        f"  Пользователей-рефереров: *{users_with_referrals}*\n"
        f"  Неиспользованных бонусов: *{total_bonus_remaining}*\n\n"
        f"⭐ *Отзывы*\n"
        f"  Опубликовано: *{published_reviews}*\n"
        f"  Ждут модерации: *{pending_reviews}*\n\n"
        f"🏆 *Топ-5 активных пользователей*\n{top_lines}"
    )


def render_users_chunks() -> list[str]:
    users = load_users()
    if not users:
        return ["Пока нет зарегистрированных пользователей."]

    lines = []
    for uid, data in sorted(users.items(), key=lambda x: x[1].get("joined_at", ""), reverse=True):
        username = data.get("username")
        full_name = data.get("full_name", "")
        sign = data.get("sign", "—")
        joined = data.get("joined_at", "")
        joined_str = datetime.fromisoformat(joined).strftime("%d.%m.%Y") if joined else "—"
        total = data.get("activity", {}).get("total", 0)

        if username:
            user_label = f"@{username} [{uid}]"
            if full_name:
                user_label += f" ({full_name})"
        elif full_name:
            user_label = f"{full_name} [{uid}]"
        else:
            user_label = f"ID {uid}"

        lines.append(f"• {user_label} · {sign} · с {joined_str} · {total} действий")

    chunk_size = 50
    messages = []
    for i in range(0, len(lines), chunk_size):
        chunk = lines[i:i + chunk_size]
        header = (
            f"👥 Пользователи ({i + 1}–{min(i + chunk_size, len(lines))} из {len(lines)})\n\n"
            if i == 0 else ""
        )
        messages.append(header + "\n".join(chunk))
    return messages


def find_user(query: str) -> tuple[str | None, dict | None]:
    query = query.strip().lstrip("@")
    if not query:
        return None, None
    users = load_users()
    for uid, data in users.items():
        if uid == query or (data.get("username") or "").lower() == query.lower():
            return uid, data
    return None, None


def render_user_detail(uid: str, data: dict) -> str:
    activity = data.get("activity", {})
    username = data.get("username", "")
    full_name = data.get("full_name", "")
    sign = data.get("sign", "не выбран")
    joined = data.get("joined_at", "")
    joined_str = datetime.fromisoformat(joined).strftime("%d.%m.%Y %H:%M") if joined else "—"
    last_seen = activity.get("last_seen", "")
    last_seen_str = datetime.fromisoformat(last_seen).strftime("%d.%m.%Y %H:%M") if last_seen else "—"

    if username:
        user_label = f"@{username}"
        if full_name:
            user_label += f" ({full_name})"
    elif full_name:
        user_label = full_name
    else:
        user_label = "—"

    return (
        f"👤 *{user_label}*\n"
        f"🆔 ID: `{uid}`\n"
        f"♈ Знак: {sign}\n"
        f"📅 Зарегистрирован: {joined_str}\n"
        f"🕐 Последняя активность: {last_seen_str}\n\n"
        f"📊 *Использование услуг:*\n"
        f"  🔮 Прогнозы: {activity.get('forecast', 0)}\n"
        f"  📖 Читать о себе: {activity.get('about_me', 0)}\n"
        f"  🎴 Консультации таролога: {activity.get('tarot', 0)}\n"
        f"  ⭐ Консультации астролога: {activity.get('astro', 0)}\n"
        f"  ✍️ Отправил отзывов: {activity.get('review', 0)}\n"
        f"  📈 Всего действий: {activity.get('total', 0)}\n\n"
        f"🎁 *Реферальная система:*\n"
        f"  Приглашено друзей: {data.get('referrals_total', 0)}\n"
        f"  Бонусных сеансов осталось: {data.get('bonus_sessions', 0)}\n"
        f"  Пришёл по ссылке от: {data.get('referred_by', '—')}"
    )


def delete_user_keyboard(uid: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💬 Переписки пользователя", callback_data=f"dlg_usr:{uid}")],
        [InlineKeyboardButton(text="🗑 Удалить из базы",        callback_data=f"deluser_ask_{uid}")],
    ])


def delete_confirm_keyboard(uid: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="✅ Да, удалить",    callback_data=f"deluser_ok_{uid}"),
        InlineKeyboardButton(text="❌ Отмена",          callback_data=f"deluser_no_{uid}"),
    ]])


# ====== ПЕРЕПИСКИ: ЗАГРУЗКА И РЕНДЕРЫ ======
def load_tarot_history() -> dict:
    try:
        with open(TAROT_HISTORY_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def load_astro_history() -> dict:
    try:
        with open(ASTRO_HISTORY_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def save_tarot_history(data: dict) -> None:
    with open(TAROT_HISTORY_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)


def save_astro_history(data: dict) -> None:
    with open(ASTRO_HISTORY_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)


def specialist_display_name(spec_type: str, spec_id: str) -> str:
    global _SPECIALIST_NAME_CACHE
    if not _SPECIALIST_NAME_CACHE:
        m: dict[tuple[str, str], str] = {}
        for req in load_consultation_requests():
            t = req.get("type")
            sid = req.get("specialist_id")
            name = req.get("specialist_name")
            if t and sid and name:
                m[(t, sid)] = name
        _SPECIALIST_NAME_CACHE = m
    return _SPECIALIST_NAME_CACHE.get((spec_type, spec_id), spec_id)


def user_display_label(uid: str, users: dict | None = None) -> str:
    data = (users if users is not None else load_users()).get(uid) or {}
    username = data.get("username") or ""
    full_name = data.get("full_name") or ""
    if username:
        return f"@{username}"
    if full_name:
        return full_name
    return f"ID {uid}"


def build_flagged_set() -> set[tuple[str, str, str]]:
    """(type, user_id, specialist_id) пар, где хотя бы один запрос был помечен."""
    result: set[tuple[str, str, str]] = set()
    for req in load_consultation_requests():
        if req.get("is_flagged"):
            result.add((
                req.get("type", ""),
                str(req.get("user_id", "")),
                req.get("specialist_id", ""),
            ))
    return result


def collect_dialogs(
    filter_type: str = "all",
    user_id: str | None = None,
    search: str | None = None,
) -> list[dict]:
    """Собирает сводки по всем диалогам, отсортированные по последней активности."""
    tarot_h = load_tarot_history() if filter_type != "astro" else {}
    astro_h = load_astro_history() if filter_type != "tarot" else {}
    flagged = build_flagged_set()
    flag_only = filter_type == "flag"
    search_lower = (search or "").lower().strip()

    dialogs: list[dict] = []
    for source_type, source in (("tarot", tarot_h), ("astro", astro_h)):
        for uid, per_spec in source.items():
            if user_id is not None and uid != user_id:
                continue
            for spec_id, messages in per_spec.items():
                if not messages:
                    continue
                is_flag = (source_type, uid, spec_id) in flagged
                if flag_only and not is_flag:
                    continue
                if search_lower and not any(
                    search_lower in (m.get("text") or "").lower() for m in messages
                ):
                    continue
                last = messages[-1]
                dialogs.append({
                    "type": source_type,
                    "user_id": uid,
                    "spec_id": spec_id,
                    "count": len(messages),
                    "last_time": last.get("time", ""),
                    "last_text": last.get("text", ""),
                    "last_role": last.get("role", ""),
                    "is_flagged": is_flag,
                })
    dialogs.sort(key=lambda d: d["last_time"], reverse=True)
    return dialogs


def get_dialog_messages(dtype: str, user_id: str, spec_id: str) -> list[dict]:
    source = load_tarot_history() if dtype == "tarot" else load_astro_history()
    return source.get(user_id, {}).get(spec_id, [])


def delete_dialog(dtype: str, user_id: str, spec_id: str) -> bool:
    if dtype == "tarot":
        history = load_tarot_history()
        saver = save_tarot_history
    else:
        history = load_astro_history()
        saver = save_astro_history
    if user_id not in history or spec_id not in history.get(user_id, {}):
        return False
    history[user_id].pop(spec_id, None)
    if not history[user_id]:
        history.pop(user_id, None)
    saver(history)
    return True


FILTER_LABELS = {"all": "Все", "tarot": "🎴 Таро", "astro": "⭐ Астро", "flag": "⚠️ С флагом"}


def render_dialogs_list(
    dialogs: list[dict],
    page: int,
    filter_type: str,
    search: str | None,
    user_scope: str | None,
) -> str:
    per_page = DIALOGS_PER_PAGE
    total_pages = max(1, (len(dialogs) + per_page - 1) // per_page)
    page = max(0, min(page, total_pages - 1))
    start = page * per_page
    chunk = dialogs[start:start + per_page]

    if user_scope:
        title = f"💬 Переписки пользователя {user_display_label(user_scope)}"
    else:
        title = "💬 Переписки"
    header_lines = [title]
    meta = [f"фильтр: {FILTER_LABELS.get(filter_type, 'Все')}"]
    if search:
        meta.append(f"поиск: «{search}»")
    meta.append(f"всего: {len(dialogs)}")
    meta.append(f"стр. {page + 1}/{total_pages}")
    header_lines.append(" · ".join(meta))

    if not dialogs:
        return "\n".join(header_lines) + "\n\nПусто."

    users_cache = load_users()
    blocks = []
    for i, d in enumerate(chunk, start=start + 1):
        ulabel = user_display_label(d["user_id"], users_cache)
        sname = specialist_display_name(d["type"], d["spec_id"])
        type_icon = "🎴" if d["type"] == "tarot" else "⭐"
        try:
            when = datetime.fromisoformat(d["last_time"]).strftime("%d.%m %H:%M")
        except Exception:
            when = "—"
        preview = (d["last_text"] or "").replace("\n", " ").strip()
        if len(preview) > 80:
            preview = preview[:77] + "…"
        who_mark = "👤" if d["last_role"] == "user" else type_icon
        flag_mark = " ⚠️" if d.get("is_flagged") else ""
        blocks.append(
            f"{i}. {type_icon} {ulabel} → {sname}{flag_mark}\n"
            f"   {d['count']} сообщ. · {when}\n"
            f"   {who_mark} {preview}"
        )

    return "\n".join(header_lines) + "\n\n" + "\n\n".join(blocks)


def _cb_dialog_open(d: dict, page: int = 0) -> str:
    return f"dlg_open:{d['type']}:{d['user_id']}:{d['spec_id']}:{page}"


def dialogs_list_keyboard(
    dialogs: list[dict],
    page: int,
    filter_type: str,
    search: str | None,
    user_scope: str | None,
) -> InlineKeyboardMarkup:
    per_page = DIALOGS_PER_PAGE
    total_pages = max(1, (len(dialogs) + per_page - 1) // per_page)
    page = max(0, min(page, total_pages - 1))
    start = page * per_page
    chunk = dialogs[start:start + per_page]

    rows: list[list[InlineKeyboardButton]] = []
    for i, d in enumerate(chunk, start=start + 1):
        sname = specialist_display_name(d["type"], d["spec_id"])
        ulabel = user_display_label(d["user_id"])
        type_icon = "🎴" if d["type"] == "tarot" else "⭐"
        label = f"{i}. {type_icon} {ulabel[:14]} → {sname[:12]}"
        rows.append([InlineKeyboardButton(text=label, callback_data=_cb_dialog_open(d))])

    pag: list[InlineKeyboardButton] = []
    if page > 0:
        pag.append(InlineKeyboardButton(text="⬅ Назад", callback_data=f"dlg_list:{filter_type}:{page - 1}"))
    if page < total_pages - 1:
        pag.append(InlineKeyboardButton(text="Вперёд ➡", callback_data=f"dlg_list:{filter_type}:{page + 1}"))
    if pag:
        rows.append(pag)

    filter_row: list[InlineKeyboardButton] = []
    for ft, label in FILTER_LABELS.items():
        if ft == filter_type:
            continue
        filter_row.append(InlineKeyboardButton(text=label, callback_data=f"dlg_list:{ft}:0"))
    if filter_row:
        rows.append(filter_row)

    tail: list[InlineKeyboardButton] = []
    if search:
        tail.append(InlineKeyboardButton(text="✖ Сброс поиска", callback_data="dlg_search_reset"))
    else:
        tail.append(InlineKeyboardButton(text="🔍 Поиск", callback_data="dlg_search"))
    if user_scope:
        tail.append(InlineKeyboardButton(text="🔁 Все пользователи", callback_data="dlg_scope_reset"))
    rows.append(tail)

    return InlineKeyboardMarkup(inline_keyboard=rows)


def _split_message_segments(text: str) -> list[str]:
    """Разбивает сохранённый текст на фактические сообщения (ответы бота шлются через `|||`)."""
    text = (text or "").strip()
    if not text:
        return [""]
    if "|||" not in text:
        return [text]
    segments = [p.strip() for p in text.split("|||") if p.strip()]
    return segments or [text]


def render_dialog_page(dtype: str, user_id: str, spec_id: str, page: int) -> tuple[str, int, int]:
    messages = get_dialog_messages(dtype, user_id, spec_id)
    total = len(messages)
    per_page = MESSAGES_PER_PAGE
    total_pages = max(1, (total + per_page - 1) // per_page)
    page = max(0, min(page, total_pages - 1))
    start = page * per_page
    chunk = messages[start:start + per_page]

    ulabel = user_display_label(user_id)
    sname = specialist_display_name(dtype, spec_id)
    type_icon = "🎴" if dtype == "tarot" else "⭐"
    spec_label = f"{type_icon} {sname}"

    header = (
        f"💬 {ulabel} ↔ {spec_label}\n"
        f"ID {user_id} · Сообщений: {total} · Стр. {page + 1}/{total_pages}"
    )

    if not messages:
        return header + "\n\nДиалог пуст.", total_pages, total

    parts = [header]
    for m in chunk:
        role = m.get("role", "")
        try:
            when = datetime.fromisoformat(m.get("time", "")).strftime("%d.%m %H:%M")
        except Exception:
            when = ""
        who = f"👤 {ulabel}" if role == "user" else spec_label
        for seg in _split_message_segments(m.get("text") or ""):
            if len(seg) > MSG_PREVIEW_MAX:
                seg = seg[:MSG_PREVIEW_MAX] + "…"
            parts.append(f"———\n{who} · {when}\n{seg}")

    return "\n\n".join(parts), total_pages, total


def dialog_detail_keyboard(
    dtype: str, user_id: str, spec_id: str, page: int, total_pages: int,
) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    pag: list[InlineKeyboardButton] = []
    if page > 0:
        pag.append(InlineKeyboardButton(
            text="⬅ Старее",
            callback_data=f"dlg_open:{dtype}:{user_id}:{spec_id}:{page - 1}",
        ))
    if page < total_pages - 1:
        pag.append(InlineKeyboardButton(
            text="Новее ➡",
            callback_data=f"dlg_open:{dtype}:{user_id}:{spec_id}:{page + 1}",
        ))
    if pag:
        rows.append(pag)
    rows.append([InlineKeyboardButton(
        text="📎 Выгрузить .txt",
        callback_data=f"dlg_exp:{dtype}:{user_id}:{spec_id}",
    )])
    rows.append([InlineKeyboardButton(
        text="🗑 Удалить диалог",
        callback_data=f"dlg_delask:{dtype}:{user_id}:{spec_id}",
    )])
    rows.append([InlineKeyboardButton(text="⬅ К списку", callback_data="dlg_list:all:0")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


async def show_dialogs_list(
    target: Message, admin_id: int, filter_type: str, page: int, edit: bool = False,
) -> None:
    user_scope = DIALOG_USER_SCOPE.get(admin_id)
    search = DIALOG_SEARCH_STATE.get(admin_id)
    dialogs = collect_dialogs(filter_type=filter_type, user_id=user_scope, search=search)
    text = render_dialogs_list(dialogs, page, filter_type, search, user_scope)
    kb = dialogs_list_keyboard(dialogs, page, filter_type, search, user_scope)
    if edit:
        try:
            await target.edit_text(text, reply_markup=kb)
            return
        except Exception:
            pass
    await target.answer(text, reply_markup=kb)


def build_dialog_txt(dtype: str, user_id: str, spec_id: str) -> tuple[bytes, str] | None:
    messages = get_dialog_messages(dtype, user_id, spec_id)
    if not messages:
        return None
    ulabel = user_display_label(user_id)
    sname = specialist_display_name(dtype, spec_id)
    type_word = "Таролог" if dtype == "tarot" else "Астролог"
    lines = [
        f"Переписка: {ulabel} (ID {user_id}) <-> {sname} ({type_word})",
        f"Сообщений: {len(messages)}",
        f"Выгружено: {datetime.now().strftime('%d.%m.%Y %H:%M')}",
        "=" * 60,
        "",
    ]
    for m in messages:
        role = m.get("role", "")
        who = ulabel if role == "user" else sname
        try:
            when = datetime.fromisoformat(m.get("time", "")).strftime("%d.%m.%Y %H:%M")
        except Exception:
            when = m.get("time", "")
        for seg in _split_message_segments(m.get("text") or ""):
            lines.append(f"[{when}] {who}:")
            lines.append(seg.rstrip())
            lines.append("")
    content = "\n".join(lines).encode("utf-8")
    safe = re.sub(r"[^\w.-]", "_", f"{ulabel.lstrip('@')}_{sname}_{dtype}")
    return content, f"dialog_{safe}.txt"


# ====== ХЭНДЛЕРЫ ======
@dp.message(CommandStart())
async def start(message: Message):
    await pin_links_message(message.chat.id)
    await message.answer(
        "🛠 *Админ-панель «Голос Звёзд»*\n\n"
        "Здесь только твои служебные уведомления и команды.\n"
        "Пользуйся кнопками ниже.",
        parse_mode="Markdown",
        reply_markup=get_admin_keyboard(),
    )


def _reset_input_state(admin_id: int) -> None:
    PENDING_INPUT.pop(admin_id, None)
    WAITING_REVIEW_EDIT.pop(admin_id, None)


@dp.message(F.text == BTN_REFRESH)
async def handle_refresh(message: Message):
    _reset_input_state(message.from_user.id)
    await message.answer("Меню обновлено 👇", reply_markup=get_admin_keyboard())


@dp.message(F.text == BTN_STATS)
async def handle_stats(message: Message):
    _reset_input_state(message.from_user.id)
    await message.answer(render_stats(), parse_mode="Markdown")


@dp.message(F.text == BTN_USERS)
async def handle_users_list(message: Message):
    _reset_input_state(message.from_user.id)
    for chunk in render_users_chunks():
        await message.answer(chunk)


@dp.message(F.text == BTN_FIND)
async def handle_find_prompt(message: Message):
    WAITING_REVIEW_EDIT.pop(message.from_user.id, None)
    PENDING_INPUT[message.from_user.id] = "user_query"
    await message.answer("Введи ID или @username пользователя:")


@dp.message(F.text == BTN_STATUS)
async def handle_status(message: Message):
    _reset_input_state(message.from_user.id)
    users = load_users()
    pending = len(load_pending_reviews())
    requests_total = len(load_consultation_requests())
    await message.answer(
        f"✅ Админ-бот работает.\n"
        f"Пользователей в базе: *{len(users)}*\n"
        f"Запросов к специалистам: *{requests_total}*\n"
        f"Отзывов на модерации: *{pending}*\n"
        f"Время: {datetime.now().strftime('%d.%m.%Y %H:%M')}",
        parse_mode="Markdown",
    )


def render_request_card(req: dict) -> str:
    type_label = "🎴 Таролог" if req.get("type") == "tarot" else "⭐ Астролог"
    username = req.get("username") or ""
    full_name = req.get("full_name") or ""
    if username:
        user_label = f"@{username}"
        if full_name:
            user_label += f" ({full_name})"
    elif full_name:
        user_label = full_name
    else:
        user_label = f"ID {req.get('user_id', '—')}"
    voice_marker = " 🎤" if req.get("is_voice") else ""
    flagged_marker = " ⚠️" if req.get("is_flagged") else ""
    created = req.get("created_at", "")
    try:
        created_str = datetime.fromisoformat(created).strftime("%d.%m.%Y %H:%M")
    except Exception:
        created_str = created or "—"
    return (
        f"📩 Запрос {req.get('id', '')}{voice_marker}{flagged_marker}\n"
        f"👤 {user_label} [ID {req.get('user_id', '—')}]\n"
        f"{type_label}: {req.get('specialist_name', '—')}\n"
        f"🕐 {created_str}\n\n"
        f"💬 Текст:\n{req.get('text', '')}"
    )


@dp.message(F.text == BTN_DIALOGS)
async def handle_dialogs(message: Message):
    _reset_input_state(message.from_user.id)
    DIALOG_USER_SCOPE.pop(message.from_user.id, None)
    DIALOG_SEARCH_STATE.pop(message.from_user.id, None)
    await show_dialogs_list(message, message.from_user.id, filter_type="all", page=0)


@dp.message(F.text == BTN_REQUESTS)
async def handle_requests(message: Message):
    _reset_input_state(message.from_user.id)
    requests = load_consultation_requests()
    if not requests:
        await message.answer("📭 Запросов от пользователей пока нет.")
        return

    last = requests[-20:]
    await message.answer(
        f"📩 Последние запросы к специалистам ({len(last)} из {len(requests)}, новые — внизу):"
    )
    for req in last:
        await message.answer(render_request_card(req))
        voice_path = req.get("voice_path")
        if req.get("is_voice") and voice_path and os.path.exists(voice_path):
            try:
                await bot.send_voice(
                    chat_id=message.chat.id,
                    voice=FSInputFile(voice_path),
                    caption=f"🎤 Голосовое к запросу {req.get('id', '')}",
                )
            except Exception as e:
                print(f"[handle_requests] voice {req.get('id')}: {e}")
                await message.answer("⚠️ Не удалось проиграть голосовое сообщение.")


# ====== ЗАПРОС ОТЗЫВА У ПОЛЬЗОВАТЕЛЯ ======
def load_waiting_feedback() -> dict:
    try:
        with open(WAITING_FEEDBACK_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def save_waiting_feedback(data: dict) -> None:
    with open(WAITING_FEEDBACK_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def collect_feedback_targets() -> list[dict]:
    """Уникальные пары (пользователь, специалист) из consultation_requests.json,
    отсортированные от самой свежей консультации к старой."""
    seen: dict[tuple[str, str, str], dict] = {}
    for req in load_consultation_requests():
        uid = str(req.get("user_id", ""))
        stype = req.get("type", "")
        sid = req.get("specialist_id", "")
        if not uid or not stype or not sid:
            continue
        key = (uid, stype, sid)
        entry = seen.get(key)
        if entry is None or (req.get("created_at", "") > entry.get("created_at", "")):
            seen[key] = {
                "user_id": uid,
                "username": req.get("username") or "",
                "full_name": req.get("full_name") or "",
                "type": stype,
                "specialist_id": sid,
                "specialist_name": req.get("specialist_name") or sid,
                "created_at": req.get("created_at", ""),
            }
    targets = sorted(seen.values(), key=lambda e: e["created_at"], reverse=True)
    return targets


def _feedback_target_label(t: dict) -> str:
    username = t.get("username") or ""
    full_name = t.get("full_name") or ""
    if username:
        user = f"@{username}"
    elif full_name:
        user = full_name
    else:
        user = f"ID {t['user_id']}"
    icon = "🎴" if t["type"] == "tarot" else "⭐"
    return f"{user} · {icon} {t['specialist_name']}"


def build_feedback_message(specialist_type: str, specialist_name: str) -> str:
    if specialist_type == "tarot":
        role_word = "тарологом"
        question_role = "таролога"
    else:
        role_word = "астрологом"
        question_role = "астролога"
    return (
        "✨ Здравствуйте!\n\n"
        "С вами связывается администрация платформы «Голос Звёзд».\n\n"
        f"Некоторое время назад вы консультировались с нашим {role_word} — "
        f"{specialist_name}. Нам очень важно знать, как вы оцениваете этот опыт.\n\n"
        "Поделитесь, пожалуйста, впечатлениями прямо здесь, в этом чате:\n"
        f"• помог ли ответ {question_role} разобраться с вашим вопросом;\n"
        "• насколько комфортно вам было в сеансе;\n"
        "• что понравилось и что, возможно, хотелось бы улучшить.\n\n"
        "Пишите свободно — ваш ответ уйдёт напрямую администрации и поможет нам "
        "делать консультации ещё лучше. 💛"
    )


def render_feedback_list(targets: list[dict], page: int) -> str:
    total_pages = max(1, (len(targets) + FEEDBACK_PER_PAGE - 1) // FEEDBACK_PER_PAGE)
    page = max(0, min(page, total_pages - 1))
    start = page * FEEDBACK_PER_PAGE
    chunk = targets[start:start + FEEDBACK_PER_PAGE]
    if not targets:
        return "💌 Запросить отзыв\n\nПока нет пользователей, проходивших консультации."
    lines = [
        "💌 Запросить отзыв о консультации",
        f"всего: {len(targets)} · стр. {page + 1}/{total_pages}",
        "",
        "Выбери пользователя и специалиста ниже — основной бот отправит ему сообщение с просьбой поделиться впечатлениями.",
    ]
    for i, t in enumerate(chunk, start=start + 1):
        try:
            when = datetime.fromisoformat(t["created_at"]).strftime("%d.%m %H:%M")
        except Exception:
            when = "—"
        lines.append(f"{i}. {_feedback_target_label(t)} · {when}")
    return "\n".join(lines)


def feedback_list_keyboard(targets: list[dict], page: int) -> InlineKeyboardMarkup:
    total_pages = max(1, (len(targets) + FEEDBACK_PER_PAGE - 1) // FEEDBACK_PER_PAGE)
    page = max(0, min(page, total_pages - 1))
    start = page * FEEDBACK_PER_PAGE
    chunk = targets[start:start + FEEDBACK_PER_PAGE]
    rows: list[list[InlineKeyboardButton]] = []
    for i, t in enumerate(chunk, start=start + 1):
        label = f"{i}. {_feedback_target_label(t)}"
        if len(label) > 60:
            label = label[:57] + "…"
        rows.append([InlineKeyboardButton(
            text=label,
            callback_data=f"fb_pick:{t['user_id']}:{t['type']}:{t['specialist_id']}",
        )])
    pag: list[InlineKeyboardButton] = []
    if page > 0:
        pag.append(InlineKeyboardButton(text="⬅ Назад", callback_data=f"fb_list:{page - 1}"))
    if page < total_pages - 1:
        pag.append(InlineKeyboardButton(text="Вперёд ➡", callback_data=f"fb_list:{page + 1}"))
    if pag:
        rows.append(pag)
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _find_feedback_target(user_id: str, spec_type: str, spec_id: str) -> dict | None:
    for t in collect_feedback_targets():
        if (str(t["user_id"]) == str(user_id)
                and t["type"] == spec_type
                and t["specialist_id"] == spec_id):
            return t
    return None


@dp.message(F.text == BTN_FEEDBACK)
async def handle_feedback_list(message: Message):
    _reset_input_state(message.from_user.id)
    if main_bot is None:
        await message.answer(
            "⚠️ Не удалось инициализировать клиент основного бота — проверь BOT_TOKEN в .env."
        )
        return
    targets = collect_feedback_targets()
    await message.answer(
        render_feedback_list(targets, page=0),
        reply_markup=feedback_list_keyboard(targets, page=0),
    )


@dp.callback_query(F.data.startswith("fb_list:"))
async def cb_feedback_page(callback: CallbackQuery):
    try:
        page = int(callback.data.split(":", 1)[1])
    except (ValueError, IndexError):
        await callback.answer()
        return
    targets = collect_feedback_targets()
    try:
        await callback.message.edit_text(
            render_feedback_list(targets, page),
            reply_markup=feedback_list_keyboard(targets, page),
        )
    except Exception:
        pass
    await callback.answer()


@dp.callback_query(F.data.startswith("fb_pick:"))
async def cb_feedback_pick(callback: CallbackQuery):
    try:
        _, uid, stype, sid = callback.data.split(":", 3)
    except ValueError:
        await callback.answer()
        return
    target = _find_feedback_target(uid, stype, sid)
    if not target:
        await callback.answer("Консультация не найдена (возможно, была удалена).", show_alert=True)
        return
    preview = build_feedback_message(stype, target["specialist_name"])
    existing = load_waiting_feedback().get(uid)
    warn = ""
    if existing:
        try:
            sent = datetime.fromisoformat(existing.get("sent_at", "")).strftime("%d.%m %H:%M")
        except Exception:
            sent = existing.get("sent_at", "—")
        warn = (
            f"\n\n⚠️ Этому пользователю уже отправлен запрос {sent} "
            f"({existing.get('specialist_name', '—')}). "
            f"Новое сообщение перезапишет ожидание ответа."
        )
    kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(
            text="✅ Отправить",
            callback_data=f"fb_send:{uid}:{stype}:{sid}",
        ),
        InlineKeyboardButton(text="❌ Отмена", callback_data="fb_cancel"),
    ]])
    await callback.message.answer(
        f"✉️ Будет отправлено пользователю {_feedback_target_label(target)}:\n\n"
        f"— — —\n{preview}\n— — —{warn}",
        reply_markup=kb,
    )
    await callback.answer()


@dp.callback_query(F.data == "fb_cancel")
async def cb_feedback_cancel(callback: CallbackQuery):
    await callback.message.edit_text((callback.message.text or "") + "\n\n❌ Отправка отменена.")
    await callback.answer()


@dp.callback_query(F.data.startswith("fb_send:"))
async def cb_feedback_send(callback: CallbackQuery):
    try:
        _, uid, stype, sid = callback.data.split(":", 3)
    except ValueError:
        await callback.answer()
        return
    target = _find_feedback_target(uid, stype, sid)
    if not target:
        await callback.answer("Консультация не найдена.", show_alert=True)
        return
    if main_bot is None:
        await callback.answer("BOT_TOKEN не задан — нечем отправлять.", show_alert=True)
        return
    text = build_feedback_message(stype, target["specialist_name"])
    user_kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="✅ Завершить отзыв", callback_data="fb_done"),
        InlineKeyboardButton(text="⏰ Ответить позже", callback_data="fb_later"),
    ]])
    try:
        await main_bot.send_message(int(uid), text, reply_markup=user_kb)
    except Exception as e:
        await callback.message.edit_text(
            (callback.message.text or "") + f"\n\n⚠️ Не удалось отправить: {e}"
        )
        await callback.answer("Ошибка при отправке", show_alert=True)
        return
    data = load_waiting_feedback()
    data[str(uid)] = {
        "type": stype,
        "specialist_id": sid,
        "specialist_name": target["specialist_name"],
        "sent_at": datetime.now().isoformat(),
        "state": "active",
        "messages_count": 0,
    }
    save_waiting_feedback(data)
    await callback.message.edit_text(
        (callback.message.text or "") + "\n\n✅ Сообщение отправлено. Ответ пользователя придёт отдельным уведомлением в этот админ-бот."
    )
    await callback.answer("Отправлено")


@dp.message(F.text == BTN_PENDING)
async def handle_pending_reviews(message: Message):
    _reset_input_state(message.from_user.id)
    pending = load_pending_reviews()
    if not pending:
        await message.answer("🎉 Нет отзывов на модерации — всё разобрано.")
        return
    await message.answer(f"⭐ Отзывов ожидают модерации: *{len(pending)}*", parse_mode="Markdown")
    for rid, review in pending.items():
        await message.answer(
            render_pending_review(rid, review),
            parse_mode="Markdown",
            reply_markup=moderation_keyboard(rid),
        )


# ====== МОДЕРАЦИЯ ОТЗЫВОВ ======
@dp.callback_query(F.data.startswith("radmin_ok_"))
async def cb_approve_review(callback: CallbackQuery):
    review_id = callback.data.replace("radmin_ok_", "")
    if publish_review(review_id):
        await callback.message.edit_text(
            (callback.message.text or "") + "\n\n✅ Опубликован",
        )
    else:
        await callback.message.edit_text(
            (callback.message.text or "") + "\n\n⚠️ Отзыв не найден (уже обработан?)",
        )
    await callback.answer()


@dp.callback_query(F.data.startswith("radmin_no_"))
async def cb_reject_review(callback: CallbackQuery):
    review_id = callback.data.replace("radmin_no_", "")
    pending = load_pending_reviews()
    if review_id in pending:
        pending.pop(review_id)
        save_pending_reviews(pending)
        await callback.message.edit_text(
            (callback.message.text or "") + "\n\n❌ Отклонён",
        )
    else:
        await callback.message.edit_text(
            (callback.message.text or "") + "\n\n⚠️ Отзыв не найден (уже обработан?)",
        )
    await callback.answer()


@dp.callback_query(F.data.startswith("radmin_edit_cancel_"))
async def cb_edit_cancel(callback: CallbackQuery):
    WAITING_REVIEW_EDIT.pop(callback.from_user.id, None)
    await callback.message.answer("Редактирование отменено.")
    await callback.answer()


@dp.callback_query(F.data.startswith("radmin_edit_"))
async def cb_edit_review(callback: CallbackQuery):
    review_id = callback.data.replace("radmin_edit_", "")
    pending = load_pending_reviews()
    if review_id not in pending:
        await callback.answer("Отзыв не найден (уже обработан?).", show_alert=True)
        return
    WAITING_REVIEW_EDIT[callback.from_user.id] = review_id
    current_text = pending[review_id]["text"]
    await callback.message.answer(
        f"✏️ *Редактирование отзыва*\n\n"
        f"Текущий текст:\n_{current_text}_\n\n"
        f"Отправь новый текст отзыва. Автор и тема останутся без изменений.",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="❌ Отмена", callback_data=f"radmin_edit_cancel_{review_id}"),
        ]]),
    )
    await callback.answer()


# ====== ПЕРЕПИСКИ: ХЭНДЛЕРЫ ======
@dp.callback_query(F.data.startswith("dlg_list:"))
async def cb_dialogs_list(callback: CallbackQuery):
    try:
        _, ft, page_s = callback.data.split(":", 2)
        page = int(page_s)
    except (ValueError, IndexError):
        await callback.answer()
        return
    await show_dialogs_list(callback.message, callback.from_user.id, ft, page, edit=True)
    await callback.answer()


@dp.callback_query(F.data.startswith("dlg_usr:"))
async def cb_dialogs_user_scope(callback: CallbackQuery):
    try:
        _, uid = callback.data.split(":", 1)
    except ValueError:
        await callback.answer()
        return
    DIALOG_USER_SCOPE[callback.from_user.id] = uid
    DIALOG_SEARCH_STATE.pop(callback.from_user.id, None)
    await show_dialogs_list(callback.message, callback.from_user.id, "all", 0)
    await callback.answer()


@dp.callback_query(F.data == "dlg_scope_reset")
async def cb_dialogs_scope_reset(callback: CallbackQuery):
    DIALOG_USER_SCOPE.pop(callback.from_user.id, None)
    await show_dialogs_list(callback.message, callback.from_user.id, "all", 0, edit=True)
    await callback.answer()


@dp.callback_query(F.data == "dlg_search")
async def cb_dialogs_search_start(callback: CallbackQuery):
    PENDING_INPUT[callback.from_user.id] = "dialog_search"
    WAITING_REVIEW_EDIT.pop(callback.from_user.id, None)
    await callback.message.answer(
        "🔍 Пришли фразу или слово, которое нужно найти в текстах сообщений.\n"
        "Поиск нечувствителен к регистру. Пустое сообщение — отмена."
    )
    await callback.answer()


@dp.callback_query(F.data == "dlg_search_reset")
async def cb_dialogs_search_reset(callback: CallbackQuery):
    DIALOG_SEARCH_STATE.pop(callback.from_user.id, None)
    await show_dialogs_list(callback.message, callback.from_user.id, "all", 0, edit=True)
    await callback.answer()


@dp.callback_query(F.data.startswith("dlg_open:"))
async def cb_dialog_open(callback: CallbackQuery):
    try:
        _, dtype, uid, sid, page_s = callback.data.split(":", 4)
        page = int(page_s)
    except (ValueError, IndexError):
        await callback.answer()
        return
    text, total_pages, total = render_dialog_page(dtype, uid, sid, page)
    if total == 0:
        await callback.answer("Диалог пуст или удалён.", show_alert=True)
        return
    kb = dialog_detail_keyboard(dtype, uid, sid, page, total_pages)
    try:
        await callback.message.edit_text(text, reply_markup=kb)
    except Exception:
        await callback.message.answer(text, reply_markup=kb)
    await callback.answer()


@dp.callback_query(F.data.startswith("dlg_exp:"))
async def cb_dialog_export(callback: CallbackQuery):
    try:
        _, dtype, uid, sid = callback.data.split(":", 3)
    except ValueError:
        await callback.answer()
        return
    built = build_dialog_txt(dtype, uid, sid)
    if built is None:
        await callback.answer("Диалог пуст.", show_alert=True)
        return
    content, filename = built
    ulabel = user_display_label(uid)
    sname = specialist_display_name(dtype, sid)
    type_icon = "🎴" if dtype == "tarot" else "⭐"
    try:
        await callback.message.answer_document(
            BufferedInputFile(content, filename=filename),
            caption=f"📎 {ulabel} ↔ {type_icon} {sname}",
        )
        await callback.answer("Готово")
    except Exception as e:
        print(f"[cb_dialog_export] {e}")
        await callback.answer("Не удалось выгрузить.", show_alert=True)


@dp.callback_query(F.data.startswith("dlg_delask:"))
async def cb_dialog_del_ask(callback: CallbackQuery):
    try:
        _, dtype, uid, sid = callback.data.split(":", 3)
    except ValueError:
        await callback.answer()
        return
    ulabel = user_display_label(uid)
    sname = specialist_display_name(dtype, sid)
    type_icon = "🎴" if dtype == "tarot" else "⭐"
    kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="✅ Да, удалить", callback_data=f"dlg_delok:{dtype}:{uid}:{sid}"),
        InlineKeyboardButton(text="❌ Отмена",      callback_data=f"dlg_delno:{dtype}:{uid}:{sid}"),
    ]])
    await callback.message.answer(
        f"⚠️ Удалить переписку {ulabel} ↔ {type_icon} {sname} безвозвратно?",
        reply_markup=kb,
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("dlg_delno:"))
async def cb_dialog_del_cancel(callback: CallbackQuery):
    await callback.message.edit_text((callback.message.text or "") + "\n\n❌ Отменено")
    await callback.answer()


@dp.callback_query(F.data.startswith("dlg_delok:"))
async def cb_dialog_del_confirm(callback: CallbackQuery):
    try:
        _, dtype, uid, sid = callback.data.split(":", 3)
    except ValueError:
        await callback.answer()
        return
    if delete_dialog(dtype, uid, sid):
        await callback.message.edit_text((callback.message.text or "") + "\n\n🗑 Удалено")
    else:
        await callback.message.edit_text((callback.message.text or "") + "\n\n⚠️ Не найдено (уже удалено?)")
    await callback.answer()


# ====== УДАЛЕНИЕ ПОЛЬЗОВАТЕЛЯ ======
@dp.callback_query(F.data.startswith("deluser_ask_"))
async def cb_delete_user_ask(callback: CallbackQuery):
    uid = callback.data.replace("deluser_ask_", "")
    data = load_users().get(uid)
    if not data:
        await callback.answer("Пользователь не найден (уже удалён?).", show_alert=True)
        return
    username = data.get("username") or ""
    full_name = data.get("full_name") or ""
    if username:
        label = f"@{username}"
        if full_name:
            label += f" ({full_name})"
        label += f" [ID {uid}]"
    elif full_name:
        label = f"{full_name} [ID {uid}]"
    else:
        label = f"ID {uid}"
    await callback.message.answer(
        f"⚠️ *Удалить пользователя {label}?*\n\n"
        f"Будут стёрты безвозвратно:\n"
        f"  • запись в `users.json`\n"
        f"  • его запросы к специалистам\n"
        f"  • его отзывы на модерации и опубликованные",
        parse_mode="Markdown",
        reply_markup=delete_confirm_keyboard(uid),
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("deluser_no_"))
async def cb_delete_user_cancel(callback: CallbackQuery):
    await callback.message.edit_text(
        (callback.message.text or "") + "\n\n❌ Отменено",
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("deluser_ok_"))
async def cb_delete_user_confirm(callback: CallbackQuery):
    uid = callback.data.replace("deluser_ok_", "")
    stats = purge_user(uid)
    if stats["users"] == 0:
        await callback.message.edit_text(
            (callback.message.text or "") + "\n\n⚠️ Пользователь не найден (уже удалён?).",
        )
        await callback.answer()
        return
    summary = (
        "\n\n✅ *Удалено:*\n"
        f"  • записей пользователей: {stats['users']}\n"
        f"  • запросов специалистам: {stats['requests']}\n"
        f"  • отзывов на модерации: {stats['pending_reviews']}\n"
        f"  • опубликованных отзывов: {stats['reviews']}\n"
        f"  • переписок с тарологами: {stats.get('tarot_dialogs', 0)}\n"
        f"  • переписок с астрологами: {stats.get('astro_dialogs', 0)}"
    )
    await callback.message.edit_text(
        (callback.message.text or "") + summary,
        parse_mode="Markdown",
    )
    await callback.answer("Готово")


# Свободный ввод: ожидание запроса «Найти пользователя» или нового текста отзыва.
@dp.message(F.text & ~F.text.startswith("/"))
async def handle_free_text(message: Message):
    admin_id = message.from_user.id

    review_id = WAITING_REVIEW_EDIT.get(admin_id)
    if review_id:
        new_text = (message.text or "").strip()
        if len(new_text) < 10:
            await message.answer("Слишком короткий текст. Попробуй ещё раз.")
            return
        pending = load_pending_reviews()
        if review_id not in pending:
            WAITING_REVIEW_EDIT.pop(admin_id, None)
            await message.answer("Отзыв не найден — возможно, уже обработан.")
            return
        pending[review_id]["text"] = new_text
        save_pending_reviews(pending)
        WAITING_REVIEW_EDIT.pop(admin_id, None)
        await message.answer(
            f"✅ Текст обновлён. Новый вариант:\n\n_{new_text}_\n\nЧто делаем с отзывом?",
            parse_mode="Markdown",
            reply_markup=moderation_keyboard(review_id),
        )
        return

    pending = PENDING_INPUT.get(admin_id)
    if pending == "user_query":
        PENDING_INPUT.pop(admin_id, None)
        query = (message.text or "").strip()
        uid, data = find_user(query)
        if not uid:
            await message.answer(
                f"Пользователь «{query}» не найден.\nИщи по ID или @username."
            )
            return
        await message.answer(
            render_user_detail(uid, data),
            parse_mode="Markdown",
            reply_markup=delete_user_keyboard(uid),
        )
        return
    if pending == "dialog_search":
        PENDING_INPUT.pop(admin_id, None)
        query = (message.text or "").strip()
        if not query:
            await message.answer("Поиск отменён.")
            return
        DIALOG_SEARCH_STATE[admin_id] = query
        await show_dialogs_list(message, admin_id, "all", 0)
        return
    await message.answer("Выбери действие кнопкой 👇", reply_markup=get_admin_keyboard())


# ====== ЗАПУСК ======
async def fetch_main_bot_username() -> str:
    """Получает @username основного бота через его getMe."""
    if not MAIN_BOT_TOKEN:
        return ""
    tmp_session = AiohttpSession(proxy=PROXY_URL) if PROXY_URL else AiohttpSession()
    tmp_bot = Bot(token=MAIN_BOT_TOKEN, session=tmp_session)
    try:
        me = await tmp_bot.get_me()
        return me.username or ""
    except Exception as e:
        print(f"[fetch_main_bot_username] {e}")
        return ""
    finally:
        await tmp_bot.session.close()


async def main():
    global MAIN_BOT_USERNAME
    print("[mainAdmin] Админ-бот запускается…")
    MAIN_BOT_USERNAME = await fetch_main_bot_username()
    if MAIN_BOT_USERNAME:
        print(f"[mainAdmin] username основного бота: @{MAIN_BOT_USERNAME}")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
