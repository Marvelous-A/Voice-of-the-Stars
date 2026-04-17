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
from datetime import datetime, timedelta
from os import getenv

from aiogram import Bot, Dispatcher, F
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.filters import CommandStart
from aiogram.types import (InlineKeyboardButton, InlineKeyboardMarkup,
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

session = AiohttpSession(proxy=PROXY_URL) if PROXY_URL else AiohttpSession()
bot = Bot(token=ADMIN_BOT_TOKEN, session=session)
dp = Dispatcher()

# Память: {admin_id: "user_query"} — ждём ввод ID/@username после кнопки «Найти пользователя»
PENDING_INPUT: dict[int, str] = {}

# Кэш username основного бота — получаем через getMe при старте
MAIN_BOT_USERNAME: str = ""

# ====== КНОПКИ =======
BTN_STATS = "📊 Статистика"
BTN_USERS = "👥 Все пользователи"
BTN_FIND = "🔍 Найти пользователя"
BTN_STATUS = "ℹ️ Статус"
BTN_REFRESH = "🔄 Обновить меню"


def get_admin_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=BTN_STATS)],
            [KeyboardButton(text=BTN_USERS), KeyboardButton(text=BTN_FIND)],
            [KeyboardButton(text=BTN_STATUS), KeyboardButton(text=BTN_REFRESH)],
        ],
        resize_keyboard=True,
    )


def is_admin(message: Message) -> bool:
    return message.from_user is not None and message.from_user.id == ADMIN_ID


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


def render_user_detail(query: str) -> str:
    query = query.strip().lstrip("@")
    users = load_users()

    found_uid = None
    found_data = None
    for uid, data in users.items():
        if uid == query or data.get("username", "").lower() == query.lower():
            found_uid = uid
            found_data = data
            break

    if not found_uid:
        return f"Пользователь «{query}» не найден.\nИщи по ID или @username."

    activity = found_data.get("activity", {})
    username = found_data.get("username", "")
    full_name = found_data.get("full_name", "")
    sign = found_data.get("sign", "не выбран")
    joined = found_data.get("joined_at", "")
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
        f"🆔 ID: `{found_uid}`\n"
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
        f"  Приглашено друзей: {found_data.get('referrals_total', 0)}\n"
        f"  Бонусных сеансов осталось: {found_data.get('bonus_sessions', 0)}\n"
        f"  Пришёл по ссылке от: {found_data.get('referred_by', '—')}"
    )


# ====== ХЭНДЛЕРЫ ======
@dp.message(CommandStart())
async def start(message: Message):
    if not is_admin(message):
        return
    await pin_links_message(message.chat.id)
    await message.answer(
        "🛠 *Админ-панель «Голос Звёзд»*\n\n"
        "Здесь только твои служебные уведомления и команды.\n"
        "Пользуйся кнопками ниже.",
        parse_mode="Markdown",
        reply_markup=get_admin_keyboard(),
    )


@dp.message(F.text == BTN_REFRESH)
async def handle_refresh(message: Message):
    if not is_admin(message):
        return
    PENDING_INPUT.pop(message.from_user.id, None)
    await message.answer("Меню обновлено 👇", reply_markup=get_admin_keyboard())


@dp.message(F.text == BTN_STATS)
async def handle_stats(message: Message):
    if not is_admin(message):
        return
    PENDING_INPUT.pop(message.from_user.id, None)
    await message.answer(render_stats(), parse_mode="Markdown")


@dp.message(F.text == BTN_USERS)
async def handle_users_list(message: Message):
    if not is_admin(message):
        return
    PENDING_INPUT.pop(message.from_user.id, None)
    for chunk in render_users_chunks():
        await message.answer(chunk)


@dp.message(F.text == BTN_FIND)
async def handle_find_prompt(message: Message):
    if not is_admin(message):
        return
    PENDING_INPUT[message.from_user.id] = "user_query"
    await message.answer("Введи ID или @username пользователя:")


@dp.message(F.text == BTN_STATUS)
async def handle_status(message: Message):
    if not is_admin(message):
        return
    PENDING_INPUT.pop(message.from_user.id, None)
    users = load_users()
    pending = len(load_pending_reviews())
    await message.answer(
        f"✅ Админ-бот работает.\n"
        f"Пользователей в базе: *{len(users)}*\n"
        f"Отзывов на модерации: *{pending}*\n"
        f"Время: {datetime.now().strftime('%d.%m.%Y %H:%M')}",
        parse_mode="Markdown",
    )


# Свободный ввод — используется только когда ждём запрос для «Найти пользователя».
@dp.message(F.text & ~F.text.startswith("/"))
async def handle_free_text(message: Message):
    if not is_admin(message):
        return
    pending = PENDING_INPUT.get(message.from_user.id)
    if pending == "user_query":
        PENDING_INPUT.pop(message.from_user.id, None)
        await message.answer(render_user_detail(message.text or ""), parse_mode="Markdown")
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
