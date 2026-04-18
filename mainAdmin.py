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
from datetime import datetime, timedelta
from os import getenv

from aiogram import Bot, Dispatcher, F
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.filters import CommandStart
from aiogram.types import (CallbackQuery, FSInputFile, InlineKeyboardButton,
                           InlineKeyboardMarkup, KeyboardButton, Message,
                           ReplyKeyboardMarkup)
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

session = AiohttpSession(proxy=PROXY_URL) if PROXY_URL else AiohttpSession()
bot = Bot(token=ADMIN_BOT_TOKEN, session=session)
dp = Dispatcher()

# Строгий фильтр: бот реагирует ТОЛЬКО на админа. Все остальные апдейты отбрасываются
# на уровне диспетчера — ни один хэндлер их не увидит.
dp.message.filter(F.from_user.id == ADMIN_ID)
dp.callback_query.filter(F.from_user.id == ADMIN_ID)

# Память: {admin_id: "user_query"} — ждём ввод ID/@username после кнопки «Найти пользователя»
PENDING_INPUT: dict[int, str] = {}

# {admin_id: review_id} — администратор редактирует текст отзыва
WAITING_REVIEW_EDIT: dict[int, str] = {}

# Кэш username основного бота — получаем через getMe при старте
MAIN_BOT_USERNAME: str = ""

# ====== КНОПКИ =======
BTN_STATS = "📊 Статистика"
BTN_USERS = "👥 Все пользователи"
BTN_FIND = "🔍 Найти пользователя"
BTN_REQUESTS = "📩 Запросы к специалистам"
BTN_PENDING = "⭐ Отзывы на модерации"
BTN_STATUS = "ℹ️ Статус"
BTN_REFRESH = "🔄 Обновить меню"


def get_admin_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=BTN_STATS)],
            [KeyboardButton(text=BTN_REQUESTS)],
            [KeyboardButton(text=BTN_USERS), KeyboardButton(text=BTN_FIND)],
            [KeyboardButton(text=BTN_PENDING)],
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
    stats = {"users": 0, "requests": 0, "pending_reviews": 0, "reviews": 0, "username": ""}
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
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="🗑 Удалить из базы", callback_data=f"deluser_ask_{uid}"),
    ]])


def delete_confirm_keyboard(uid: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="✅ Да, удалить",    callback_data=f"deluser_ok_{uid}"),
        InlineKeyboardButton(text="❌ Отмена",          callback_data=f"deluser_no_{uid}"),
    ]])


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
        f"  • опубликованных отзывов: {stats['reviews']}"
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
