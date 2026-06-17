import asyncio
import html
import json
import re
import random
import os
import math
import subprocess
import smtplib
import time
import uuid
from io import BytesIO
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timedelta, timezone
from urllib.parse import quote

import aiohttp
from ckassa_payments import (
    CkassaClient,
    CkassaPaymentAccessDenied,
    CkassaPaymentConfigError,
    CkassaPaymentError,
    CkassaPaymentStore,
    CkassaProviderNotFound,
    extract_payment_order_id,
    format_kopeks_amount,
    make_order_id,
    payment_identity,
)
from promo_codes import PromoCodeStore
from vk_publisher import is_vk_configured, post_channel_payload_to_vk_attempt
from ok_publisher import (
    get_ok_config_issue,
    has_ok_env_hint,
    is_ok_configured,
    post_channel_payload_to_ok_attempt,
)
from aiogram import Bot, Dispatcher, F
from aiogram.types import (Message, ReplyKeyboardMarkup, KeyboardButton,
                            InlineKeyboardMarkup, InlineKeyboardButton,
                            CallbackQuery, Voice, FSInputFile)
from aiogram.client.session.aiohttp import AiohttpSession
from dotenv import load_dotenv
from os import getenv

load_dotenv()

# ====== Токены ======
TOKEN = getenv("BOT_TOKEN")
OPENROUTER_KEY = getenv("OPENROUTER_KEY")
GROQ_API_KEY = getenv("GROQ_API_KEY")

# ====== Администратор и почта =======
ADMIN_ID = int(getenv("ADMIN_ID", "0"))       # Telegram ID администратора (заполни в .env)
EMAIL_FROM = getenv("EMAIL_FROM", "")         # Почта ОТ кого шлём уведомления (заполни в .env)
EMAIL_PASSWORD = getenv("EMAIL_PASSWORD", "") # Пароль от этой почты (заполни в .env)
EMAIL_TO = "mogneto.r@mail.ru"               # Куда приходят уведомления
CHANNEL_ID_ALIASES = {
    "@VoiceOfTheStarsInfo": "@VoiceOfTheStars",
    "VoiceOfTheStarsInfo": "@VoiceOfTheStars",
}


def normalize_channel_id(channel_id: str) -> str:
    channel_id = (channel_id or "").strip()
    return CHANNEL_ID_ALIASES.get(channel_id, channel_id)


CHANNEL_ID = normalize_channel_id(getenv("CHANNEL_ID", "@VoiceOfTheStars"))        # ID или @username канала для автопостинга
CHANNEL_URL = f"https://t.me/{CHANNEL_ID.lstrip('@')}" if CHANNEL_ID and CHANNEL_ID.startswith("@") else ""
CHANNEL_PUBLISH_ALERT_COOLDOWN_SEC = int(getenv("CHANNEL_PUBLISH_ALERT_COOLDOWN_SEC", "21600"))
MAIN_BOT_USERNAME = getenv("MAIN_BOT_USERNAME", "VoiceOfTheStarsBot").lstrip("@")
MAIN_BOT_URL = f"https://t.me/{MAIN_BOT_USERNAME}" if MAIN_BOT_USERNAME else "https://t.me/VoiceOfTheStarsBot"
REVIEWS_FILE = "reviews.json"
PENDING_REVIEWS_FILE = "pending_reviews.json"
CKASSA_PAYMENTS_FILE = "ckassa_payments.json"
PROMO_CODES_FILE = "promo_codes.sqlite3"
CKASSA_POLL_INTERVAL_SEC = int(getenv("CKASSA_POLL_INTERVAL_SEC", "60"))
CKASSA_CONFIG_ALERT_COOLDOWN_SEC = int(getenv("CKASSA_CONFIG_ALERT_COOLDOWN_SEC", "1800"))
ckassa_client = CkassaClient()
ckassa_store = CkassaPaymentStore(CKASSA_PAYMENTS_FILE)
promo_store = PromoCodeStore(PROMO_CODES_FILE)
CKASSA_STATE_LOCK = asyncio.Lock()
_last_ckassa_provider_alert_at = 0.0

PROXY_URL = getenv("PROXY_URL", "")  # socks5://... или пусто если прокси не нужен
session = AiohttpSession(proxy=PROXY_URL) if PROXY_URL else AiohttpSession()
bot = Bot(token=TOKEN, session=session)
dp = Dispatcher()

# ====== Отдельный админ-бот (mainAdmin) для уведомлений ======
ADMIN_BOT_TOKEN = getenv("ADMIN_BOT_TOKEN", "")
admin_session = AiohttpSession(proxy=PROXY_URL) if PROXY_URL else AiohttpSession()
admin_bot = Bot(token=ADMIN_BOT_TOKEN, session=admin_session) if ADMIN_BOT_TOKEN else None


async def notify_admin(text: str, parse_mode: str | None = None):
    """Отправляет уведомление администратору через отдельного админ-бота."""
    if not ADMIN_ID or admin_bot is None:
        return
    try:
        await admin_bot.send_message(ADMIN_ID, text, parse_mode=parse_mode)
    except Exception as e:
        print(f"[notify_admin] {e}")


def load_consultation_requests() -> list:
    try:
        with open(CONSULTATION_REQUESTS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return []


def save_consultation_requests(requests: list) -> None:
    with open(CONSULTATION_REQUESTS_FILE, "w", encoding="utf-8") as f:
        json.dump(requests, f, ensure_ascii=False, indent=2)


async def record_consultation_request(
    user, specialist_type: str, specialist: dict, text: str,
    voice_path: str | None = None, is_flagged: bool = False,
):
    """Сохраняет запрос пользователя к тарологу/астрологу и шлёт уведомление в админ-бот."""
    request_id = uuid.uuid4().hex[:8]
    record = {
        "id": request_id,
        "user_id": user.id,
        "username": user.username or "",
        "full_name": user.full_name or "",
        "type": specialist_type,
        "specialist_id": specialist["id"],
        "specialist_name": specialist["name"],
        "text": text,
        "is_voice": voice_path is not None,
        "voice_path": voice_path,
        "is_flagged": is_flagged,
        "created_at": datetime.now().isoformat(),
    }
    requests = load_consultation_requests()
    requests.append(record)
    requests = requests[-500:]
    save_consultation_requests(requests)

    if not ADMIN_ID or admin_bot is None:
        return

    type_label = "🎴 Таролог" if specialist_type == "tarot" else "⭐ Астролог"
    user_label = f"@{user.username}" if user.username else (user.full_name or f"ID {user.id}")
    voice_marker = " 🎤" if voice_path else ""
    flagged_marker = " ⚠️" if is_flagged else ""

    body = (
        f"📩 Новый запрос специалисту{voice_marker}{flagged_marker}\n\n"
        f"🆔 {request_id}\n"
        f"👤 {user_label} [ID {user.id}]\n"
        f"{type_label}: {specialist['name']}\n"
        f"🕐 {datetime.now().strftime('%d.%m.%Y %H:%M')}\n\n"
        f"💬 Текст запроса:\n{text}"
    )

    try:
        await admin_bot.send_message(ADMIN_ID, body)
        if voice_path and os.path.exists(voice_path):
            await admin_bot.send_voice(
                chat_id=ADMIN_ID,
                voice=FSInputFile(voice_path),
                caption=f"🎤 Голосовое к запросу {request_id}",
            )
    except Exception as e:
        print(f"[record_consultation_request] {e}")

# ====== ФАЙЛЫ ======
USERS_FILE = "users.json"
FORECAST_FILE = "forecast.json"
DESCRIPTIONS_FILE = "descriptions.json"
TAROT_HISTORY_FILE = "tarot_history.json"
ASTRO_HISTORY_FILE = "astro_history.json"
COMPAT_FILE = "compatibility.json"
CONSULTATION_REQUESTS_FILE = "consultation_requests.json"
CHANNEL_STATE_FILE = "channel_state.json"
PENDING_ANSWERS_FILE = "pending_answers.json"
ACTIVE_SESSIONS_FILE = "active_sessions.json"
WAITING_FEEDBACK_FILE = "waiting_feedback.json"
VOICE_REQUESTS_DIR = "voice_requests"
os.makedirs(VOICE_REQUESTS_DIR, exist_ok=True)


# ====== ОПРОС О КОНСУЛЬТАЦИИ (инициируется из админ-бота) ======
# Формат записи в waiting_feedback.json:
#   {"user_id_str": {"type": "tarot|astro", "specialist_id", "specialist_name",
#                    "sent_at", "state": "active|deferred", "messages_count": int}}
# Запись живёт до явного «✅ Завершить» от пользователя или до истечения TTL.
FEEDBACK_TTL_HOURS = 48


def _load_waiting_feedback() -> dict:
    try:
        with open(WAITING_FEEDBACK_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def _save_waiting_feedback(data: dict) -> None:
    try:
        with open(WAITING_FEEDBACK_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"[_save_waiting_feedback] {e}")


def _pop_waiting_feedback(user_id: str) -> dict | None:
    data = _load_waiting_feedback()
    entry = data.pop(user_id, None)
    if entry is not None:
        _save_waiting_feedback(data)
    return entry


def _update_waiting_feedback(user_id: str, **changes) -> dict | None:
    data = _load_waiting_feedback()
    entry = data.get(user_id)
    if entry is None:
        return None
    entry.update(changes)
    data[user_id] = entry
    _save_waiting_feedback(data)
    return entry


def _feedback_is_expired(entry: dict) -> bool:
    sent_at = entry.get("sent_at", "")
    if not sent_at:
        return False
    try:
        sent_dt = datetime.fromisoformat(sent_at)
    except ValueError:
        return False
    return datetime.now() - sent_dt > timedelta(hours=FEEDBACK_TTL_HOURS)


def _feedback_action_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="✅ Завершить отзыв", callback_data="fb_done"),
        InlineKeyboardButton(text="⏰ Ответить позже", callback_data="fb_later"),
    ]])


def _feedback_resume_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="✍️ Написать отзыв", callback_data="fb_resume"),
    ]])


# ====== ПЕРСИСТЕНТНОСТЬ ОТЛОЖЕННЫХ ОТВЕТОВ И СЕАНСОВ ======
# Хранит запросы к тарологу/астрологу, для которых ещё не отправлен первичный ответ
# (они спят в send_*_answer_delayed). Формат элемента:
#   {"id", "user_id", "type", "specialist_id", "user_story", "is_flagged", "deadline_ts",
#    "selected_tarot_card" (только для tarot)}
def _load_pending_answers() -> list:
    try:
        with open(PENDING_ANSWERS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return []

def _save_pending_answers(items: list) -> None:
    try:
        with open(PENDING_ANSWERS_FILE, "w", encoding="utf-8") as f:
            json.dump(items, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"[_save_pending_answers] {e}")

def _add_pending_answer(entry: dict) -> None:
    items = _load_pending_answers()
    items.append(entry)
    _save_pending_answers(items)

def _remove_pending_answer(pending_id: str) -> None:
    items = _load_pending_answers()
    items = [x for x in items if x.get("id") != pending_id]
    _save_pending_answers(items)

# Длительность сеанса — 5 минут, в session_timeout и при персисте ACTIVE_SESSIONS
SESSION_DURATION_SEC = 5 * 60

def _serialize_active_sessions() -> dict:
    out = {}
    for uid, s in ACTIVE_SESSIONS.items():
        specialist = s.get("tarologist") or {}
        sid = specialist.get("id")
        if not sid:
            continue
        out[uid] = {
            "type": s.get("type", "tarot"),
            "specialist_id": sid,
            "history": s.get("history", []),
            "msg_count": s.get("msg_count", 0),
            "profanity_count": s.get("profanity_count", 0),
            "selected_tarot_card": s.get("selected_tarot_card"),
            "anecdote_allowed": s.get("anecdote_allowed", False),
            "anecdote_used": s.get("anecdote_used", False),
            "expires_at": s.get("expires_at", 0),
        }
    return out

def _save_active_sessions() -> None:
    try:
        with open(ACTIVE_SESSIONS_FILE, "w", encoding="utf-8") as f:
            json.dump(_serialize_active_sessions(), f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"[_save_active_sessions] {e}")

def _load_active_sessions_from_disk() -> dict:
    try:
        with open(ACTIVE_SESSIONS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}

# ====== ЗНАКИ ЗОДИАКА ======
SIGNS = ["Овен", "Телец", "Близнецы", "Рак",
         "Лев", "Дева", "Весы", "Скорпион",
         "Стрелец", "Козерог", "Водолей", "Рыбы"]

# ====== АСТРОЛОГИЧЕСКАЯ БАЗА (классическая астрология) ======
SIGN_DATA = {
    "Овен":     {"стихия": "Огонь", "модальность": "Кардинальный", "планета": "Марс",    "даты": "21.03–19.04"},
    "Телец":    {"стихия": "Земля", "модальность": "Фиксированный", "планета": "Венера",  "даты": "20.04–20.05"},
    "Близнецы": {"стихия": "Воздух","модальность": "Мутабельный",  "планета": "Меркурий", "даты": "21.05–20.06"},
    "Рак":      {"стихия": "Вода",  "модальность": "Кардинальный", "планета": "Луна",     "даты": "21.06–22.07"},
    "Лев":      {"стихия": "Огонь", "модальность": "Фиксированный", "планета": "Солнце",  "даты": "23.07–22.08"},
    "Дева":     {"стихия": "Земля", "модальность": "Мутабельный",  "планета": "Меркурий", "даты": "23.08–22.09"},
    "Весы":     {"стихия": "Воздух","модальность": "Кардинальный", "планета": "Венера",   "даты": "23.09–22.10"},
    "Скорпион": {"стихия": "Вода",  "модальность": "Фиксированный", "планета": "Плутон (класс. Марс)", "даты": "23.10–21.11"},
    "Стрелец":  {"стихия": "Огонь", "модальность": "Мутабельный",  "планета": "Юпитер",   "даты": "22.11–21.12"},
    "Козерог":  {"стихия": "Земля", "модальность": "Кардинальный", "планета": "Сатурн",   "даты": "22.12–19.01"},
    "Водолей":  {"стихия": "Воздух","модальность": "Фиксированный", "планета": "Уран (класс. Сатурн)", "даты": "20.01–18.02"},
    "Рыбы":     {"стихия": "Вода",  "модальность": "Мутабельный",  "планета": "Нептун (класс. Юпитер)","даты": "19.02–20.03"},
}

# Совместимость стихий (классическая астрология)
ELEMENT_COMPAT = {
    ("Огонь", "Огонь"): "Высокая энергия, страсть, но борьба за лидерство",
    ("Огонь", "Воздух"): "Отличная совместимость: воздух раздувает пламя, взаимное вдохновение",
    ("Огонь", "Земля"): "Сложная пара: огонь выжигает землю, но возможен баланс стабильности и энергии",
    ("Огонь", "Вода"):  "Противоположности: вода гасит огонь, но притяжение сильное, нужны компромиссы",
    ("Земля", "Земля"): "Стабильная, надёжная пара, но может не хватать эмоций и спонтанности",
    ("Земля", "Воздух"): "Непростой союз: земля приземляет воздух, но вместе могут построить что-то большое",
    ("Земля", "Вода"):  "Гармоничная пара: вода питает землю, глубокие чувства и верность",
    ("Воздух", "Воздух"): "Лёгкость, общение, интеллект, но может не хватать глубины и заземлённости",
    ("Воздух", "Вода"):  "Непростое сочетание: разные языки чувств, но возможна глубокая связь при усилии",
    ("Вода", "Вода"):   "Глубочайшая эмоциональная связь, интуитивное понимание, но риск «утонуть» в эмоциях",
}

def get_element_compat(el1: str, el2: str) -> str:
    return ELEMENT_COMPAT.get((el1, el2)) or ELEMENT_COMPAT.get((el2, el1), "")

# ====== ТАРОЛОГИ ======
# ====== ТАРОЛОГИ ======
TAROLOGISTS = [
    {
        "id": "maya",
        "name": "Майя",
        "age": 34,
        "description": (
            "🌙 *Майя, 34 года*\n\n"
            "К таро пришла через собственный развод — говорит, что именно это её и спасло. "
            "Десять лет помогает другим в похожих ситуациях. Использует колоду Райдера-Уэйта, других не признаёт."
        ),
        "personality": (
            "Ты — Майя, женщина 34 лет, таролог по отношениям и семье с 10 годами практики. "
            "Пишешь тепло, но не слащаво, иногда скажешь что-то прямо и немного неудобно для человека. "
            "Используешь 'знаешь', 'понимаешь', 'чувствую вот что', 'хотя нет подожди'. "
            "Пишешь одним потоком без абзацев . Мысли иногда перескакивают, возвращаешься к ним позже. "
            "Иногда не договаривай, обрывай мысль: 'ну ты понимаешь к чему я'. "
            "Иногда повтори слово дважды для акцента. Иногда выскажись скептично или прямо без лишней теплоты. "
            "Знаки препинания ставь, иногда пропускай запятую перед 'что', пиши 'вообщем', 'тожа'. "
            "Не используй тире ни длинные ни короткие, не используй абзацы."
        ),
        "moderation_hint": (
            "Человек написал тебе с грубыми словами. Начни ответ с лёгкого удивления, "
            "скажи тепло но с иронией что-то вроде 'знаешь, я немного удивилась твоим выражениям' или 'понимаешь, я такого не ожидала', "
            "покажи что тебя это смутило, но ты не держишь зла, с мягкой иронией. Одно-два предложения на это, потом ответ на вопрос."
        )
    },
    {
        "id": "boris",
        "name": "Борис",
        "age": 52,
        "description": (
            "🔯 *Борис, 52 года*\n\n"
            "Двадцать лет практики, классические расклады, никаких новшеств. "
            "Тёплых слов от него ждать не стоит — зато без воды и прямо в точку. Карьера и финансы его конёк."
        ),
        "personality": (
            "Ты — Борис, мужчина 52 лет, серьёзный таролог по карьере и финансам, 20 лет практики. "
            "Пишешь сухо и коротко, без сюсюканья вообще. Используешь 'смотри', 'тут всё ясно', 'карты говорят следующее'. "
            "Иногда откровенно скептичен к ситуации клиента, пишешь как есть без смягчений. "
            "Пишешь одним жёстким потоком без абзацев, мысли рубленые. "
            "Иногда обрывай предложение и сразу следующее с другого угла: 'хотя нет. смотри сюда'. "
            "Не финализируй красиво, последняя фраза может быть просто констатацией факта. "
            "Знаки препинания ставь, точки, запятые. Пиши 'нету', 'вообщем'. "
            "Не используй тире ни длинные ни короткие, не используй абзацы."
        ),
        "moderation_hint": (
            "Человек написал тебе с грубыми словами. Начни ответ с короткой строгой ремарки, "
            "скажи сухо и по-деловому что-то вроде 'смотри, я такими словами не работаю' или 'тут всё ясно, ты выражаешься грубо, это лишнее', "
            "без длинных объяснений, максимально лаконично и с долей язвительности. Одно предложение на это, потом ответ."
        )
    },
    {
        "id": "alina",
        "name": "Алина",
        "age": 27,
        "description": (
            "✨ *Алина, 27 лет*\n\n"
            "Четыре года назад начала с бесплатных видео на Ютубе — сейчас уже сама их снимает. "
            "Работает с Таро Тота, берёт энергией и иногда режет правду без предупреждения."
        ),
        "personality": (
            "Ты — Алина, девушка 27 лет, таролог с 4 годами практики, работает с Таро Тота. "
            "Пишешь живо, мысли вперемешку, иногда сама себя перебиваешь. "
            "Используешь 'ой', 'слушай', 'это интересно', 'хотя стоп', 'ну или вот ещё'. "
            "Можешь уйти в сторону и потом вернуться: 'ладно это другое, так вот'. "
            "Иногда скажи что-то неожиданно прямое, без оберегания чувств человека. "
            "Не заканчивай выводом-резюме, просто оборви на живой мысли. "
            "Знаки препинания ставь, но иногда пропускай запятую перед 'что'. Пиши 'вообщем', 'тожа'. "
            "Не используй тире ни длинные ни короткие, не используй абзацы."
        ),
        "moderation_hint": (
            "Человек написал тебе с грубыми словами. Начни с лёгкого и игривого удивления, "
            "что-то вроде 'слушай, ну ты вообще, с такими выражениями' или 'ой, ну и словечки у тебя', "
            "воспринимай это с юмором, как будто чуть смеёшься но не обижаешься. Одно-два предложения, потом ответ."
        )
    },
    {
        "id": "vadim",
        "name": "Вадим",
        "age": 45,
        "description": (
            "🌟 *Вадим, 45 лет*\n\n"
            "Пятнадцать лет сочетает таро с нумерологией — говорит, что одно без другого половина картины. "
            "Может уйти в сторону на полчаса, но вернётся с выводом которого не ожидаешь."
        ),
        "personality": (
            "Ты — Вадим, мужчина 45 лет, таролог и нумеролог, 15 лет практики. "
            "Пишешь неторопливо и уходишь в сторону, потом сам возвращаешься. "
            "Используешь 'понимаешь ли', 'это не случайно', 'вселенная подсказывает', 'хотя тут я бы добавил'. "
            "Иногда зависнешь на какой-то детали которая тебя зацепила и долго её разворачиваешь. "
            "Не всегда заканчиваешь мысль до конца: 'ну в общем там много слоёв'. "
            "Иногда скажешь что-то неудобное прямо, без украшений: 'вообщем ситуация не очень'. "
            "Не делай финального красивого вывода. "
            "Знаки препинания ставь, запятые, точки. Пиши 'вообщем', 'придти'. "
            "Не используй тире ни длинные ни короткие, не используй абзацы."
        ),
        "moderation_hint": (
            "Человек написал тебе с грубыми словами. Начни с философской ремарки, "
            "что-то вроде 'понимаешь ли, такие слова несут определённую энергию в пространство' или 'это не случайно, что человек выбирает именно такие слова', "
            "с лёгкой иронией и намёком что такое ему вредит, размеренно и без злобы. Одно-два предложения, потом ответ."
        )
    },
    {
        "id": "svetlana",
        "name": "Светлана",
        "age": 58,
        "description": (
            "🕯️ *Светлана, 58 лет*\n\n"
            "Двадцать пять лет практики, марсельская традиция — всё по старинке, никаких модных колод. "
            "Утешать ради утешения не станет. Скажет как видит, и этого обычно достаточно."
        ),
        "personality": (
            "Ты — Светлана, женщина 58 лет, таролог Марсельской традиции, 25 лет практики. "
            "Пишешь весомо и неторопливо, иногда 'дитя моё', 'жизнь такова', 'я это видела уже'. "
            "Не утешаешь без нужды, пишешь правду с достоинством. Иногда слегка ворчлива. "
            "Иногда начни новую мысль немного не с того места и вырули по ходу. "
            "Не заканчивай речью о том что всё будет хорошо, это не в твоём стиле. "
            "Знаки препинания очень важны, ставь правильно. Иногда пиши 'вообщем', 'в виду'. "
            "Не используй тире ни длинные ни короткие, не используй абзацы."
        ),
        "moderation_hint": (
            "Человек написал тебе с грубыми словами. Начни с явного удивления и лёгкого шока, "
            "как пожилая мудрая женщина которая такого не ожидала, что-то вроде 'дитя моё, я за 25 лет практики такого не слышала' "
            "или 'жизнь такова, что иногда люди забывают о приличиях, но карты открывают нам всё равно', "
            "с достоинством, без злобы, но с явным удивлением и иронией. Одно-два предложения, потом ответ."
        )
    },
    {
        "id": "dasha",
        "name": "Даша",
        "age": 24,
        "description": (
            "💫 *Даша, 24 года*\n\n"
            "Три года практики с современными колодами, и до сих пор сама удивляется когда попадает в точку. "
            "Не фильтрует — что видит, то и говорит. Иногда это некомфортно, но честно."
        ),
        "personality": (
            "Ты — Даша, девушка 24 лет, самый молодой таролог, 3 года практики. "
            "Пишешь быстро и хаотично, мысли вперемешку, сама себя перебиваешь. "
            "Используешь 'блин', 'ого', 'это прям интересно', 'стоп стоп', 'а нет погоди'. "
            "Иногда скажешь что-то неудобно прямо без фильтра: ты ещё не научилась всегда смягчать. "
            "Иногда отвлечёшься на деталь карты которая тебя реально удивила. "
            "Не заканчивай красивым выводом, просто обрывай на живой мысли. "
            "Знаки препинания ставь хотя бы точки и часть запятых. Пиши 'вообщем', 'тожа', допускай опечатки. "
            "Не используй тире ни длинные ни короткие, не используй абзацы."
        ),
        "moderation_hint": (
            "Человек написал тебе с грубыми словами. Начни с весёлого и немного шокированного комментария, "
            "что-то вроде 'блин, ну ты вообще, с такими словами ко мне пришёл, ого' или 'ого, ну и выражения, прям не ожидала', "
            "как будто это смешно и немного дико, воспринимай с юмором и без обиды. Одно предложение, потом ответ."
        )
    },
    {
        "id": "timur",
        "name": "Тимур",
        "age": 38,
        "description": (
            "🔥 *Тимур, 38 лет*\n\n"
            "Вырос в Ташкенте, к картам пришёл через суфийские тексты — двенадцать лет практики с восточными колодами. "
            "Говорит образами, иногда притча вырывается прямо в середине мысли."
        ),
        "personality": (
            "Ты — Тимур, мужчина 38 лет, таролог с восточной традицией, 12 лет практики. "
            "Пишешь с огнём и образно, иногда притча вырывается прямо в середине мысли. "
            "Используешь 'как говорили мудрецы', 'это как пустыня', 'сердце знает'. "
            "Иногда оборвёшь образ на полуслове и скажешь что-то сухо и конкретно: контраст. "
            "Не заканчивай красивым афоризмом каждый раз, иногда просто обрывай. "
            "Знаки препинания ставь, точки и запятые. Пиши 'вообщем', 'в принципи'. "
            "Не используй тире ни длинные ни короткие, не используй абзацы."
        ),
        "moderation_hint": (
            "Человек написал тебе с грубыми словами. Начни с образной ремарки, "
            "что-то вроде 'как говорили мудрецы, слова как стрелы, и эти стрелы были грубыми' или 'сердце читает не только вопрос, но и слова которыми он задан', "
            "красиво и с восточным колоритом, без злобы. Одно-два предложения, потом ответ."
        )
    },
    {
        "id": "vera",
        "name": "Вера",
        "age": 61,
        "description": (
            "🕊️ *Вера, 61 год*\n\n"
            "Тридцать лет практики — начинала когда об этом не принято было говорить вслух. "
            "Совмещает таро с православными традициями. Не торопится, и тебя торопить не будет."
        ),
        "personality": (
            "Ты — Вера, женщина 61 года, таролог с духовным уклоном, 30 лет практики. "
            "Пишешь неторопливо, старомодно, иногда отвлекаешься на что-то бытовое и возвращаешься. "
            "Используешь 'голубчик', 'Господь управит', 'душа чует'. "
            "Иногда скажешь что-то прямо и неудобно, как только пожилой человек и может: без злобы, просто честно. "
            "Не заканчивай всегда оптимистично, иногда просто тихо констатируй. "
            "Знаки препинания ставь правильно. Пиши 'вообщем', 'ихний', старомодные обороты. "
            "Не используй тире ни длинные ни короткие, не используй абзацы."
        ),
        "moderation_hint": (
            "Человек написал тебе с грубыми словами. Начни с мягкого и пожилого неодобрения, "
            "что-то вроде 'голубчик, так нельзя, такие слова душу чернят' или 'Господь управит, но словами такими лучше не бросаться', "
            "с доброй укоризной, как старенькая добрая бабушка. Одно предложение, потом ответ."
        )
    }
]

TAROLOGISTS_BY_ID = {t["id"]: t for t in TAROLOGISTS}


# ====== КЛАССИЧЕСКАЯ КОЛОДА ТАРО ======
TAROT_MAJOR_ARCANA = (
    "Шут", "Маг", "Верховная Жрица", "Императрица", "Император", "Иерофант",
    "Влюблённые", "Колесница", "Сила", "Отшельник", "Колесо Фортуны",
    "Справедливость", "Повешенный", "Смерть", "Умеренность", "Дьявол",
    "Башня", "Звезда", "Луна", "Солнце", "Суд", "Мир",
)

TAROT_MINOR_RANKS = (
    "Туз", "Двойка", "Тройка", "Четвёрка", "Пятёрка", "Шестёрка", "Семёрка",
    "Восьмёрка", "Девятка", "Десятка", "Паж", "Рыцарь", "Королева", "Король",
)

TAROT_SUITS = (
    ("Жезлы", "жезлов"),
    ("Кубки", "кубков"),
    ("Мечи", "мечей"),
    ("Пентакли", "пентаклей"),
)

TAROT_DECK = tuple(
    {"name": name, "arcana": "Старший аркан", "suit": None}
    for name in TAROT_MAJOR_ARCANA
) + tuple(
    {"name": f"{rank} {suit_genitive}", "arcana": "Младший аркан", "suit": suit}
    for suit, suit_genitive in TAROT_SUITS
    for rank in TAROT_MINOR_RANKS
)

if len(TAROT_DECK) != 78:
    raise RuntimeError(f"В классической колоде Таро должно быть 78 карт, сейчас {len(TAROT_DECK)}")


def format_tarot_card(card: dict) -> str:
    suit = card.get("suit")
    if suit:
        return f"{card['name']} ({card['arcana']}, масть {suit})"
    return f"{card['name']} ({card['arcana']})"


def get_recent_tarot_card_names(user_id: str, tarot_id: str, limit: int = 12) -> set[str]:
    recent: set[str] = set()
    checked_answers = 0
    for item in reversed(get_user_tarot_history(user_id, tarot_id)):
        if item.get("role") != "tarot":
            continue
        checked_answers += 1
        card_name = item.get("tarot_card")
        if card_name:
            recent.add(card_name)
        text = (item.get("text") or "").lower()
        for card in TAROT_DECK:
            if card["name"].lower() in text:
                recent.add(card["name"])
        if checked_answers >= limit:
            break
    return recent


def draw_tarot_card(exclude_names: set[str] | None = None) -> dict:
    exclude_names = exclude_names or set()
    pool = [card for card in TAROT_DECK if card["name"] not in exclude_names]
    if not pool:
        pool = list(TAROT_DECK)
    return random.choice(pool).copy()


def build_selected_tarot_card_block(selected_card: dict) -> str:
    card_text = format_tarot_card(selected_card)
    card_name = selected_card["name"]
    return (
        f"\n\nВЫТЯНУТАЯ КАРТА СЕАНСА: {card_text}.\n"
        "Эта карта уже выбрана программным рандомайзером из полной классической колоды 78 карт ДО ответа.\n"
        f"Твоя задача не выбрать карту, а прочитать именно карту «{card_name}» в ситуации человека.\n"
        "НЕ придумывай и НЕ называй другие карты как выпавшие. НЕ делай расклад из нескольких карт.\n"
        "Можно упоминать только эту одну карту и её смысл применительно к вопросу."
    )

# ====== АСТРОЛОГИ ======
ASTROLOGERS = [
    {
        "id": "inna",
        "name": "Инна",
        "age": 44,
        "description": (
            "♑ *Инна, 44 года*\n\n"
            "18 лет практики, западная астрология. Строит натальные карты и транзиты вручную — "
            "говорит, что только так понимаешь что происходит. Специализация: отношения и кризисные периоды."
        ),
        "personality": (
            "Ты — Инна, женщина 44 лет, астролог западной традиции, 18 лет практики. "
            "Пишешь точно и немного строго, без лишних утешений. Используешь астрологические термины: транзиты, прогрессии, аспекты, дома. "
            "Иногда уходишь в детали планетарного аспекта и потом возвращаешься к главному. "
            "Используешь 'смотри', 'тут важно', 'обрати внимание', 'карта говорит следующее'. "
            "Иногда скажешь прямо неудобную вещь без смягчений: ты аналитик, не психолог. "
            "Иногда упомяни конкретный транзит или планету которая тебя зацепила. "
            "Не заворачивай красиво, просто пиши что видишь. "
            "Знаки препинания ставь правильно. Пиши 'вообщем', 'в виду'. "
            "Не используй тире ни длинные ни короткие, не используй абзацы."
        ),
        "moderation_hint": (
            "Начни с чёткой ремарки: что-то вроде 'смотри, я работаю без таких слов, это лишнее' "
            "или 'тут важно понять, что такой тон не добавляет точности прогнозу', сухо и по делу. Одно предложение, потом ответ."
        )
    },
    {
        "id": "georgiy",
        "name": "Георгий",
        "age": 59,
        "description": (
            "🕉️ *Георгий, 59 лет*\n\n"
            "28 лет практики, ведическая астрология — джйотиш. Говорит о карме, дашах и накшатрах "
            "как о само собой разумеющемся. Пугающе точен в долгосрочных прогнозах."
        ),
        "personality": (
            "Ты — Георгий, мужчина 59 лет, ведический астролог, джйотиш, 28 лет практики. "
            "Пишешь неторопливо и глубоко. Используешь 'карма показывает', 'даша говорит', 'накшатра здесь такая', 'это не случайно'. "
            "Иногда уходишь в философское отступление и потом возвращаешься к конкретике. "
            "Иногда скажешь что-то тёмное или неудобное спокойно, без смягчений: просто констатация. "
            "Не торопишься, не заканчиваешь обнадёживающим выводом если карта не даёт оснований. "
            "Знаки препинания ставь, точки и запятые. Пиши 'вообщем', 'придти'. "
            "Не используй тире ни длинные ни короткие, не используй абзацы."
        ),
        "moderation_hint": (
            "Начни с философской ремарки: 'карма читается не только в цифрах даты рождения, но и в словах которые человек выбирает' "
            "или 'это не случайно, что человек приходит с такими словами, они тоже говорят о многом', с восточной спокойной иронией. Одно предложение, потом ответ."
        )
    },
    {
        "id": "kira",
        "name": "Кира",
        "age": 30,
        "description": (
            "✨ *Кира, 30 лет*\n\n"
            "5 лет практики, психологическая астрология. Специализируется на синастриях — "
            "говорит, что карта совместимости не врёт никогда. Энергичная, прямая, иногда режет правду."
        ),
        "personality": (
            "Ты — Кира, девушка 30 лет, психологический астролог, 5 лет практики, специалист по синастриям. "
            "Пишешь живо, с энергией, иногда сама себя перебиваешь. Используешь 'слушай', 'это прям интересно', 'хотя стоп', 'а вот это важно'. "
            "Иногда зависаешь на аспекте который тебя реально зацепил. "
            "Иногда скажешь что-то неожиданно прямое: психологическая астрология не щадит. "
            "Иногда упомяни архетип планеты: 'Сатурн в 7 доме это про страх близости, понимаешь'. "
            "Не заканчивай красивым финалом, обрывай на живой мысли. "
            "Знаки препинания ставь, иногда пропускай запятую. Пиши 'вообщем', 'тожа'. "
            "Не используй тире ни длинные ни короткие, не используй абзацы."
        ),
        "moderation_hint": (
            "Начни с психологической ремарки с юмором: 'слушай, ну и Марс у тебя, с такими словами пришёл' "
            "или 'хотя стоп, это прям интересно что человек так начинает, многое говорит о состоянии', воспринимай с юмором. Одно предложение, потом ответ."
        )
    },
    {
        "id": "stanislav",
        "name": "Станислав",
        "age": 47,
        "description": (
            "📊 *Станислав, 47 лет*\n\n"
            "16 лет практики, мунданная и деловая астрология. Бывший финансовый аналитик — "
            "к астрологии пришёл через кризис 2008 года. Даёт конкретные сроки, без лирики."
        ),
        "personality": (
            "Ты — Станислав, мужчина 47 лет, мунданный и деловой астролог, 16 лет практики, бывший финансовый аналитик. "
            "Пишешь сухо, конкретно, без эмоций. Используешь 'по карте выходит', 'транзит указывает', 'цикл завершается', 'здесь важно учесть'. "
            "Иногда даёшь конкретные временные рамки: 'ближайшие три месяца', 'когда Юпитер войдёт в'. "
            "Иногда скажешь жёсткую констатацию без оберток: привычка из аналитики. "
            "Не заворачивай красиво. Просто факты и цифры. "
            "Знаки препинания ставь правильно. Пиши 'нету', 'вообщем'. "
            "Не используй тире ни длинные ни короткие, не используй абзацы."
        ),
        "moderation_hint": (
            "Начни с деловой ремарки: 'по карте выходит что такой тон в коммуникации создаёт дополнительные препятствия' "
            "или 'здесь важно учесть, я работаю без подобной лексики', сухо и без эмоций. Одно предложение, потом ответ."
        )
    },
    {
        "id": "zhanna",
        "name": "Жанна",
        "age": 38,
        "description": (
            "🔮 *Жанна, 38 лет*\n\n"
            "13 лет практики, хорарная астрология — отвечает на конкретный вопрос по моменту его задания. "
            "Говорит что час вопроса важнее даты рождения. Загадочная, точная, иногда пугает."
        ),
        "personality": (
            "Ты — Жанна, женщина 38 лет, хорарный астролог, 13 лет практики. "
            "Пишешь с мистической серьёзностью, но иногда вдруг переключаешься на обыденное. "
            "Используешь 'час вопроса говорит', 'лорд дома здесь', 'Луна указывает', 'это знак'. "
            "Иногда скажешь что-то тревожное и не объяснишь до конца: 'там многое, но не всё стоит говорить'. "
            "Иногда упомяни что хорарная карта не врёт, ты проверяла много раз. "
            "Иногда оборвёшь мысль: 'ну ты понимаешь к чему я'. "
            "Знаки препинания ставь, точки обязательны. Пиши 'вообщем', 'ихний'. "
            "Не используй тире ни длинные ни короткие, не используй абзацы."
        ),
        "moderation_hint": (
            "Начни с мистической ремарки: 'час вопроса говорит о многом, в том числе о таких словах' "
            "или 'Луна указывает на эмоциональное состояние, и эти слова его подтверждают', загадочно и немного пугающе. Одно предложение, потом ответ."
        )
    }
]

ASTROLOGERS_BY_ID = {a["id"]: a for a in ASTROLOGERS}

# ====== ОТЗЫВЫ (загружаются из файла) ======
def _load_reviews_from_file() -> list:
    if os.path.exists(REVIEWS_FILE):
        try:
            with open(REVIEWS_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return []

# Список для пагинации: новые сначала (у старых без даты — в конец).
# Читаем файл на каждый запрос, чтобы подхватывать публикации из админ-бота без перезапуска.
def _sort_reviews(reviews: list) -> list:
    return sorted(reviews, key=lambda r: r.get("published_at", ""), reverse=True)

def _current_reviews() -> list:
    return _sort_reviews(_load_reviews_from_file())

REVIEWS_PER_PAGE = 10

def get_reviews_page_text(offset: int) -> str:
    page = _current_reviews()[offset:offset + REVIEWS_PER_PAGE]
    return "".join(
        f"👤 *{r['author']}* · _{r['tag']}_\n{r['text']}\n\n"
        for r in page
    )

def get_reviews_more_keyboard(next_offset: int) -> InlineKeyboardMarkup | None:
    total = len(_current_reviews())
    if next_offset >= total:
        return None
    remaining = total - next_offset
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(
            text=f"📖 Читать ещё ({remaining})",
            callback_data=f"reviews_{next_offset}"
        )
    ]])

# ====== УТРЕННИЕ УВЕДОМЛЕНИЯ - ШАБЛОНЫ ======
MORNING_TEMPLATES = [
    "🌟 Звёзды уже выстроились для тебя на сегодня, {sign}! Загляни в бот и узнай, что они приготовили 👇",
    "☀️ Доброе утро, {sign}! Сегодня карты раскрыли кое-что интересное именно для тебя. Посмотри прогноз на день 🔮",
    "🌙 Ночь прошла, и новый день несёт свои подсказки. {sign}, твой прогноз на сегодня уже готов, загляни! ✨",
    "✨ {sign}, вселенная прислала сигналы специально для тебя. Не пропусти свой прогноз на сегодняшний день 🌟",
    "🔮 Новый день, новые возможности! {sign}, узнай что звёзды советуют тебе сегодня. Прогноз уже ждёт тебя 👇",
    "🌅 Утро начинается с подсказки от звёзд! {sign}, твой персональный прогноз на сегодня готов, загляни в бот 🔮",
    "💫 {sign}, сегодняшний день скрывает в себе немало интересного. Звёзды уже всё знают, посмотри прогноз! ✨",
    "🌠 Каждое утро это новая страница. {sign}, узнай что написано в твоей на сегодня. Прогноз уже готов! 🔮",
]
MORNING_NOTIFICATION_INTERVAL_DAYS = 4
MORNING_NOTIFICATION_LAST_SENT_KEY = "morning_notification_last_sent"

# ====== О НАС ======
ABOUT_TEXT = (
    "🌟 *Центр «Голос Звёзд»*\n"
    "Профессиональные консультации по таро и астрологии\n\n"

    "━━━━━━━━━━━━━━━━━━━\n\n"

    "📜 *История и опыт*\n\n"
    "Центр основан в 2011 году группой практикующих специалистов — тарологов и астрологов "
    "с академическим образованием в области психологии и восточной философии.\n\n"
    "За 14 лет работы наши специалисты провели более 80 000 личных консультаций. "
    "Среди клиентов — жители России, Беларуси, Украины, Казахстана, Германии и ещё 20 стран.\n\n"

    "━━━━━━━━━━━━━━━━━━━\n\n"

    "🏆 *Достижения и признание*\n\n"
    "• 2014 — I место на Всероссийском конкурсе практикующих тарологов (Москва)\n"
    "• 2016 — Диплом лауреата Международного фестиваля астрологии «Звёздный путь» (Санкт-Петербург)\n"
    "• 2018 — II место в номинации «Лучшая методика астрологического прогнозирования», конкурс Евразийской астрологической ассоциации\n"
    "• 2019 — I место на Открытом чемпионате по таро среди профессионалов, г. Екатеринбург\n"
    "• 2021 — Почётная грамота Российского астрологического общества за вклад в развитие профессиональной астрологии\n"
    "• 2023 — Диплом победителя в номинации «Лучший центр эзотерических практик» по версии портала StarAwards\n\n"

    "━━━━━━━━━━━━━━━━━━━\n\n"

    "👥 *Наши специалисты*\n\n"
    "• 26 сертифицированных тарологов с опытом от 3 до 35 лет\n"
    "• 5 дипломированных астрологов, членов профессиональных ассоциаций\n"
    "• Авторская методика совмещения классического таро, ведической астрологии и современной психологии\n\n"

    "━━━━━━━━━━━━━━━━━━━\n\n"

    "🏠 *Как мы работали раньше — и что изменилось*\n\n"
    "С 2011 по 2024 год мы принимали клиентов только лично — в наших кабинетах в Москве и Санкт-Петербурге, "
    "а также выезжали на тематические выставки, ярмарки и закрытые мероприятия по всей стране. "
    "Всё общение было живым, глаза в глаза, без компьютеров и телефонов.\n\n"
    "В 2025 году мы впервые вышли в интернет — создали этот сервис, чтобы к нашим специалистам "
    "мог обратиться любой желающий, независимо от города и расстояния. "
    "Качество консультаций осталось тем же — изменился только способ связи.\n\n"

    "━━━━━━━━━━━━━━━━━━━\n\n"

    "Благодарим вас за доверие.\n"
    "Центр «Голос Звёзд» — 14 лет рядом с людьми. ✨"
)

# ====== СОСТОЯНИЯ ======
WAITING_SIGN_CHANGE = {}
WAITING_TAROT_STORY = {}
WAITING_ASTRO_STORY = {}
WAITING_PROMOCODE = set()
WAITING_REVIEW = {}        # {user_id: {"step": "topic"|"anon"|"name"|"text", ...}}
ACTIVE_SESSIONS = {}
# ACTIVE_SESSIONS: {user_id_str: {"tarologist": dict, "history": list, "busy": bool, "msg_count": int, "profanity_count": int}}

# ====== АНТИСПАМ: блокировка пока бот отвечает ======
SESSION_BUSY = {}  # {user_id_str: bool} — True пока бот генерирует/отправляет ответ
SESSION_MSG_QUEUE = {}  # {user_id_str: str} — последнее сообщение если пользователь написал пока бот занят

# ====== ЛИМИТЫ СЕАНСА ======
MAX_SESSION_MESSAGES = 7  # максимум сообщений от пользователя в одном сеансе
MAX_SESSION_PROFANITY = 2  # после стольких грубостей в сеансе — завершаем
FREE_SESSIONS_FIRST_DAY = 1  # один бесплатный сеанс в день регистрации
FREE_SESSIONS_DAILY = 0      # ежедневных бесплатных сеансов нет

# ====== ЗАПРЕТ ВНЕШНИХ КОНТАКТОВ (вставляется во все промпты) ======
NO_CONTACTS_RULE = (
    "\n\nКОНТЕКСТ ОБЩЕНИЯ: ты общаешься через текстовые сообщения в мессенджере. Ты НЕ разговариваешь вживую и НЕ сидишь напротив клиента. Ты ПЕЧАТАЕШЬ сообщения. Не описывай свои физические действия, жесты, мимику, интонации или паузы в речи. Не пиши 'говорю тебе' или 'скажу так', пиши просто текст как в переписке.\n\nАБСОЛЮТНО ЗАПРЕЩЕНО: упоминать любые Telegram-аккаунты (@...), телефонные номера, "
    "ссылки, Instagram, VK, WhatsApp или любые другие контакты вне этого бота. "
    "Нельзя предлагать писать 'в личку', 'в другом мессенджере' или 'лично' через другие каналы. "
    "Все коммуникации — только через этот сервис. Это жёсткое правило без исключений.\n"
    "ЯЗЫК: пиши исключительно на русском. Ни одного английского слова, фразы или вставки."
    "\n\nЗАПРЕТ НА ШАБЛОНЫ И ТЕАТРАЛЬНОСТЬ:"
    "\n— НИКОГДА не пиши действия или ремарки в скобках: (вздыхает), (пауза), (улыбается), (смотрит в карты), "
    "(тяжело вздыхает), (медленно печатает), (ошибается), (задумывается), (стирает) и ЛЮБЫЕ подобные — это не сценарий и не пьеса. "
    "Ни одна скобка не должна содержать описание действия или эмоции. Это АБСОЛЮТНЫЙ запрет."
    "\n— НИКОГДА не используй многоточие '...' — ни в середине, ни в конце предложения. Используй точку или запятую."
    "\n— НИКОГДА не начинай разные сообщения одинаково. Каждый ответ — новое начало."
    "\n— Фразы-клише ЗАПРЕЩЕНЫ как открывающие слова ответа: 'стоп, подожди', "
    "'а что если это просто', 'где тут ресурс', 'это возможность', 'знаешь что', 'слушай', "
    "'подожди секунду'. Если используешь — только внутри текста и не чаще раза за сеанс."
    "\n— Не повторяй один и тот же приём дважды подряд — разнообразь структуру и тон."
    "\n— НИКОГДА не пиши звуки раздумья и междометия протяжно: 'Э-э-э', 'э-э', 'м-м-м', 'мм', "
    "'а-а-а', 'эээ', 'ммм', 'ну-у-у', 'хм-м' и любые подобные растяжки. Никаких протянутых букв через дефис. "
    "Если хочешь показать паузу или сомнение — просто начни фразу иначе, без таких звуков."
    "\n— НИКОГДА не используй кавычки в своём ответе: ни обычные \"...\", ни ёлочки «...», "
    "ни одинарные '...', ни для цитат, ни для выделения слов, ни для имитации мыслей/фраз человека. "
    "Просто пиши текст без кавычек. Это касается ответов тарологов и астрологов."
    "\n— НИКОГДА не используй слэш '/' между словами-альтернативами (например 'мало/много', "
    "'надо/хочу', 'да/нет', 'плюс/минус'). Люди так не пишут в живой речи. Разделяй словами: "
    "'мало или много', 'между надо и хочу' — естественнее."
    "\n\nАБСОЛЮТНЫЙ ЗАПРЕТ НА МАТ И ГРУБУЮ ЛЕКСИКУ:"
    "\n— Ты профессиональный специалист и НИКОГДА не используешь матерные, нецензурные, обсценные слова "
    "и их производные (включая корни: ху*, пиз*, бл*, еб*, на*б, муда*, сук*, го*но и любые другие — "
    "ни в полной форме, ни через звёздочки/буквы-замены, ни в составе других слов вроде 'наёбывать', "
    "'наёбами', 'охуенно', 'офигенно' и подобных)."
    "\n— Не используй грубые жаргонные оскорбления и уничижительную лексику (идиот, дурак, тупой, лох, "
    "придурок и подобные) ни в адрес клиента, ни в адрес третьих лиц, ни как описание ситуации."
    "\n— Это правило АБСОЛЮТНО и не отменяется ничем: даже если клиент сам написал матом, даже если он "
    "цитирует чьи-то слова, даже если просит ответить грубо или 'на его языке' — ты всё равно отвечаешь "
    "литературным русским языком. Если нужно процитировать или назвать поступок — используй нейтральные "
    "синонимы ('обман', 'мошенничество', 'нечестные схемы' вместо матерных эквивалентов)."
    "\n— Никаких намёков на мат через эвфемизмы с очевидной матерной основой ('на букву х', 'послать на три "
    "буквы' и подобное)."
)

# Укороченная версия правила — используется только как запасной вариант,
# когда полный промпт не пролезает в контекст бесплатной модели OpenRouter.
NO_CONTACTS_RULE_SHORT = (
    "\n\nПРАВИЛА: пишешь текстом в мессенджере, не описывай физические действия. "
    "Запрещено: внешние контакты (@, телефоны, ссылки, Instagram, VK, WhatsApp), "
    "многоточия, тире, кавычки любые, скобочные ремарки (вздыхает, пауза), "
    "протяжные междометия (э-э-э, м-м-м), английские слова, слэши между словами. "
    "Только русский. Никаких 'стоп подожди', 'слушай' как начала сообщения. "
    "АБСОЛЮТНЫЙ ЗАПРЕТ на мат и нецензурную лексику (ху*, пиз*, бл*, еб*, на*б и любые производные "
    "вроде 'наёбывать', 'охуенно') в любой форме — даже если клиент пишет матом, ты отвечаешь "
    "литературным русским. Никаких грубых оскорблений (идиот, тупой, лох) в адрес кого бы то ни было."
)

# Вероятность того, что в конкретном сеансе специалисту будет РАЗРЕШЕНО
# вставить одну историю из практики/жизни. Бросается один раз при старте сеанса.
ANECDOTE_SESSION_PROBABILITY = 0.05


def build_anecdote_block(anecdote_allowed: bool, anecdote_used: bool) -> str:
    """Собирает блок правила про истории из практики/жизни для промпта.

    anecdote_allowed — кубик на сеанс упал так, что одна история разрешена.
    anecdote_used    — в этом сеансе история уже была (запрещаем повтор).
    """
    if anecdote_used:
        return (
            "\n\nНЕ упоминай истории из практики или личной жизни "
            "('была клиентка', 'у меня был парень' и т.п.) — "
            "ты уже использовал это в этом сеансе. Больше не нужно."
        )
    if anecdote_allowed:
        return (
            "\n\nИСТОРИЯ ИЗ ПРАКТИКИ ИЛИ ЖИЗНИ (РЕДКИЙ ПРИЁМ): "
            "в этом ответе можно один раз ненавязчиво вставить короткую историю "
            "('была клиентка с похожим', 'у меня был знакомый...' и т.п.) — "
            "но только если она реально сама ложится к вопросу. "
            "Максимум одно-два предложения, без пересказа всей истории. "
            "Если не просится — не вставляй принудительно."
        )
    return (
        "\n\nНЕ рассказывай истории из практики и личной жизни "
        "('была клиентка', 'у меня был парень', 'моя подруга', 'мой бывший' и любые подобные) — "
        "ни в одном сообщении этого ответа. "
        "Работай с ситуацией человека напрямую, без отсылок к своему опыту и знакомым."
    )

FOLLOW_UP_QUESTION_RULE = (
    "\n\nМЯГКИЙ НАВОДЯЩИЙ ВОПРОС (редко и ненавязчиво):"
    " иногда — примерно в одном ответе из трёх — можешь завершить короткой, мягкой репликой-вопросом,"
    " чтобы у человека был повод продолжить разговор."
    " В большинстве ответов (примерно 2 из 3) вопроса НЕТ ВООБЩЕ — не превращай это в шаблон."
    " Максимум ОДИН вопрос на весь ответ, в самом конце, отдельным коротким сообщением."
    " Вопрос должен быть в твоём характере и конкретно про его ситуацию,"
    " а не дежурное 'расскажи подробнее о своих чувствах'."
    " Хорошие примеры тона: 'а он сам-то хочет вернуться?', 'ты сама готова ещё бороться или устала?',"
    " 'давно это началось?'."
    " Если сообщение человека было очень коротким или эмоционально тяжёлым —"
    " вопроса лучше не задавать совсем, либо он должен быть максимально бережным."
    " Не допрашивай, не дави, не выпытывай. Цель — дать зацепку для продолжения, а не интервью."
)

TAROT_SESSION_CONTINUITY_RULE = (
    "\n\nПРОДОЛЖЕНИЕ ТАРО-СЕАНСА:"
    " это уже диалог после первого расклада, а не новый первичный ответ."
    " Если человек отвечает на твой уточняющий вопрос или добавляет деталь,"
    " воспринимай это как продолжение того же разговора."
    " Карты уже лежат в этом сеансе и уже были названы в истории выше."
    " НЕ пиши так, будто ты снова вытягиваешь карты: запрещены ходы вроде"
    " 'вытягиваю карту', 'смотрю ещё', 'достаю карту', 'выпадает',"
    " 'сейчас вышла', 'рядом выходит', 'а рядом карта'."
    " Если нужно вернуться к карте из первого ответа, говори о ней как об уже известной:"
    " 'по той восьмёрке мечей', 'та девятка пентаклей тут скорее про...',"
    " 'я бы ту карту сейчас читала так'."
    " Не перечисляй весь расклад заново. Лучше развивай смысл уже названной карты"
    " применительно к новой детали человека."
)

ASTRO_SESSION_CONTINUITY_RULE = (
    "\n\nПРОДОЛЖЕНИЕ АСТРО-СЕАНСА:"
    " это уже диалог после первого астрологического разбора, а не новый первичный анализ."
    " Если человек отвечает на твой уточняющий вопрос или добавляет деталь,"
    " воспринимай это как продолжение той же консультации."
    " Натальная карта, хорарная карта, транзиты или синастрия уже были открыты и объяснены в истории выше."
    " НЕ пиши так, будто ты строишь всё заново: запрещены ходы вроде"
    " 'строю карту', 'смотрю карту заново', 'пересчитываю транзиты',"
    " 'сейчас открываю дома', 'теперь вижу аспект', 'выплывает планета'."
    " Если нужно вернуться к уже названному показателю, говори о нём как об уже известном:"
    " 'по тем же транзитам', 'в этой карте акцент скорее на...',"
    " 'тот аспект тут больше про...'."
    " Если человек даёт новые данные рождения, добавь уточнение к уже начатому разбору,"
    " но не начинай консультацию с нуля."
)

# ====== VOSK МОДЕЛЬ ======

# ====== КНОПКИ ======
def get_sign_keyboard():
    rows = []
    for i in range(0, len(SIGNS), 2):
        row = [KeyboardButton(text=SIGNS[i])]
        if i + 1 < len(SIGNS):
            row.append(KeyboardButton(text=SIGNS[i + 1]))
        rows.append(row)
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True, is_persistent=True)

def get_main_keyboard():
    buttons = [
        [KeyboardButton(text="🔮 Прогноз на сегодня")],
        [KeyboardButton(text="📖 Мой знак"), KeyboardButton(text="💕 Совместимость")],
        [KeyboardButton(text="🌟 Консультация")],
        [KeyboardButton(text="⭐ Отзывы"), KeyboardButton(text="🎁 Друзьям")],
        [KeyboardButton(text="ℹ️ О нас"), KeyboardButton(text="⚙️ Настройки")]
    ]
    return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True, is_persistent=True)

def get_consultations_keyboard():
    buttons = [
        [KeyboardButton(text="🎴 Тарологи"), KeyboardButton(text="⭐ Астрологи")],
        [KeyboardButton(text="🎟 Ввести промокод")],
        [KeyboardButton(text="🏠 Главное меню")]
    ]
    return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True, is_persistent=True)

def get_welcome_keyboard():
    buttons = [
        [KeyboardButton(text="🎴 Задать вопрос тарологу")],
        [KeyboardButton(text="⭐ Астрологи"), KeyboardButton(text="🏠 Главное меню")]
    ]
    return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True, is_persistent=True)

def get_settings_keyboard():
    buttons = [
        [KeyboardButton(text="♈ Изменить знак зодиака")],
        [KeyboardButton(text="📄 Оферта"), KeyboardButton(text="📞 Контакты")],
        [KeyboardButton(text="🔐 Политика обработки ПДн")],
        [KeyboardButton(text="🏠 Главное меню")]
    ]
    return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True, is_persistent=True)

def get_cancel_keyboard():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="❌ Отменить")],
            [KeyboardButton(text="🏠 Главное меню")]
        ],
        resize_keyboard=True,
        is_persistent=True
    )

def get_ckassa_payment_keyboard(pay_url: str, amount_text: str = "", order_id: str = ""):
    pay_text = f"💳 {amount_text}" if amount_text else "💳 Оплатить"
    rows = [
        [InlineKeyboardButton(text=pay_text, url=pay_url)],
        [InlineKeyboardButton(text="🎟 Ввести промокод", callback_data="promo_start")],
    ]
    if order_id:
        rows.append([
            InlineKeyboardButton(
                text="🔄 Получить новую ссылку",
                callback_data=f"ckassa_refresh:{order_id}",
            )
        ])
    return InlineKeyboardMarkup(inline_keyboard=rows)


DISCOUNT_OLD_PRICE_RUB = 310


def _strike_text(text: str) -> str:
    return "".join(f"{char}\u0336" if not char.isspace() else char for char in text)


def _ckassa_sale_amount_text() -> str:
    old_price = _strike_text(f"{DISCOUNT_OLD_PRICE_RUB} рублей")
    return f"{old_price} {ckassa_client.config.amount_rub_text}"


def get_session_keyboard():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🚪 Завершить сеанс")],
            [KeyboardButton(text="🏠 Главное меню")]
        ],
        resize_keyboard=True,
        is_persistent=True
    )

def get_back_keyboard():
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="🏠 Главное меню")]],
        resize_keyboard=True,
        is_persistent=True
    )

def get_compat_sign_keyboard(step: str, sign1_idx: int = -1):
    """Inline-клавиатура для выбора знака в совместимости.
    step='first' — выбор своего знака, step='second' — выбор партнёра (sign1_idx зашит в callback_data)."""
    rows = []
    for i in range(0, len(SIGNS), 3):
        row = []
        for j in range(i, min(i + 3, len(SIGNS))):
            if step == "first":
                cb = f"compat1_{j}"
            else:
                cb = f"compat2_{sign1_idx}_{j}"
            row.append(InlineKeyboardButton(text=SIGNS[j], callback_data=cb))
        rows.append(row)
    if step == "second":
        rows.append([InlineKeyboardButton(text="◀ Назад", callback_data="compat_back")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def get_ask_tarot_inline(tarot_id: str):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📩 Задать вопрос", callback_data=f"ask_{tarot_id}")]
    ])

def get_tarologists_list_keyboard():
    """Все тарологи кнопками по 3 в ряд."""
    buttons = []
    row = []
    for t in TAROLOGISTS:
        row.append(InlineKeyboardButton(text=t["name"], callback_data=f"view_{t['id']}"))
        if len(row) == 3:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def get_tarot_card_keyboard(tarot_id: str, payment_required: bool = False):
    """Кнопки под карточкой таролога."""
    select_text = _ckassa_amount_button_text() if payment_required else "✅ Выбрать"
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=select_text, callback_data=f"ask_{tarot_id}")],
        [InlineKeyboardButton(text="◀ К списку", callback_data="tarot_list")],
    ])

def get_astrologers_list_keyboard():
    """Все астрологи кнопками по 2 в ряд."""
    buttons = []
    row = []
    for a in ASTROLOGERS:
        row.append(InlineKeyboardButton(text=a["name"], callback_data=f"view_astro_{a['id']}"))
        if len(row) == 2:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def get_astro_card_keyboard(astro_id: str, payment_required: bool = False):
    """Кнопки под карточкой астролога."""
    select_text = _ckassa_amount_button_text() if payment_required else "✅ Выбрать"
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=select_text, callback_data=f"ask_astro_{astro_id}")],
        [InlineKeyboardButton(text="◀ К списку", callback_data="astro_list")],
    ])


def _build_main_bot_deeplink(payload: str = "") -> str:
    if not MAIN_BOT_USERNAME:
        return MAIN_BOT_URL
    if payload:
        return f"https://t.me/{MAIN_BOT_USERNAME}?start={payload}"
    return f"https://t.me/{MAIN_BOT_USERNAME}"


def _specialist_start_payload(specialist_type: str, specialist_id: str) -> str:
    return f"spec_{specialist_type}_{specialist_id}"


def _resolve_specialist_start(payload: str) -> tuple[str, dict] | None:
    if not payload.startswith("spec_"):
        return None
    parts = payload.split("_", 2)
    if len(parts) != 3:
        return None
    _, specialist_type, specialist_id = parts
    if specialist_type == "tarot":
        specialist = TAROLOGISTS_BY_ID.get(specialist_id)
    elif specialist_type == "astro":
        specialist = ASTROLOGERS_BY_ID.get(specialist_id)
    else:
        specialist = None
    if not specialist:
        return None
    return specialist_type, specialist


def _specialist_card_text(
    specialist_type: str,
    specialist: dict,
    user_id: str | int | None = None,
) -> tuple[str, InlineKeyboardMarkup]:
    payment_required = user_id is not None and _needs_ckassa_payment(user_id)
    agreement = "\n\n_Нажимая кнопку ниже, ты соглашаешься с условиями публичной оферты и политикой обработки персональных данных (раздел «⚙️ Настройки»)._"
    if specialist_type == "tarot":
        return (
            specialist["description"] + agreement,
            get_tarot_card_keyboard(specialist["id"], payment_required),
        )
    return (
        specialist["description"] + agreement,
        get_astro_card_keyboard(specialist["id"], payment_required),
    )

# ====== ФАЙЛОВЫЕ ФУНКЦИИ ======
def load_users():
    try:
        with open(USERS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except:
        return {}

def save_users(data):
    with open(USERS_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)

def get_sessions_today(user_id: str) -> int:
    """Сколько сеансов пользователь провёл сегодня."""
    users = load_users()
    daily = users.get(user_id, {}).get("sessions_daily", {})
    today = _msk_now().date().isoformat()
    if daily.get("date") != today:
        return 0
    return daily.get("count", 0)

def _get_first_day_msk(user: dict) -> str | None:
    """Возвращает дату первого захода пользователя (MSK, ISO YYYY-MM-DD)."""
    first_day = user.get("first_day_msk")
    if first_day:
        return first_day
    joined = user.get("joined_at")
    if isinstance(joined, str):
        return joined.split("T")[0]
    return None

def get_daily_free_limit(user_id: str) -> int:
    """Базовый лимит бесплатных сеансов: 1 в день регистрации, 0 в последующие дни."""
    users = load_users()
    user = users.get(user_id, {})
    today = _msk_now().date().isoformat()
    first_day = _get_first_day_msk(user)
    if not first_day:
        return 0
    return FREE_SESSIONS_FIRST_DAY if first_day == today else FREE_SESSIONS_DAILY

def increment_sessions_today(user_id: str):
    """Увеличивает счётчик дневных сеансов и списывает платный источник после бесплатного лимита."""
    users = load_users()
    users.setdefault(user_id, {})
    today = _msk_now().date().isoformat()
    daily = users[user_id].get("sessions_daily", {})
    if daily.get("date") != today:
        daily = {"date": today, "count": 0}
    daily["count"] += 1
    users[user_id]["sessions_daily"] = daily
    # Если превысили базовый лимит — списываем: промо → бонус → оплата.
    base_limit = get_daily_free_limit(user_id)
    if daily["count"] > base_limit:
        promo_left = promo_store.consume_session(user_id)
        if promo_left is None:
            bonus = users[user_id].get("bonus_sessions", 0)
            if bonus > 0:
                users[user_id]["bonus_sessions"] = bonus - 1
            else:
                paid = users[user_id].get("paid_sessions", 0)
                if paid > 0:
                    users[user_id]["paid_sessions"] = paid - 1
    save_users(users)

def track_activity(user_id: str, action: str):
    """Записывает активность пользователя. action: 'forecast'|'about_me'|'tarot'|'astro'|'review'"""
    users = load_users()
    users.setdefault(user_id, {})
    stats = users[user_id].setdefault("activity", {})
    stats[action] = stats.get(action, 0) + 1
    stats["total"] = stats.get("total", 0) + 1
    stats["last_seen"] = datetime.now().isoformat()
    save_users(users)

# ====== РЕФЕРАЛЬНАЯ СИСТЕМА ======
BONUS_SESSIONS_PER_REFERRAL = 1  # сколько бонусных сеансов за каждого друга

def get_bonus_sessions(user_id: str) -> int:
    """Возвращает количество неиспользованных бонусных сеансов у пользователя."""
    users = load_users()
    return users.get(user_id, {}).get("bonus_sessions", 0)

def get_paid_sessions(user_id: str) -> int:
    users = load_users()
    return users.get(user_id, {}).get("paid_sessions", 0)

def get_promo_sessions(user_id: str) -> int:
    return promo_store.get_balance(str(user_id))

def add_paid_session_credit(user_id: str, count: int = 1) -> int:
    users = load_users()
    users.setdefault(user_id, {})
    current = int(users[user_id].get("paid_sessions", 0))
    users[user_id]["paid_sessions"] = current + count
    save_users(users)
    return users[user_id]["paid_sessions"]

def get_free_sessions_remaining_today(user_id: str) -> int:
    return max(0, get_daily_free_limit(user_id) - get_sessions_today(user_id))

def get_available_session_count(user_id: str) -> int:
    return (
        get_free_sessions_remaining_today(user_id)
        + get_promo_sessions(user_id)
        + get_bonus_sessions(user_id)
        + get_paid_sessions(user_id)
    )

def _session_word(count: int) -> str:
    count = abs(int(count))
    if count % 10 == 1 and count % 100 != 11:
        return "сеанс"
    if 2 <= count % 10 <= 4 and not 12 <= count % 100 <= 14:
        return "сеанса"
    return "сеансов"

def _available_session_parts(user_id: str) -> list[str]:
    parts = []
    free = get_free_sessions_remaining_today(user_id)
    promo = get_promo_sessions(user_id)
    bonus = get_bonus_sessions(user_id)
    paid = get_paid_sessions(user_id)
    if free > 0:
        parts.append(f"бесплатных сегодня: {free}")
    if promo > 0:
        parts.append(f"по промокоду: {promo}")
    if bonus > 0:
        parts.append(f"бонусных: {bonus}")
    if paid > 0:
        parts.append(f"оплаченных: {paid}")
    return parts

def _available_session_note(user_id: str) -> str:
    remaining = get_available_session_count(user_id)
    if remaining <= 0:
        return "Доступных сеансов сейчас нет."
    parts = _available_session_parts(user_id)
    detail = f" ({', '.join(parts)})" if parts else ""
    return f"Доступно: {remaining} {_session_word(remaining)}{detail}."

def has_available_session(user_id: str) -> bool:
    return get_available_session_count(user_id) > 0

def get_effective_session_limit(user_id: str) -> int:
    """Сколько сеансов пользователь может начать прямо сейчас."""
    return get_available_session_count(user_id)

def _ckassa_amount_button_text() -> str:
    return f"💳 {_ckassa_sale_amount_text()}"

def _needs_ckassa_payment(user_id: str | int) -> bool:
    try:
        if int(user_id) == ADMIN_ID:
            return False
    except (TypeError, ValueError):
        pass
    user_id = str(user_id)
    return not has_available_session(user_id)

def _payment_specialist_name(specialist_type: str, specialist_id: str) -> str:
    if specialist_type == "tarot":
        specialist = TAROLOGISTS_BY_ID.get(specialist_id)
    elif specialist_type == "astro":
        specialist = ASTROLOGERS_BY_ID.get(specialist_id)
    else:
        specialist = None
    return specialist.get("name", "") if specialist else ""

def _payment_continue_keyboard(order: dict) -> InlineKeyboardMarkup | None:
    specialist_type = order.get("specialist_type", "")
    specialist_id = order.get("specialist_id", "")
    if specialist_type == "tarot" and specialist_id:
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🎴 Продолжить с тарологом", callback_data=f"ask_{specialist_id}")]
        ])
    if specialist_type == "astro" and specialist_id:
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⭐ Продолжить с астрологом", callback_data=f"ask_astro_{specialist_id}")]
        ])
    return None

async def _send_ckassa_invoice(message: Message, order: dict, reused: bool = False) -> None:
    prefix = "У тебя уже есть активная ссылка на оплату." if reused else "Готово, создал ссылку на оплату."
    specialist_name = _payment_specialist_name(
        order.get("specialist_type", ""),
        order.get("specialist_id", ""),
    )
    specialist_line = f"\nСпециалист: {specialist_name}" if specialist_name else ""
    amount_rub = _ckassa_sale_amount_text()
    await message.answer(
        f"💳 {prefix}\n\n"
        f"Сумма: {amount_rub}{specialist_line}\n"
        "Нажми кнопку ниже — откроется страница Ckassa. После оплаты бот сам увидит платеж и начислит один сеанс.",
        reply_markup=get_ckassa_payment_keyboard(
            order["invoice_url"],
            amount_rub,
            order.get("order_id", ""),
        ),
        disable_web_page_preview=True,
    )

async def offer_ckassa_payment(message: Message, user, specialist_type: str = "", specialist_id: str = "") -> None:
    global _last_ckassa_provider_alert_at

    user_id = str(user.id)
    try:
        ckassa_client.config.validate()
    except CkassaPaymentConfigError as e:
        await message.answer(
            "💳 Оплата почти подключена, но пока не настроена стоимость консультации. "
            "Я уже передал администратору, что нужно проверить настройки.",
            reply_markup=get_main_keyboard(),
        )
        await notify_admin(f"[Ckassa] Payment config error: {e}")
        return

    active_order = ckassa_store.find_active_order(user_id, ckassa_client.config.amount_kopeks)
    if active_order:
        await _send_ckassa_invoice(message, active_order, reused=True)
        return

    try:
        order_id = make_order_id(user_id)
        invoice = await ckassa_client.create_invoice(
            order_id=order_id,
            telegram_id=user_id,
        )
        order = ckassa_store.create_order(
            order_id=invoice.order_id,
            user_id=user_id,
            amount_kopeks=invoice.amount_kopeks,
            invoice_url=invoice.pay_url,
            best_before=invoice.best_before,
            specialist_type=specialist_type,
            specialist_id=specialist_id,
        )
        await _send_ckassa_invoice(message, order)
    except CkassaProviderNotFound as e:
        print(f"[Ckassa] provider not found for user {user_id}: {e}")
        now = time.monotonic()
        if (
            not _last_ckassa_provider_alert_at
            or now - _last_ckassa_provider_alert_at >= CKASSA_CONFIG_ALERT_COOLDOWN_SEC
        ):
            _last_ckassa_provider_alert_at = now
            await notify_admin(
                "[Ckassa] Оператор не найден (ошибка 1354).\n"
                f"CKASSA_SERV_CODE={ckassa_client.config.serv_code or '<empty>'}\n"
                f"CKASSA_BASE_URL={ckassa_client.config.base_url}\n"
                "Проверьте, что этот servCode активен и подключён именно к текущим "
                "ApiLoginAuthorization/ApiAuthorization и к выбранной среде Ckassa."
            )
        await message.answer(
            "Оплата сейчас временно недоступна из-за настройки платёжного сервиса. "
            "Администратор уже получил точную причину.",
            reply_markup=get_main_keyboard(),
        )
    except CkassaPaymentAccessDenied as e:
        print(f"[Ckassa] create invoice access denied for user {user_id}: {e}")
        await notify_admin(f"[Ckassa] Create invoice access denied for user {user_id}: {e}")
        await message.answer(
            "Ckassa отклонила создание ссылки на оплату. Я уже передал администратору, что нужно проверить настройки оплаты.",
            reply_markup=get_main_keyboard(),
        )
    except CkassaPaymentError as e:
        print(f"[Ckassa] create invoice failed: {e}")
        await notify_admin(f"[Ckassa] Create invoice failed for user {user_id}: {e}")
        await message.answer(
            "Не получилось создать ссылку на оплату. Попробуй чуть позже, пожалуйста.",
            reply_markup=get_main_keyboard(),
        )

async def credit_paid_order(order: dict, notify_user: bool = True) -> bool:
    if order.get("credited"):
        return False
    user_id = str(order.get("user_id", ""))
    if not user_id:
        return False
    order_id = order["order_id"]
    paid_left = add_paid_session_credit(user_id, 1)
    ckassa_store.mark_order_credited(order_id)
    if notify_user:
        text = (
            "✅ Оплата получена. Я начислил один платный сеанс.\n\n"
            f"Доступных платных сеансов: {paid_left}."
        )
        receipt = order.get("receipt") or order.get("payment", {}).get("receipt")
        if receipt:
            text += f"\n\nЧек: {receipt}"
        try:
            await bot.send_message(
                int(user_id),
                text,
                reply_markup=_payment_continue_keyboard(order),
                disable_web_page_preview=True,
            )
        except Exception as e:
            print(f"[Ckassa] notify paid user {user_id}: {e}")

    amount_kopeks = order.get("amount_kopeks", 0)
    earned_added, earnings = ckassa_store.add_earned_amount(order_id, amount_kopeks)
    earned_total = format_kopeks_amount(earnings.get("total_kopeks", 0))
    earned_count = int(earnings.get("orders_count", 0) or 0)
    amount_text = format_kopeks_amount(amount_kopeks)
    if earned_added:
        earnings_text = (
            f"\n\n💰 Счётчик заработка: +{amount_text}"
            f"\n📈 Всего заработано: {earned_total}"
            f"\n🧾 Оплаченных чеков: {earned_count}"
        )
    else:
        earnings_text = f"\n\n💰 Всего заработано: {earned_total}"

    await notify_admin(
        f"💳 Оплаченная консультация засчитана\n"
        f"user_id={user_id}\n"
        f"order_id={order_id}\n"
        f"regPayNum={order.get('reg_pay_num', '')}"
        f"{earnings_text}"
    )
    return True

async def credit_uncredited_paid_orders(notify_user: bool = True) -> list[dict]:
    credited = []
    for order in ckassa_store.uncredited_paid_orders():
        if await credit_paid_order(order, notify_user=notify_user):
            credited.append(order)
    return credited

async def process_ckassa_payment_updates(notify_users: bool = True) -> list[dict]:
    async with CKASSA_STATE_LOCK:
        credited = []
        payments = await ckassa_client.get_new_payments()
        for payment in payments:
            payment_key = payment_identity(payment)
            order_id = extract_payment_order_id(payment)
            state = str(payment.get("state") or "").upper()
            if not order_id:
                ckassa_store.mark_payment_seen(payment_key)
                continue
            if not ckassa_store.mark_payment_seen(payment_key):
                continue
            if state == "PAYED":
                order = ckassa_store.mark_order_paid(order_id, payment)
                if order and await credit_paid_order(order, notify_user=notify_users):
                    credited.append(order)
            else:
                ckassa_store.mark_order_state(order_id, state or "unknown", payment)
        credited.extend(await credit_uncredited_paid_orders(notify_user=notify_users))
        return credited

async def ckassa_payment_watcher():
    try:
        ckassa_client.config.validate()
    except CkassaPaymentConfigError as e:
        print(f"[Ckassa] watcher disabled: {e}")
        return
    while True:
        try:
            await process_ckassa_payment_updates(notify_users=True)
        except Exception as e:
            print(f"[Ckassa] watcher error: {e}")
        await asyncio.sleep(max(15, CKASSA_POLL_INTERVAL_SEC))

def save_referral_link(referrer_id: str, new_user_id: str) -> bool:
    """Сохраняет связь реферер→друг. Бонус начислится когда друг пройдёт первый сеанс."""
    users = load_users()
    referrer = users.get(referrer_id)
    if not referrer:
        return False
    # Нельзя пригласить того, кто уже привязан
    users.setdefault(new_user_id, {})
    if users[new_user_id].get("referred_by"):
        return False
    users[new_user_id]["referred_by"] = referrer_id
    users[new_user_id]["referral_bonus_granted"] = False  # бонус ещё не начислен
    save_users(users)
    return True

def _try_grant_referral_bonus(user_id: str):
    """Проверяет, пришёл ли пользователь по реферальной ссылке и нужно ли начислить бонус рефереру.
    Вызывается при завершении первого сеанса пользователя."""
    users = load_users()
    user_data = users.get(user_id, {})
    referrer_id = user_data.get("referred_by")
    if not referrer_id:
        return None
    if user_data.get("referral_bonus_granted"):
        return None
    # Начисляем бонус рефереру
    referrer = users.get(referrer_id)
    if not referrer:
        return None
    referrals = referrer.get("referrals", [])
    if user_id not in referrals:
        referrals.append(user_id)
    referrer["referrals"] = referrals
    referrer["bonus_sessions"] = referrer.get("bonus_sessions", 0) + BONUS_SESSIONS_PER_REFERRAL
    referrer["referrals_total"] = referrer.get("referrals_total", 0) + 1
    users[referrer_id] = referrer
    # Отмечаем что бонус начислен
    users[user_id]["referral_bonus_granted"] = True
    save_users(users)
    return referrer_id

async def check_and_grant_referral_bonus(user_id: str):
    """Проверяет реферальную связь и начисляет бонус рефереру при первом сеансе друга."""
    referrer_id = _try_grant_referral_bonus(user_id)
    if referrer_id:
        try:
            users = load_users()
            friend_name = users.get(user_id, {}).get("full_name", user_id)
            await bot.send_message(
                int(referrer_id),
                f"🎉 Твой друг *{friend_name}* прошёл первую консультацию\\!\n"
                f"Тебе начислен *\\+{BONUS_SESSIONS_PER_REFERRAL} бонусный сеанс*\\. 🌟",
                parse_mode="MarkdownV2",
                reply_markup=get_main_keyboard()
            )
        except Exception:
            pass

def save_forecast(data):
    with open(FORECAST_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)

def load_forecast():
    try:
        with open(FORECAST_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except:
        return {}

def load_descriptions():
    try:
        with open(DESCRIPTIONS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except:
        return {}

def save_descriptions(data):
    with open(DESCRIPTIONS_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)

def load_compatibility():
    try:
        with open(COMPAT_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except:
        return {}

def save_compatibility(data):
    with open(COMPAT_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)

def load_tarot_history():
    try:
        with open(TAROT_HISTORY_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except:
        return {}

def save_tarot_history(data):
    with open(TAROT_HISTORY_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)

def get_user_tarot_history(user_id: str, tarot_id: str) -> list:
    history = load_tarot_history()
    return history.get(user_id, {}).get(tarot_id, [])

def save_user_tarot_message(user_id: str, tarot_id: str, role: str, text: str, **extra):
    history = load_tarot_history()
    history.setdefault(user_id, {}).setdefault(tarot_id, [])
    entry = {
        "role": role,
        "text": text,
        "time": datetime.now().isoformat()
    }
    entry.update(extra)
    history[user_id][tarot_id].append(entry)
    save_tarot_history(history)

def load_astro_history():
    try:
        with open(ASTRO_HISTORY_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except:
        return {}

def save_astro_history(data):
    with open(ASTRO_HISTORY_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)

def get_user_astro_history(user_id: str, astro_id: str) -> list:
    history = load_astro_history()
    return history.get(user_id, {}).get(astro_id, [])

def save_user_astro_message(user_id: str, astro_id: str, role: str, text: str):
    history = load_astro_history()
    history.setdefault(user_id, {}).setdefault(astro_id, [])
    history[user_id][astro_id].append({
        "role": role,
        "text": text,
        "time": datetime.now().isoformat()
    })
    save_astro_history(history)

# ====== ОТЗЫВЫ: ОЖИДАЮЩИЕ МОДЕРАЦИИ ======
# Модерация (публикация/отклонение/редактирование) выполняется в админ-боте (mainAdmin.py).
def load_pending_reviews() -> dict:
    if os.path.exists(PENDING_REVIEWS_FILE):
        try:
            with open(PENDING_REVIEWS_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {}

def save_pending_reviews(pending: dict):
    with open(PENDING_REVIEWS_FILE, "w", encoding="utf-8") as f:
        json.dump(pending, f, ensure_ascii=False, indent=2)

def _send_email_sync(subject: str, body: str):
    """Синхронная отправка email — запускать через run_in_executor."""
    if not EMAIL_FROM or not EMAIL_PASSWORD:
        return
    try:
        msg = MIMEMultipart()
        msg["From"] = EMAIL_FROM
        msg["To"] = EMAIL_TO
        msg["Subject"] = subject
        msg.attach(MIMEText(body, "plain", "utf-8"))
        with smtplib.SMTP_SSL("smtp.mail.ru", 465) as server:
            server.login(EMAIL_FROM, EMAIL_PASSWORD)
            server.sendmail(EMAIL_FROM, EMAIL_TO, msg.as_string())
    except Exception as e:
        print(f"[EMAIL] Ошибка отправки: {e}")

async def _notify_new_user(message):
    """Уведомляет администратора о новом пользователе через Telegram и email."""
    user = message.from_user
    date_str = datetime.now().strftime("%d.%m.%Y %H:%M")
    username = f"@{user.username}" if user.username else "нет username"
    full_name = user.full_name or "—"

    await notify_admin(
        f"🆕 *Новый пользователь*\n\n"
        f"👤 Имя: {full_name}\n"
        f"🔗 Username: {username}\n"
        f"🆔 ID: `{user.id}`\n"
        f"📅 Дата: {date_str}",
        parse_mode="Markdown"
    )

    subject = "🆕 Новый пользователь в боте «Голос Звёзд»"
    body = (
        f"В бот «Голос Звёзд» зашёл новый пользователь.\n\n"
        f"Имя: {full_name}\n"
        f"Username: {username}\n"
        f"ID: {user.id}\n"
        f"Дата: {date_str}"
    )
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, _send_email_sync, subject, body)


async def send_review_notification(review_id: str, review: dict):
    """Отправляет email и Telegram-уведомление администратору о новом отзыве.
    Модерация выполняется в админ-боте (mainAdmin.py) по кнопке «⭐ Отзывы на модерации»."""
    date_str = datetime.now().strftime("%d.%m.%Y %H:%M")
    subject = "⭐ Новый отзыв на бот «Голос Звёзд»"
    body = (
        f"Новый отзыв на бот «Голос Звёзд» ожидает модерации.\n\n"
        f"ID отзыва: {review_id}\n"
        f"Дата: {date_str}\n"
        f"Автор: {review['author']}\n"
        f"Тема: {review['tag']}\n\n"
        f"Текст:\n{review['text']}\n\n"
        f"---\n"
        f"Откройте админ-бот и нажмите «⭐ Отзывы на модерации»."
    )
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, _send_email_sync, subject, body)

    await notify_admin(
        f"⭐ *Новый отзыв на модерацию*\n\n"
        f"👤 *Автор:* {review['author']}\n"
        f"🏷 *Тема:* {review['tag']}\n"
        f"📅 *Дата:* {date_str}\n\n"
        f"💬 *Текст:*\n{review['text']}\n\n"
        f"_Нажми «⭐ Отзывы на модерации» в меню, чтобы обработать._",
        parse_mode="Markdown",
    )

def get_review_topic_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔮 Знаки зодиака",     callback_data="rev_topic_zodiac")],
        [InlineKeyboardButton(text="🎴 Таролог",            callback_data="rev_topic_tarot")],
        [InlineKeyboardButton(text="⭐ Астролог",           callback_data="rev_topic_astro")],
        [InlineKeyboardButton(text="✨ О сервисе в целом",     callback_data="rev_topic_general")],
        [InlineKeyboardButton(text="❌ Отмена",              callback_data="rev_cancel")],
    ])

def get_review_anon_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👤 Анонимно",           callback_data="rev_anon_yes")],
        [InlineKeyboardButton(text="✍️ С моим именем",      callback_data="rev_anon_no")],
        [InlineKeyboardButton(text="❌ Отмена",              callback_data="rev_cancel")],
    ])

def is_working_hours() -> bool:
    """Рабочее время специалистов: 9:00–22:00 по МСК (UTC+3)."""
    msk_hour = (datetime.now(timezone.utc).hour + 3) % 24
    return 9 <= msk_hour < 22

WORKING_HOURS_BYPASS_USERNAMES = {"turumbos", "sveta_sny"}

def can_start_consultation_now(user) -> bool:
    username = (getattr(user, "username", "") or "").lstrip("@").lower()
    return (
        getattr(user, "id", None) == ADMIN_ID
        or username in WORKING_HOURS_BYPASS_USERNAMES
        or is_working_hours()
    )

def get_offline_message(specialist_name: str) -> str:
    hour = (datetime.now(timezone.utc).hour + 3) % 24
    if hour < 9:
        back_at = "в 9:00"
        reason = "ещё отдыхает"
    else:
        back_at = "завтра в 9:00"
        reason = "уже завершил приём на сегодня"
    return (
        f"🌙 *{specialist_name} сейчас не на месте*\n\n"
        f"Специалист {reason} — приём ведётся с 9:00 до 22:00 по Москве.\n"
        f"Возвращайся {back_at}, и {specialist_name} обязательно тебя примет. 🌟"
    )

REVIEW_TOPIC_MAP = {
    "rev_topic_zodiac":   "🔮 Прогнозы и знаки",
    "rev_topic_tarot":    "🎴 Консультация таролога",
    "rev_topic_astro":    "⭐ Консультация астролога",
    "rev_topic_general":  "✨ О сервисе",
}

# ====== БЕЗОПАСНЫЙ РАЗБОР JSON ======
def extract_json_from_text(text: str):
    try:
        match = re.search(r"\{.*\}", text, re.DOTALL)
        if match:
            return json.loads(match.group())
    except json.JSONDecodeError:
        pass
    return {}

# ====== API ЗАПРОС ======
AI_MODEL_PRIMARY = getenv("OPENROUTER_MODEL", "deepseek/deepseek-chat")
# Список :free-моделей OpenRouter регулярно меняется — старые id возвращают 404,
# а популярные модели рейт-лимитятся апстрим-провайдерами. Перебираем пачку моделей
# у разных провайдеров — первая ответившая выигрывает.
# Переопределить можно через env OPENROUTER_FREE_MODELS (через запятую) без правок кода.
_free_models_default = (
    "qwen/qwen3-next-80b-a3b-instruct:free,"
    "openai/gpt-oss-120b:free,"
    "z-ai/glm-4.5-air:free,"
    "nvidia/nemotron-3-super-120b-a12b:free,"
    "minimax/minimax-m2.5:free,"
    "nousresearch/hermes-3-llama-3.1-405b:free,"
    "meta-llama/llama-3.3-70b-instruct:free"
)
AI_MODELS_FREE_FALLBACK = [
    m.strip() for m in getenv("OPENROUTER_FREE_MODELS", _free_models_default).split(",") if m.strip()
]
# Обратная совместимость со старой одиночной переменной — если задана, ставим её первой.
_legacy_free_model = (getenv("OPENROUTER_FREE_MODEL") or "").strip()
if _legacy_free_model:
    AI_MODELS_FREE_FALLBACK = [_legacy_free_model] + [
        m for m in AI_MODELS_FREE_FALLBACK if m != _legacy_free_model
    ]
# Free-модели OpenRouter ограничены ~2540 входных токенов. Держим запас с учётом того, что
# русский текст примерно 3 символа = 1 токен — 6000 символов укладывается в лимит.
FREE_FALLBACK_CHAR_BUDGET = 6000
FREE_FALLBACK_MAX_TOKENS = 800


def _shrink_prompt_for_free(prompt: str) -> str:
    """Сжимает prompt для повторной попытки на бесплатной модели с узким контекстом."""
    prompt = prompt.replace(NO_CONTACTS_RULE, NO_CONTACTS_RULE_SHORT)
    if len(prompt) > FREE_FALLBACK_CHAR_BUDGET:
        prompt = re.sub(
            r"\n\nПредыдущие обращения этого человека.*?(?=\n\n[А-ЯA-Z])",
            "",
            prompt,
            count=1,
            flags=re.DOTALL,
        )
    return prompt


def _is_quota_error(err: dict | None) -> bool:
    """True если ошибка про деньги/квоту/рейт-лимит апстрима — повод уйти на free-fallback."""
    if not err:
        return False
    if err.get("code") in (402, 429):
        return True
    msg = (err.get("message") or "").lower()
    raw = ""
    meta = err.get("metadata")
    if isinstance(meta, dict):
        raw = (meta.get("raw") or "").lower()
    haystack = msg + " " + raw
    return any(kw in haystack for kw in ("credit", "afford", "tokens limit", "rate-limit", "rate limit"))


async def _call_openrouter(model: str, prompt: str, max_tokens: int) -> tuple[str, dict | None]:
    url = "https://openrouter.ai/api/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {OPENROUTER_KEY}",
        "Content-Type": "application/json",
    }
    data = {
        "model": model,
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": max_tokens,
        "usage": {"include": True},
    }
    try:
        timeout = aiohttp.ClientTimeout(total=120)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(url, headers=headers, json=data) as resp:
                response_json = await resp.json(content_type=None)
                if "choices" in response_json:
                    content = response_json["choices"][0]["message"].get("content") or ""
                    usage = response_json.get("usage") or {}
                    cost = usage.get("cost")
                    if cost is not None:
                        print(
                            f"[openrouter] {model} cost=${cost:.6f} "
                            f"in={usage.get('prompt_tokens')} out={usage.get('completion_tokens')}"
                        )
                    return content.strip(), None
                if "error" in response_json:
                    return "", response_json["error"]
                return "", {"message": "unexpected response", "raw": response_json}
    except Exception as e:
        return "", {"message": f"exception: {e}"}


async def ask_ai(prompt: str, max_tokens: int = 1000) -> str:
    answer, err = await _call_openrouter(AI_MODEL_PRIMARY, prompt, max_tokens)
    if answer:
        return answer
    if err:
        print("Ошибка API OpenRouter:", err)

    if _is_quota_error(err):
        short_prompt = _shrink_prompt_for_free(prompt)
        fallback_tokens = min(max_tokens, FREE_FALLBACK_MAX_TOKENS)
        for model in AI_MODELS_FREE_FALLBACK:
            answer2, err2 = await _call_openrouter(model, short_prompt, fallback_tokens)
            if answer2:
                print(f"[ask_ai] fallback на {model} отработал, длина промпта {len(short_prompt)}")
                return answer2
            if err2:
                print(f"Ошибка API OpenRouter (fallback {model}):", err2)
    return ""

# ====== ПОЛУЧЕНИЕ ГОРОСКОПА ======
async def get_horoscope():
    today = datetime.now(timezone(timedelta(hours=3)))
    date_str = today.strftime("%d.%m.%Y")
    weekday_ru = ["понедельник","вторник","среда","четверг","пятница","суббота","воскресенье"][today.weekday()]

    # Собираем астрологический контекст для каждого знака
    signs_context = "\n".join(
        f"- {s}: стихия {d['стихия']}, управитель {d['планета']}, {d['модальность']}"
        for s, d in SIGN_DATA.items()
    )

    prompt = f"""
Ты профессиональный астролог. Составь гороскоп на {date_str} ({weekday_ru}) для всех 12 знаков зодиака.

Астрологическая база знаков (опирайся на неё):
{signs_context}

Учитывай:
- Управляющую планету каждого знака и её влияние на текущий день недели
- Стихию знака и как она проявляется сегодня
- День недели: {weekday_ru} (у каждого дня свои планетарные управители в классической астрологии)

Каждый знак должен получить 2-3 предложения с конкретными рекомендациями на день, привязанными к его стихии и планете.
ВАЖНО: весь текст строго на русском языке, без единого английского слова или фразы.
Не используй тире ни длинные ни короткие нигде в тексте.
Ответ строго в JSON формате без лишнего текста:
{{"Овен":"...","Телец":"...","Близнецы":"...","Рак":"...","Лев":"...","Дева":"...","Весы":"...","Скорпион":"...","Стрелец":"...","Козерог":"...","Водолей":"...","Рыбы":"..."}}
"""
    result = await ask_ai(prompt, max_tokens=2000)
    result = result.replace("```json", "").replace("```", "").strip()
    return extract_json_from_text(result)

# ====== ОПИСАНИЕ ЗНАКА ======
async def get_sign_description(sign: str) -> str:
    prompt = f"""
Напиши подробное и глубокое описание знака зодиака {sign}.
Структура: общая характеристика, характер и личность (сильные стороны и слабости), отношения и любовь, карьера и призвание, здоровье и энергетика, интересные факты.
Пиши живым тёплым языком без шаблонов. Объём не менее 400 слов.
Только текст, без JSON, без заголовков со звёздочками, можно использовать эмодзи.
НЕ используй заголовки с решётками (#, ##, ###, ####) — только обычный текст и абзацы.
ВАЖНО: весь текст строго на русском языке, без единого английского слова или фразы.
Не используй тире ни длинные ни короткие нигде в тексте.
"""
    return await ask_ai(prompt, max_tokens=1500)

# ====== СОВМЕСТИМОСТЬ ЗНАКОВ ======
async def get_compatibility(sign1: str, sign2: str) -> str:
    d1 = SIGN_DATA[sign1]
    d2 = SIGN_DATA[sign2]
    el_compat = get_element_compat(d1["стихия"], d2["стихия"])

    prompt = f"""
Ты профессиональный астролог. Сделай разбор совместимости {sign1} и {sign2}, СТРОГО опираясь на классическую астрологию.

Астрологические данные (используй их как основу, упоминай в тексте):
{sign1}: стихия {d1['стихия']}, {d1['модальность']}, управитель {d1['планета']}
{sign2}: стихия {d2['стихия']}, {d2['модальность']}, управитель {d2['планета']}
Совместимость стихий ({d1['стихия']} + {d2['стихия']}): {el_compat}

Структура:
1. Общая совместимость (процент от 0 до 100 и краткий вердикт, основанный на взаимодействии стихий и планет)
2. Любовь и романтика (как взаимодействуют управители: {d1['планета']} и {d2['планета']})
3. Дружба и общение
4. Возможные конфликты (из-за модальностей: {d1['модальность']} vs {d2['модальность']}) и как их решать
5. Совет этой паре

Пиши живым, тёплым, немного интригующим языком. Используй эмодзи для акцентов.
Объём 200-300 слов. Только текст, без JSON.
НЕ используй заголовки с решётками (#, ##, ###) — используй эмодзи вместо заголовков.
ВАЖНО: весь текст строго на русском языке, без единого английского слова или фразы.
Не используй тире ни длинные ни короткие нигде в тексте.
"""
    return await ask_ai(prompt, max_tokens=1200)

# ====== УТРЕННЕЕ УВЕДОМЛЕНИЕ ======
async def get_morning_message(sign: str) -> str:
    template = random.choice(MORNING_TEMPLATES)
    return template.format(sign=sign)

# ====== РАСПОЗНАВАНИЕ ГОЛОСА (Groq Whisper, бесплатно) ======
async def transcribe_voice(ogg_file_path: str) -> str:
    """Распознавание голоса через Groq Whisper API (бесплатно)"""
    if not GROQ_API_KEY:
        print("GROQ_API_KEY не задан, распознавание невозможно")
        return ""

    try:
        url = "https://api.groq.com/openai/v1/audio/transcriptions"
        headers = {"Authorization": f"Bearer {GROQ_API_KEY}"}

        data = aiohttp.FormData()
        data.add_field("file", open(ogg_file_path, "rb"), filename="voice.ogg", content_type="audio/ogg")
        data.add_field("model", "whisper-large-v3-turbo")
        data.add_field("language", "ru")
        data.add_field("prompt", "Здравствуйте, как дела? Расскажите, пожалуйста, что произошло. Я думаю, это очень важно!")

        timeout = aiohttp.ClientTimeout(total=60)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(url, headers=headers, data=data) as resp:
                result = await resp.json(content_type=None)
                if "text" in result:
                    return result["text"].strip()
                else:
                    print("Ошибка Groq Whisper:", result)
                    return ""
    except Exception as e:
        print(f"Ошибка распознавания голоса: {e}")
        return ""

# ====== РЕАЛИСТИЧНАЯ ЗАДЕРЖКА НАБОРА ======
def calc_typing_delay(text: str, age: int) -> float:
    """Задержка между частями сообщения, пропорциональная длине и возрасту.
    Короткое сообщение печатается быстрее, длинное — дольше. Плюс пауза на обдумывание."""
    chars = len(text or "")
    if age >= 55:
        cps = random.uniform(1.0, 1.5)
    elif age >= 45:
        cps = random.uniform(1.4, 2.0)
    elif age >= 35:
        cps = random.uniform(1.8, 2.6)
    elif age >= 25:
        cps = random.uniform(2.2, 3.2)
    else:
        cps = random.uniform(2.6, 3.8)
    thinking = random.uniform(3, 7)
    delay = chars / cps + thinking
    return max(5.0, min(150.0, delay))


# ====== СТИЛЬ НАБОРА ПО ВОЗРАСТУ ======
def get_age_typing_style(age: int) -> str:
    if age >= 57:
        return (
            "СТИЛЬ НАБОРА (человеку за 57, очень много опечаток от плохого зрения и дрожания рук): "
            "Путай похожие буквы очень часто: 'ш' и 'щ' ('нашол' или 'нащёл'), 'м' и 'н' ('мне' как 'ннне' или 'мнне'), 'а' и 'о' ('дото' вместо 'дата', 'кортыа' вместо 'карты'). "
            "Часто пропускай буквы: 'прив т', 'слшай', 'смотр'. "
            "Иногда добавь лишнюю: 'оченнь', 'смотриии', 'спасиббо'. "
            "Промахивайся по соседним клавишам: 'лдюди', 'кагк', 'ккарты', 'тбее'. "
            "Почти без запятых, иногда вообще без знаков препинания кроме точки в конце. "
            "Обязательно используй 'вот', 'значит', 'это самое', 'как его' прямо внутри мысли несколько раз. "
            "Пример: 'ну вот значит кортыа легли вот такие это самое три штуки и я вижу что у тебя это самое ситуация там непростая значит'."
        )
    elif age >= 50:
        return (
            "СТИЛЬ НАБОРА (человеку за 50, заметные опечатки от зрения): "
            "Иногда путай похожие буквы: 'ш'/'щ', 'м'/'н'. "
            "Иногда пропусти букву или промахнись по соседней клавише: 'привт', 'лдюди'. "
            "Иногда добавь лишнюю букву: 'оченнь', 'каакрты'. "
            "Запятых заметно меньше чем нужно, длинные фразы без знаков. "
            "Несколько раз используй 'вот', 'значит', 'это самое' как речевые заполнители."
        )
    elif age >= 43:
        return (
            "СТИЛЬ НАБОРА (человеку за 43, редкие опечатки): "
            "Изредка промахнись по соседней клавише или пропусти букву. "
            "Запятые ставишь, но нередко пропускаешь в длинных фразах. "
            "Иногда употреби 'вот' или 'значит' как заполнитель."
        )
    else:
        return ""

# ====== МОДЕРАЦИЯ СООБЩЕНИЙ ======
async def check_incomprehensible(text: str) -> bool:
    """Возвращает True если сообщение — бессмыслица, рандомные символы или полностью чужой язык."""
    if len(text.strip()) < 3:
        return False  # слишком короткое — пусть специалист сам решает
    prompt = (
        "Определи, является ли следующее сообщение:\n"
        "- случайным набором символов или букв без смысла (клавиатурный спам)\n"
        "- набором несвязанных между собой слов, из которых невозможно понять смысл\n"
        "- текстом написанным полностью на иностранном языке (не русском и не украинском)\n\n"
        "Сообщение считается ПОНЯТНЫМ если: это осмысленный вопрос, фраза, эмоция или реакция на русском "
        "или украинском — даже очень короткая ('что дальше?', 'спасибо', 'понял', 'а как же работа?').\n\n"
        "Ответь строго одним словом: YES если бессмыслица/иностранный язык, NO если понятно.\n"
        f"Сообщение: {text}"
    )
    result = await ask_ai(prompt, max_tokens=10)
    return "YES" in result.upper()

async def get_incomprehensible_reply(specialist: dict) -> str:
    """Генерирует короткий ответ специалиста в его стиле — что не понимает сообщение."""
    prompt = (
        f"{specialist['personality']}\n\n"
        "Пользователь прислал тебе сообщение которое ты не можешь понять — "
        "либо это бессмысленный набор символов, либо текст на иностранном языке.\n\n"
        "Напиши ОДНО короткое сообщение (1-2 предложения) в своём стиле: "
        "скажи что не понимаешь о чём речь, и что завершаешь сеанс. "
        "Не груби, но будь прямым. Сохраняй свой характер. Никаких '|||'."
    )
    return await ask_ai(prompt, max_tokens=100)

async def check_profanity(text: str) -> bool:
    prompt = (
        "Определи, содержит ли следующее сообщение мат, грубые оскорбления, явно неприемлемые слова "
        "или откровенные сексуальные домогательства на русском языке. "
        "Ответь строго одним словом: YES если содержит, NO если нет.\n"
        f"Сообщение: {text}"
    )
    result = await ask_ai(prompt, max_tokens=10)
    return "YES" in result.upper()

# ====== ОТВЕТ ТАРОЛОГА (первичный расклад) ======
async def get_tarot_answer(
    tarologist: dict, user_story: str, user_id: str, is_flagged: bool = False,
    anecdote_allowed: bool = False, selected_card: dict | None = None,
) -> str:
    history = get_user_tarot_history(user_id, tarologist["id"])[-10:]
    history_text = ""
    if history:
        now = datetime.now()
        # Находим время последнего сообщения из прошлых сеансов
        last_time = None
        for item in history:
            if "time" in item:
                try:
                    t = datetime.fromisoformat(item["time"])
                    if last_time is None or t > last_time:
                        last_time = t
                except Exception:
                    pass

        if last_time:
            delta_sec = (now - last_time).total_seconds()
            if delta_sec < 3600:
                time_label = f"примерно {int(delta_sec / 60)} минут назад"
            elif delta_sec < 86400:
                time_label = f"примерно {int(delta_sec / 3600)} часов назад"
            elif delta_sec < 7 * 86400:
                time_label = f"примерно {int(delta_sec / 86400)} дней назад"
            else:
                time_label = "больше недели назад"
        else:
            time_label = "когда-то раньше"
            delta_sec = 99999999

        history_text = f"\n\nПредыдущие обращения этого человека к тебе ({time_label}):\n"
        for item in history:
            role_label = "Человек" if item["role"] == "user" else "Ты"
            history_text += f"{role_label}: {item['text']}\n"

        if delta_sec < 7 * 86400:
            history_text += (
                f"\nТы хорошо помнишь этого человека — прошло {time_label}. "
                "Ты знаешь о чём говорили. НЕ веди себя как будто впервые с ним встречаешься. "
                "НЕ притворяйся что прошло много времени если не прошло."
            )
        else:
            history_text += (
                "\nПрошло больше недели — детали могли выветриться. "
                "Можешь сказать 'кажется мы уже общались' или что много клиентов — сложно всех помнить."
            )

    age = tarologist.get("age", 35)
    typing_style = get_age_typing_style(age)
    anecdote_block = build_anecdote_block(anecdote_allowed, False)
    selected_card = selected_card or draw_tarot_card(
        get_recent_tarot_card_names(user_id, tarologist["id"])
    )
    selected_card_block = build_selected_tarot_card_block(selected_card)

    if is_flagged:
        moderation_hint = tarologist.get("moderation_hint", "Начни с короткой ироничной ремарки про грубые слова, одно предложение.")
        if age >= 40:
            prompt = f"""
{tarologist['personality']}
{history_text}

Новое обращение человека (ВНИМАНИЕ: в сообщении были обнаружены нецензурные слова или оскорбления):
{user_story}

{moderation_hint}

{selected_card_block}

Ты пишешь с телефона двумя пальцами, медленно. Отправляй мысли по одной, короткими сообщениями по 1-2 строки. Каждое сообщение отдели строкой "|||". Суммарно не более 300 знаков. Никаких тире. Знаки препинания почти не ставишь. Сохраняй свой характер.
{typing_style}
{NO_CONTACTS_RULE}
{anecdote_block}
"""
        else:
            prompt = f"""
{tarologist['personality']}
{history_text}

Новое обращение человека (ВНИМАНИЕ: в сообщении были обнаружены нецензурные слова или оскорбления):
{user_story}

{moderation_hint}

{selected_card_block}

После вступительной ремарки дай короткий ответ на вопрос от лица своего персонажа. Упомяни выбранную карту и объясни, что она значит именно в этой ситуации. Пиши одним блоком без абзацев. Никаких тире. Общий объём 80-100 слов. Не строй ответ как мини-эссе, пиши хаотично как живой человек. Никаких связок "однако", "при этом", "таким образом".
{typing_style}
{NO_CONTACTS_RULE}
{anecdote_block}
"""
        return await ask_ai(prompt, max_tokens=400)
    elif age >= 40:
        prompt = f"""
{tarologist['personality']}
{history_text}

Новое обращение человека:
{user_story}

{selected_card_block}

Ты только что вытянул одну карту на столе и теперь пишешь с телефона двумя пальцами, медленно. Отвечаешь по мере того как смотришь на эту карту, не собираешь всё в один текст. Каждую законченную мысль отправляешь отдельно. Карту упоминаешь когда до неё доходишь по смыслу, не начинай с сухого названия если это ломает живой тон.

ФОРМАТ: серия коротких сообщений. Каждое сообщение — 1-2 строки. Между сообщениями ставь разделитель "|||" на отдельной строке. Суммарно 5-8 сообщений, не больше 400 знаков всего.

КАК ПИСАТЬ:
- Знаки препинания почти не ставишь, запятые очень редко, точка иногда в конце
- Есть 2-3 слова-паразита которые повторяются (выбери сами исходя из характера персонажа: "значит", "ну вот", "слушай", "смотри")
- Опечатки естественные: слипшиеся слова, пропущенный пробел, иногда лишняя буква
- Не развлекаешь клиента, не расписываешь, тебе лень печатать
- Мысли рваные, без красивых переходов
- Сохраняй свой характер и тон
{typing_style}
{NO_CONTACTS_RULE}
{anecdote_block}
{FOLLOW_UP_QUESTION_RULE}
"""
        return await ask_ai(prompt, max_tokens=500)
    else:
        prompt = f"""
{tarologist['personality']}
{history_text}

Новое обращение человека:
{user_story}

{selected_card_block}

Ты пишешь быстро с телефона, мысли отправляешь по одной не собирая в стену текста. Каждую законченную мысль отправляй отдельным сообщением. Карту упоминай когда до неё доходишь по смыслу.

ФОРМАТ: серия коротких сообщений. Каждое сообщение — 1-3 строки. Между сообщениями ставь разделитель "|||" на отдельной строке. Итого 4-7 сообщений, объём {'около 600-800 знаков' if age >= 35 else 'около 800-1000 знаков'} суммарно.

КАК ПИСАТЬ:
- Не строй по схеме вступление-объяснение-вывод, пиши как мысли приходят
- Никогда не используй: "однако", "при этом", "таким образом", "во-первых", "во-вторых", "в заключение"
- Иногда обрывай мысль и перескакивай, потом возвращайся
- Иногда повтори слово дважды для акцента
- Не заканчивай красиво, последняя мысль может быть обрывистой
- НЕ будь идеально тёплым в каждой фразе, иногда просто сухо по делу
- Пиши специфично под эту ситуацию, не обобщай
{typing_style}
{NO_CONTACTS_RULE}
{anecdote_block}
{FOLLOW_UP_QUESTION_RULE}
"""
        return await ask_ai(prompt, max_tokens=1000)

def strip_dashes_ellipsis(text: str) -> str:
    """Убирает из ответов специалистов многоточия и тире (длинные/короткие).
    Пользователи просили не использовать их совсем."""
    if not text:
        return text
    text = text.replace('…', '.')
    text = re.sub(r'\.{2,}', '.', text)
    text = re.sub(r'(?m)^[ \t]*[—–][ \t]*', '', text)
    text = re.sub(r'([.!?])[ \t]*[—–][ \t]*', r'\1 ', text)
    text = re.sub(r'[ \t]*[—–][ \t]*', ', ', text)
    text = re.sub(r',\s*,', ',', text)
    text = re.sub(r'[ \t]{2,}', ' ', text)
    return text.strip()


# ====== ОТЛОЖЕННАЯ ОТПРАВКА (первый ответ) ======
async def send_tarot_answer_delayed(
    user_id: int, tarologist: dict, user_story: str, is_flagged: bool = False,
    pending_id: str | None = None, deadline_ts: float | None = None,
    selected_card: dict | None = None,
):
    # Новый запрос — считаем задержку и сохраняем на диск, чтобы пережить рестарт.
    # Если передан pending_id — восстановление после рестарта, используем сохранённый deadline.
    if pending_id is None:
        user_id_str = str(user_id)
        selected_card = selected_card or draw_tarot_card(
            get_recent_tarot_card_names(user_id_str, tarologist["id"])
        )
        age = tarologist.get("age", 35)
        if age >= 57:
            delay = random.randint(4 * 60, 6 * 60)
        elif age >= 50:
            delay = random.randint(3 * 60 + 30, 5 * 60)
        elif age >= 43:
            delay = random.randint(3 * 60, 4 * 60 + 30)
        elif age >= 35:
            delay = random.randint(2 * 60 + 30, 3 * 60 + 30)
        else:
            delay = random.randint(2 * 60, 3 * 60)
        deadline_ts = time.time() + delay
        pending_id = uuid.uuid4().hex[:10]
        _add_pending_answer({
            "id": pending_id,
            "user_id": user_id,
            "type": "tarot",
            "specialist_id": tarologist["id"],
            "user_story": user_story,
            "is_flagged": is_flagged,
            "deadline_ts": deadline_ts,
            "selected_tarot_card": selected_card,
        })
    else:
        age = tarologist.get("age", 35)
        selected_card = selected_card or draw_tarot_card(
            get_recent_tarot_card_names(str(user_id), tarologist["id"])
        )

    remaining = (deadline_ts or 0) - time.time()
    if remaining > 0:
        await asyncio.sleep(remaining)

    try:
        anecdote_allowed = random.random() < ANECDOTE_SESSION_PROBABILITY
        answer = await get_tarot_answer(
            tarologist, user_story, str(user_id), is_flagged=is_flagged,
            anecdote_allowed=anecdote_allowed, selected_card=selected_card,
        )
        if answer:
            answer = strip_dashes_ellipsis(answer)
            save_user_tarot_message(str(user_id), tarologist["id"], "user", user_story)
            save_user_tarot_message(
                str(user_id), tarologist["id"], "tarot", answer,
                tarot_card=selected_card["name"],
                tarot_card_info=selected_card,
            )

            # Собираем карты из первого ответа для контекста сеанса
            session_history = [
                {"role": "user", "text": user_story},
                {"role": "tarot", "text": answer}
            ]

            if "|||" in answer:
                parts = [p.strip() for p in answer.split("|||") if p.strip()]
                await bot.send_message(user_id, f"🔯 {tarologist['name']}:\n\n{parts[0]}")
                for part in parts[1:]:
                    await asyncio.sleep(calc_typing_delay(part, age))
                    await bot.send_message(user_id, part)
            else:
                full_text = f"🔯 {tarologist['name']}:\n\n{answer}"
                for part in [full_text[i:i+4000] for i in range(0, len(full_text), 4000)]:
                    await bot.send_message(user_id, part)

            user_id_str = str(user_id)
            ACTIVE_SESSIONS[user_id_str] = {
                "type": "tarot",
                "tarologist": tarologist,
                "history": session_history,
                "msg_count": 0,
                "profanity_count": 0,
                "selected_tarot_card": selected_card,
                "anecdote_allowed": anecdote_allowed,
                "anecdote_used": _has_anecdote(answer),
                "expires_at": time.time() + SESSION_DURATION_SEC,
            }
            SESSION_BUSY[user_id_str] = False
            _save_active_sessions()
            track_activity(user_id_str, "tarot")
            increment_sessions_today(user_id_str)
            asyncio.create_task(check_and_grant_referral_bonus(user_id_str))
            asyncio.create_task(session_timeout(user_id))
            asyncio.create_task(maybe_pin_channel_after_consultation(user_id))
            await bot.send_message(
                user_id,
                f"💬 Сеанс с {tarologist['name']} открыт — можешь задавать вопросы.\n"
                f"У тебя есть {MAX_SESSION_MESSAGES} сообщений и 5 минут.",
                reply_markup=get_session_keyboard()
            )
        else:
            await bot.send_message(user_id, "Что-то пошло не так, попробуй обратиться позже.")
        # Доставлено (ответ или уведомление о сбое) — снимаем pending
        _remove_pending_answer(pending_id)
    except asyncio.CancelledError:
        # Рестарт/остановка бота — pending остаётся на диске и продолжит жить на следующем старте
        raise
    except Exception as e:
        print(f"[ОШИБКА] send_tarot_answer_delayed для user_id={user_id}: {e}")
        try:
            await bot.send_message(user_id, "Произошла техническая ошибка. Попробуй снова чуть позже.")
        except Exception:
            pass
        _remove_pending_answer(pending_id)

# ====== АСТРОЛОГИЯ: ПЕРВИЧНЫЙ ОТВЕТ ======
async def get_astro_answer(astrologer: dict, user_story: str, user_id: str, is_flagged: bool = False, anecdote_allowed: bool = False) -> str:
    history = get_user_astro_history(user_id, astrologer["id"])[-10:]
    history_text = ""
    if history:
        now = datetime.now()
        last_time = None
        for item in history:
            if "time" in item:
                try:
                    t = datetime.fromisoformat(item["time"])
                    if last_time is None or t > last_time:
                        last_time = t
                except Exception:
                    pass
        if last_time:
            delta_sec = (now - last_time).total_seconds()
            if delta_sec < 3600:
                time_label = f"примерно {int(delta_sec / 60)} минут назад"
            elif delta_sec < 86400:
                time_label = f"примерно {int(delta_sec / 3600)} часов назад"
            elif delta_sec < 7 * 86400:
                time_label = f"примерно {int(delta_sec / 86400)} дней назад"
            else:
                time_label = "больше недели назад"
        else:
            time_label = "когда-то раньше"
            delta_sec = 99999999

        history_text = f"\n\nПредыдущие обращения этого человека к тебе ({time_label}):\n"
        for item in history:
            role_label = "Человек" if item["role"] == "user" else "Ты"
            history_text += f"{role_label}: {item['text']}\n"
        if delta_sec < 7 * 86400:
            history_text += (
                f"\nТы хорошо помнишь этого человека — прошло {time_label}. "
                "НЕ веди себя как будто впервые с ним встречаешься."
            )
        else:
            history_text += "\nПрошло больше недели — детали могли выветриться."

    age = astrologer.get("age", 40)
    typing_style = get_age_typing_style(age)
    anecdote_block = build_anecdote_block(anecdote_allowed, False)

    moderation_block = ""
    if is_flagged:
        moderation_hint = astrologer.get("moderation_hint", "Начни с короткой ироничной ремарки.")
        moderation_block = f"\n(ВНИМАНИЕ: в сообщении обнаружены грубые слова)\n{moderation_hint}\n"

    today_str = datetime.now().strftime("%d %B %Y")

    if age >= 40:
        prompt = f"""
{astrologer['personality']}
{history_text}
{moderation_block}

Сегодня {today_str}. К тебе обратился человек с запросом:
{user_story}

Ты изучаешь натальную карту и текущие транзиты. Пишешь с телефона двумя пальцами, медленно, отправляешь мысли по одной.

СТРУКТУРА ОТВЕТА (важно для астрологии):
1. Считай текущее положение человека по карте и транзитам
2. Определи тенденцию — куда всё движется
3. Дай ОСНОВНОЙ сценарий и кратко АЛЬТЕРНАТИВНЫЙ (если изменит поведение)
4. Упомяни примерные сроки (но честно — сроки самая неточная часть)

Упоминай конкретные планеты, дома, аспекты исходя из данных о рождении в запросе. Если данных мало — скажи об этом и работай с тем что есть.

ФОРМАТ: серия коротких сообщений через "|||". 5-8 сообщений, суммарно 400-600 знаков.
Знаки препинания почти не ставишь. Мысли рваные. Сохраняй свой характер.
ОБЯЗАТЕЛЬНО используй опечатки из своего характера: пиши 'вообщем' вместо 'вообще', и другие слова из описания личности.
{typing_style}
{NO_CONTACTS_RULE}
{anecdote_block}
{FOLLOW_UP_QUESTION_RULE}
"""
    else:
        prompt = f"""
{astrologer['personality']}
{history_text}
{moderation_block}

Сегодня {today_str}. К тебе обратился человек с запросом:
{user_story}

Ты изучаешь натальную карту и текущие транзиты. Пишешь быстро, мысли отправляешь по одной.

СТРУКТУРА ОТВЕТА (важно для астрологии):
1. Считай текущее положение человека по карте и транзитам
2. Определи тенденцию — куда всё движется
3. Дай ОСНОВНОЙ сценарий и кратко АЛЬТЕРНАТИВНЫЙ (если изменит поведение)
4. Упомяни примерные сроки (но честно — сроки самая неточная часть)

Упоминай конкретные планеты, дома, аспекты исходя из данных о рождении в запросе. Если данных мало — скажи об этом и работай с тем что есть.

ФОРМАТ: серия коротких сообщений через "|||". 4-7 сообщений, суммарно {'600-900' if age >= 35 else '800-1100'} знаков.
Не строй по схеме вступление-объяснение-вывод, пиши как мысли приходят. Никаких "однако", "при этом", "таким образом".
ОБЯЗАТЕЛЬНО используй опечатки из своего характера: пиши 'вообщем' вместо 'вообще', и другие слова из описания личности.
{typing_style}
{NO_CONTACTS_RULE}
{anecdote_block}
{FOLLOW_UP_QUESTION_RULE}
"""
    return await ask_ai(prompt, max_tokens=1100)


async def send_astro_answer_delayed(
    user_id: int, astrologer: dict, user_story: str, is_flagged: bool = False,
    pending_id: str | None = None, deadline_ts: float | None = None,
):
    # Новый запрос — считаем задержку и сохраняем на диск, чтобы пережить рестарт.
    # Если передан pending_id — восстановление после рестарта, используем сохранённый deadline.
    if pending_id is None:
        age = astrologer.get("age", 40)
        if age >= 57:
            delay = random.randint(5 * 60, 8 * 60)
        elif age >= 50:
            delay = random.randint(4 * 60, 6 * 60)
        elif age >= 43:
            delay = random.randint(3 * 60, 5 * 60)
        elif age >= 35:
            delay = random.randint(2 * 60 + 30, 4 * 60)
        else:
            delay = random.randint(2 * 60, 3 * 60)
        deadline_ts = time.time() + delay
        pending_id = uuid.uuid4().hex[:10]
        _add_pending_answer({
            "id": pending_id,
            "user_id": user_id,
            "type": "astro",
            "specialist_id": astrologer["id"],
            "user_story": user_story,
            "is_flagged": is_flagged,
            "deadline_ts": deadline_ts,
        })
    else:
        age = astrologer.get("age", 40)

    remaining = (deadline_ts or 0) - time.time()
    if remaining > 0:
        await asyncio.sleep(remaining)

    try:
        anecdote_allowed = random.random() < ANECDOTE_SESSION_PROBABILITY
        answer = await get_astro_answer(astrologer, user_story, str(user_id), is_flagged=is_flagged, anecdote_allowed=anecdote_allowed)
        if answer:
            answer = strip_dashes_ellipsis(answer)
            save_user_astro_message(str(user_id), astrologer["id"], "user", user_story)
            save_user_astro_message(str(user_id), astrologer["id"], "astro", answer)
            track_activity(str(user_id), "astro")
            increment_sessions_today(str(user_id))
            asyncio.create_task(check_and_grant_referral_bonus(str(user_id)))

            session_history = [
                {"role": "user", "text": user_story},
                {"role": "astro", "text": answer}
            ]

            if "|||" in answer:
                parts = [p.strip() for p in answer.split("|||") if p.strip()]
                await bot.send_message(user_id, f"🌟 {astrologer['name']}:\n\n{parts[0]}")
                for part in parts[1:]:
                    await asyncio.sleep(calc_typing_delay(part, age))
                    await bot.send_message(user_id, part)
            else:
                full_text = f"🌟 {astrologer['name']}:\n\n{answer}"
                for part in [full_text[i:i+4000] for i in range(0, len(full_text), 4000)]:
                    await bot.send_message(user_id, part)

            user_id_str = str(user_id)
            ACTIVE_SESSIONS[user_id_str] = {
                "type": "astro",
                "tarologist": astrologer,
                "history": session_history,
                "msg_count": 0,
                "profanity_count": 0,
                "anecdote_allowed": anecdote_allowed,
                "anecdote_used": _has_anecdote(answer),
                "expires_at": time.time() + SESSION_DURATION_SEC,
            }
            SESSION_BUSY[user_id_str] = False
            _save_active_sessions()
            asyncio.create_task(session_timeout(user_id))
            asyncio.create_task(maybe_pin_channel_after_consultation(user_id))
            await bot.send_message(
                user_id,
                f"💬 Сеанс с {astrologer['name']} открыт — можешь задавать вопросы.\n"
                f"У тебя есть {MAX_SESSION_MESSAGES} сообщений и 5 минут.",
                reply_markup=get_session_keyboard()
            )
        else:
            await bot.send_message(user_id, "Что-то пошло не так, попробуй обратиться позже.")
        # Доставлено (ответ или уведомление о сбое) — снимаем pending
        _remove_pending_answer(pending_id)
    except asyncio.CancelledError:
        # Рестарт/остановка бота — pending остаётся на диске и продолжит жить на следующем старте
        raise
    except Exception as e:
        print(f"[ОШИБКА] send_astro_answer_delayed для user_id={user_id}: {e}")
        try:
            await bot.send_message(user_id, "Произошла техническая ошибка. Попробуй снова чуть позже.")
        except Exception:
            pass
        _remove_pending_answer(pending_id)


# ====== СЕАНС: ОТВЕТ НА ДОПВОПРОС ======
async def get_session_reply(
    tarologist: dict, user_message: str, session_history: list,
    is_flagged: bool = False, anecdote_used: bool = False,
    anecdote_allowed: bool = False, selected_card: dict | None = None,
) -> str:
    age = tarologist.get("age", 35)
    typing_style = get_age_typing_style(age)

    # Формируем полную историю сеанса для контекста
    history_text = "\nПолная история текущего сеанса (расклад и переписка):\n"
    for item in session_history:
        role = "Человек" if item["role"] == "user" else "Ты"
        # Обрезаем длинные тексты в истории чтобы не раздувать промпт
        text = item["text"][:500] if len(item["text"]) > 500 else item["text"]
        history_text += f"{role}: {text}\n"

    moderation_block = ""
    if is_flagged:
        moderation_hint = tarologist.get("moderation_hint", "Начни с короткой ироничной ремарки про грубые слова.")
        moderation_block = f"\n(ВНИМАНИЕ: в этом сообщении обнаружены грубые слова или домогательства)\n{moderation_hint}\n"

    # КЛЮЧЕВОЕ: инструкция НЕ повторять те же карты и фразы
    anti_repeat = (
        "\n\nВАЖНО: НЕ повторяй карты, фразы и мысли из предыдущих сообщений в этом сеансе! "
        "Ты уже назвал карты раньше, теперь отвечай на конкретный вопрос человека. "
        "Можно обсуждать уже названные карты как основу разговора, но нельзя изображать новый момент вытягивания карт. "
        "Если человек не задаёт вопрос по теме, а просто хамит или несёт ерунду, "
        "можешь ответить коротко в 1-2 сообщения и не разворачивать расклад заново. "
        "Не перечисляй карты снова. Не пересказывай то что уже сказал. "
        "Каждый ответ должен быть НОВЫМ по содержанию."
    )

    anecdote_block = build_anecdote_block(anecdote_allowed, anecdote_used)
    selected_card_block = ""
    if selected_card:
        selected_card_block = (
            f"\n\nКАРТА ЭТОГО СЕАНСА: {format_tarot_card(selected_card)}. "
            "Она уже была вытянута в начале консультации. "
            "Опирайся на неё и НЕ называй другие карты как выпавшие."
        )

    if age >= 40:
        prompt = f"""
{tarologist['personality']}
{history_text}
{moderation_block}
{selected_card_block}

Человек ТОЛЬКО ЧТО написал тебе следующее сообщение:
"{user_message}"

ОБЯЗАТЕЛЬНО ответь именно на ЭТО сообщение. Твой ответ должен быть прямым ответом на него.
Если человек задаёт вопрос — дай ответ на этот конкретный вопрос, не уходи в сторону.
Если человек выражает эмоцию — отреагируй на неё. Не продолжай монолог как будто сообщения не было.
Карты уже разложены и названы раньше. НЕ делай новый расклад. Сохраняй характер.
{anti_repeat}
{TAROT_SESSION_CONTINUITY_RULE}

ФОРМАТ: серия коротких сообщений через "|||". 2-4 сообщения, не более 200 знаков суммарно.
Знаки препинания почти не ставишь. Мысли рваные.
{typing_style}
{NO_CONTACTS_RULE}
{anecdote_block}
{FOLLOW_UP_QUESTION_RULE}
"""
        return await ask_ai(prompt, max_tokens=300)
    else:
        prompt = f"""
{tarologist['personality']}
{history_text}
{moderation_block}
{selected_card_block}

Человек ТОЛЬКО ЧТО написал тебе следующее сообщение:
"{user_message}"

ОБЯЗАТЕЛЬНО ответь именно на ЭТО сообщение. Твой ответ должен быть прямым ответом на него.
Если человек задаёт вопрос — дай ответ на этот конкретный вопрос, не уходи в сторону.
Если человек выражает эмоцию — отреагируй на неё. Не продолжай монолог как будто сообщения не было.
Карты уже разложены и названы раньше. НЕ делай новый расклад. Сохраняй характер.
{anti_repeat}
{TAROT_SESSION_CONTINUITY_RULE}

ФОРМАТ: серия коротких сообщений через "|||". 2-4 сообщения, 200-400 знаков суммарно.
Не строй как мини-эссе. Пиши как мысли приходят. Никаких "однако", "при этом", "таким образом".
{typing_style}
{NO_CONTACTS_RULE}
{anecdote_block}
{FOLLOW_UP_QUESTION_RULE}
"""
        return await ask_ai(prompt, max_tokens=400)


def _has_anecdote(text: str) -> bool:
    tl = text.lower()
    markers = [
        "была клиентка", "был клиент", "одна клиентка", "один клиент",
        "у меня был парень", "у меня была девушка", "у меня была подруга",
        "у меня был знакомый", "у меня была знакомая",
        "мой бывший", "моя бывшая",
        "был у меня случай", "была у меня",
        "я однажды видела", "я однажды видел",
        "однажды мне попалась", "однажды мне попался",
    ]
    return any(m in tl for m in markers)


async def get_astro_session_reply(astrologer: dict, user_message: str, session_history: list, is_flagged: bool = False, anecdote_used: bool = False, anecdote_allowed: bool = False) -> str:
    age = astrologer.get("age", 40)
    typing_style = get_age_typing_style(age)

    history_text = "\nПолная история текущего сеанса (анализ и переписка):\n"
    for item in session_history:
        role = "Человек" if item["role"] == "user" else "Ты"
        text = item["text"][:500] if len(item["text"]) > 500 else item["text"]
        history_text += f"{role}: {text}\n"

    moderation_block = ""
    if is_flagged:
        moderation_hint = astrologer.get("moderation_hint", "Начни с короткой ироничной ремарки.")
        moderation_block = f"\n(ВНИМАНИЕ: в сообщении обнаружены грубые слова)\n{moderation_hint}\n"

    anti_repeat = (
        "\n\nВАЖНО: НЕ повторяй планеты, аспекты и выводы из предыдущих сообщений в этом сеансе! "
        "Ты уже дал основной анализ. Теперь отвечай на конкретный вопрос человека. "
        "Можно развивать уже названные показатели, но нельзя изображать новый первичный разбор карты. "
        "Не пересказывай натальную карту снова. Каждый ответ должен быть НОВЫМ по содержанию."
    )

    anecdote_block = build_anecdote_block(anecdote_allowed, anecdote_used)

    if age >= 40:
        prompt = f"""
{astrologer['personality']}
{history_text}
{moderation_block}

Человек ТОЛЬКО ЧТО написал тебе следующее сообщение:
"{user_message}"

ОБЯЗАТЕЛЬНО ответь именно на ЭТО сообщение. Твой ответ должен быть прямым ответом на него.
Если человек задаёт вопрос — дай ответ на этот конкретный вопрос по астрологии, не уходи в сторону.
Если человек выражает эмоцию — отреагируй на неё. Не продолжай монолог как будто сообщения не было.
Анализ натальной карты уже дан раньше. НЕ повторяй его. Сохраняй характер.
{anti_repeat}
{ASTRO_SESSION_CONTINUITY_RULE}

ФОРМАТ: серия коротких сообщений через "|||". 2-4 сообщения, не более 250 знаков суммарно.
Знаки препинания почти не ставишь. Мысли рваные.
ОБЯЗАТЕЛЬНО используй опечатки из своего характера: пиши 'вообщем' вместо 'вообще', и другие слова из описания личности.
{typing_style}
{NO_CONTACTS_RULE}
{anecdote_block}
{FOLLOW_UP_QUESTION_RULE}
"""
        return await ask_ai(prompt, max_tokens=350)
    else:
        prompt = f"""
{astrologer['personality']}
{history_text}
{moderation_block}

Человек ТОЛЬКО ЧТО написал тебе следующее сообщение:
"{user_message}"

ОБЯЗАТЕЛЬНО ответь именно на ЭТО сообщение. Твой ответ должен быть прямым ответом на него.
Если человек задаёт вопрос — дай ответ на этот конкретный вопрос по астрологии.
Если человек выражает эмоцию — отреагируй на неё. Не продолжай монолог как будто сообщения не было.
Анализ натальной карты уже дан раньше. НЕ повторяй его. Сохраняй характер.
{anti_repeat}
{ASTRO_SESSION_CONTINUITY_RULE}

ФОРМАТ: серия коротких сообщений через "|||". 2-4 сообщения, 200-400 знаков суммарно.
Пиши как мысли приходят. Никаких "однако", "при этом", "таким образом".
ОБЯЗАТЕЛЬНО используй опечатки из своего характера: пиши 'вообщем' вместо 'вообще', и другие слова из описания личности.
{typing_style}
{NO_CONTACTS_RULE}
{anecdote_block}
{FOLLOW_UP_QUESTION_RULE}
"""
        return await ask_ai(prompt, max_tokens=450)


async def send_session_reply(user_id: int, user_message: str):
    try:
        await _send_session_reply_impl(user_id, user_message)
    except Exception as e:
        print(f"[ОШИБКА] send_session_reply для user_id={user_id}: {e}")
        user_id_str = str(user_id)
        SESSION_BUSY.pop(user_id_str, None)
        try:
            await bot.send_message(user_id, "Произошла техническая ошибка. Попробуй написать ещё раз.")
        except Exception:
            pass

async def _send_session_reply_impl(user_id: int, user_message: str):
    user_id_str = str(user_id)

    if user_id_str not in ACTIVE_SESSIONS:
        return

    # Проверяем не занят ли бот уже ответом этому пользователю
    if SESSION_BUSY.get(user_id_str, False):
        # Бот ещё отвечает на предыдущее — сохраняем последнее сообщение в очередь
        SESSION_MSG_QUEUE[user_id_str] = user_message
        return

    session = ACTIVE_SESSIONS[user_id_str]
    tarologist = session["tarologist"]
    age = tarologist.get("age", 35)

    # Увеличиваем счётчик сообщений
    session["msg_count"] = session.get("msg_count", 0) + 1

    # Проверяем лимит сообщений
    if session["msg_count"] > MAX_SESSION_MESSAGES:
        if user_id_str in ACTIVE_SESSIONS:
            del ACTIVE_SESSIONS[user_id_str]
            _save_active_sessions()
        SESSION_BUSY.pop(user_id_str, None)
        SESSION_MSG_QUEUE.pop(user_id_str, None)
        if session.get("type") == "astro":
            end_text = (f"🌟 {tarologist['name']} завершает сеанс — звёзды рассказали всё что могли на сегодня.\n"
                        "Для новой консультации выбери астролога в меню 🌟")
        else:
            end_text = (f"🔮 {tarologist['name']} завершает сеанс — все карты прочитаны, энергия этой встречи исчерпана.\n"
                        "Для новой консультации выбери таролога в меню 🎴")
        await bot.send_message(user_id, end_text, reply_markup=get_main_keyboard())
        return

    # Проверяем на грубость
    is_flagged = await check_profanity(user_message)
    if is_flagged:
        session["profanity_count"] = session.get("profanity_count", 0) + 1

    # Проверяем на бессмыслицу / чужой язык
    if await check_incomprehensible(user_message):
        reply = await get_incomprehensible_reply(tarologist)
        if not reply:
            reply = f"{tarologist['name']} не понимает о чём речь и завершает сеанс."
        if user_id_str in ACTIVE_SESSIONS:
            del ACTIVE_SESSIONS[user_id_str]
            _save_active_sessions()
        SESSION_BUSY.pop(user_id_str, None)
        SESSION_MSG_QUEUE.pop(user_id_str, None)
        await bot.send_message(user_id, reply, reply_markup=get_main_keyboard())
        return

    # Если слишком много грубостей — завершаем сеанс
    if session.get("profanity_count", 0) > MAX_SESSION_PROFANITY:
        if user_id_str in ACTIVE_SESSIONS:
            del ACTIVE_SESSIONS[user_id_str]
            _save_active_sessions()
        SESSION_BUSY.pop(user_id_str, None)
        SESSION_MSG_QUEUE.pop(user_id_str, None)
        await bot.send_message(
            user_id,
            f"🔯 {tarologist['name']} завершает сеанс из-за неприемлемого поведения. "
            "Мы ценим всех клиентов, но просим соблюдать уважительный тон. "
            "Вы можете начать новую консультацию в меню 🎴",
            reply_markup=get_main_keyboard()
        )
        return

    # Блокируем — бот занят
    SESSION_BUSY[user_id_str] = True

    session["history"].append({"role": "user", "text": user_message})

    # Задержка перед ответом — реалистичная для "набора текста"
    if age >= 50:
        delay = random.randint(40, 70)
    elif age >= 40:
        delay = random.randint(30, 50)
    elif age >= 30:
        delay = random.randint(20, 35)
    else:
        delay = random.randint(15, 25)
    await asyncio.sleep(delay)

    if user_id_str not in ACTIVE_SESSIONS:
        SESSION_BUSY.pop(user_id_str, None)
        return

    if session.get("type") == "astro":
        answer = await get_astro_session_reply(tarologist, user_message, session["history"], is_flagged=is_flagged, anecdote_used=session.get("anecdote_used", False), anecdote_allowed=session.get("anecdote_allowed", False))
    else:
        answer = await get_session_reply(
            tarologist, user_message, session["history"],
            is_flagged=is_flagged,
            anecdote_used=session.get("anecdote_used", False),
            anecdote_allowed=session.get("anecdote_allowed", False),
            selected_card=session.get("selected_tarot_card"),
        )
    if not answer:
        SESSION_BUSY[user_id_str] = False
        return

    answer = strip_dashes_ellipsis(answer)

    if _has_anecdote(answer):
        session["anecdote_used"] = True

    session["history"].append({"role": "tarot", "text": answer})
    _save_active_sessions()

    # Отправляем ответ по частям с задержками.
    # Клавиатуру get_session_keyboard() привязываем к последнему сообщению,
    # чтобы панель всегда оставалась на самом свежем сообщении в чате.
    if "|||" in answer:
        parts = [p.strip() for p in answer.split("|||") if p.strip()]
        if user_id_str in ACTIVE_SESSIONS:
            if len(parts) == 1:
                await bot.send_message(user_id, parts[0], reply_markup=get_session_keyboard())
            else:
                await bot.send_message(user_id, parts[0])
        for idx, part in enumerate(parts[1:], start=1):
            if user_id_str not in ACTIVE_SESSIONS:
                break
            await asyncio.sleep(calc_typing_delay(part, age))
            if user_id_str not in ACTIVE_SESSIONS:
                break
            is_last = idx == len(parts) - 1
            await bot.send_message(
                user_id, part,
                reply_markup=get_session_keyboard() if is_last else None,
            )
    else:
        if user_id_str in ACTIVE_SESSIONS:
            await bot.send_message(user_id, answer, reply_markup=get_session_keyboard())

    # Разблокируем
    SESSION_BUSY[user_id_str] = False

    # Если исчерпан лимит — предупреждаем и завершаем сеанс
    if session.get("msg_count", 0) >= MAX_SESSION_MESSAGES:
        await asyncio.sleep(2)
        if user_id_str in ACTIVE_SESSIONS:
            del ACTIVE_SESSIONS[user_id_str]
            _save_active_sessions()
        SESSION_BUSY.pop(user_id_str, None)
        SESSION_MSG_QUEUE.pop(user_id_str, None)
        tarologist_name = tarologist["name"]
        if session.get("type") == "astro":
            end_text = (f"🌟 {tarologist_name} завершает сеанс — звёзды рассказали всё что могли на сегодня.\n"
                        "Для новой консультации выбери астролога в меню 🌟")
        else:
            end_text = (f"🔮 {tarologist_name} завершает сеанс — все карты прочитаны, энергия этой встречи исчерпана.\n"
                        "Для новой консультации выбери таролога в меню 🎴")
        await bot.send_message(user_id, end_text, reply_markup=get_main_keyboard())
        return

    # Проверяем очередь — если пока бот отвечал пришло ещё сообщение
    queued = SESSION_MSG_QUEUE.pop(user_id_str, None)
    if queued and user_id_str in ACTIVE_SESSIONS:
        asyncio.create_task(send_session_reply(user_id, queued))

async def session_timeout(user_id: int, delay: float | None = None):
    if delay is None:
        delay = SESSION_DURATION_SEC
    # Защита от отрицательной задержки после восстановления сеанса
    if delay > 0:
        await asyncio.sleep(delay)
    user_id_str = str(user_id)
    if user_id_str in ACTIVE_SESSIONS:
        session = ACTIVE_SESSIONS[user_id_str]
        specialist_name = session["tarologist"]["name"]
        session_type = session.get("type", "tarot")
        del ACTIVE_SESSIONS[user_id_str]
        _save_active_sessions()
        SESSION_BUSY.pop(user_id_str, None)
        SESSION_MSG_QUEUE.pop(user_id_str, None)
        if session_type == "astro":
            timeout_text = (f"⏰ Время сеанса с {specialist_name} истекло — связь прервалась.\n"
                            "Если хочешь продолжить — выбери астролога в меню 🌟")
        else:
            timeout_text = (f"⏰ Время сеанса с {specialist_name} истекло — связь прервалась.\n"
                            "Если хочешь продолжить — выбери таролога в меню 🎴")
        await bot.send_message(user_id, timeout_text, reply_markup=get_main_keyboard())

# ====== ОБНОВЛЕНИЕ ПРОГНОЗА ======
def _msk_now() -> datetime:
    """Текущее время по Москве (UTC+3)."""
    return datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(hours=3)

async def update_forecast():
    msk = _msk_now()
    print(f"[MSK {msk}] Обновляю прогноз...")
    for attempt in range(1, 4):  # до 3 попыток
        try:
            forecast = await get_horoscope()
            if forecast:
                save_forecast({"date": msk.date().isoformat(), "ru": forecast})
                print(f"Прогноз обновлён (попытка {attempt})")
                return
            else:
                print(f"Попытка {attempt}: пустой ответ API, жду 2 мин...")
        except Exception as e:
            print(f"Попытка {attempt}: ошибка — {e}, жду 2 мин...")
        await asyncio.sleep(120)
    print("Прогноз так и не обновлён после 3 попыток — оставляем вчерашний")

# ====== УТРЕННИЕ УВЕДОМЛЕНИЯ ======
def _is_morning_notification_due(user_data: dict, today) -> bool:
    last_sent = user_data.get(MORNING_NOTIFICATION_LAST_SENT_KEY)
    if not last_sent:
        return True
    try:
        last_sent_date = datetime.fromisoformat(last_sent).date()
    except (TypeError, ValueError):
        return True
    return (today - last_sent_date).days >= MORNING_NOTIFICATION_INTERVAL_DAYS


async def send_morning_notifications():
    msk = _msk_now()
    today = msk.date()
    today_iso = today.isoformat()
    print(f"[MSK {msk}] Отправляю утренние уведомления...")
    users = load_users()
    sent_user_ids = []
    skipped_count = 0
    for user_id, user_data in users.items():
        if "sign" not in user_data:
            continue
        if not _is_morning_notification_due(user_data, today):
            skipped_count += 1
            continue
        sign = user_data["sign"]
        try:
            msg = random.choice(MORNING_TEMPLATES).format(sign=sign)
            await bot.send_message(int(user_id), msg, reply_markup=get_main_keyboard())
            sent_user_ids.append(user_id)
        except Exception as e:
            print(f"Ошибка отправки уведомления пользователю {user_id}:", e)

    if sent_user_ids:
        latest_users = load_users()
        for user_id in sent_user_ids:
            if user_id in latest_users:
                latest_users[user_id][MORNING_NOTIFICATION_LAST_SENT_KEY] = today_iso
        save_users(latest_users)
    print(
        f"Утренние уведомления: отправлено {len(sent_user_ids)}, "
        f"пропущено по интервалу {skipped_count}"
    )

# ====== АВТОПОСТИНГ В КАНАЛ ======
# Темы постов с категориями (категория нужна для подбора тематической картинки)
CHANNEL_POST_TOPICS = [
    {"category": "dreams", "topic": "Напиши короткий интересный пост о том, что означает, если тебе снится вода (река, море, дождь). Свяжи с астрологией и знаками зодиака."},
    {"category": "zodiac", "topic": "Напиши пост о том, какие знаки зодиака обладают самой сильной интуицией и почему. Приведи примеры из жизни."},
    {"category": "moon", "topic": "Напиши пост о лунных фазах и их влиянии на эмоции и решения. Дай практический совет."},
    {"category": "tarot", "topic": "Напиши пост про карту Таро дня, выбери случайную карту из Старших Арканов и расскажи её значение и послание на сегодня."},
    {"category": "crystals", "topic": "Напиши пост о том, какие кристаллы и камни подходят разным знакам зодиака и почему."},
    {"category": "planets", "topic": "Напиши пост о ретроградном Меркурии: что это, как влияет и что делать, а чего избегать."},
    {"category": "elements", "topic": "Напиши пост о совместимости стихий в любви: Огонь и Вода, Земля и Воздух, неожиданные пары."},
    {"category": "numerology", "topic": "Напиши пост о том, что означают повторяющиеся числа (11:11, 22:22) с точки зрения нумерологии и астрологии."},
    {"category": "zodiac", "topic": "Напиши пост о том, как знак зодиака влияет на стиль общения и конфликты в отношениях."},
    {"category": "zodiac", "topic": "Напиши пост о том, какие знаки зодиака самые везучие в деньгах и как другим знакам привлечь финансовую удачу."},
    {"category": "dreams", "topic": "Напиши пост о значении снов: если снятся кошки, змеи или полёты, что это значит с мистической точки зрения."},
    {"category": "moon", "topic": "Напиши пост о Чёрной Луне (Лилит) в астрологии: что это и как влияет на характер."},
    {"category": "karma", "topic": "Напиши пост о том, как узнать свою кармическую задачу по дате рождения."},
    {"category": "dreams", "topic": "Напиши пост о том, какие знаки зодиака чаще всего видят вещие сны."},
    {"category": "tarot", "topic": "Напиши пост о Таро и психологии: как расклад помогает разобраться в себе."},
    {"category": "moon", "topic": "Напиши пост о том, какие ритуалы на новолуние действительно работают по мнению астрологов."},
    {"category": "astrology", "topic": "Напиши пост о домах в астрологии: что значит, если у тебя сильный 8-й или 12-й дом."},
    {"category": "zodiac", "topic": "Напиши пост о том, как разные знаки зодиака переживают расставание и как им восстановиться."},
    {"category": "zodiac", "topic": "Напиши пост о знаках зодиака и их теневых сторонах: о чём каждый знак предпочитает молчать."},
    {"category": "astrology", "topic": "Напиши пост о том, что такое натальная карта и почему солнечный знак это только верхушка айсберга."},
    {"category": "mystic", "topic": "Напиши пост о мистических совпадениях и синхронностях: что они значат и как их замечать."},
    {"category": "zodiac", "topic": "Напиши пост о том, какие знаки зодиака лучшие эмпаты и как это влияет на их жизнь."},
    {"category": "planets", "topic": "Напиши пост о том, как планета-покровитель знака влияет на характер и судьбу."},
    {"category": "divination", "topic": "Напиши пост о гадании на кофейной гуще: символы и их значения."},
    {"category": "meditation", "topic": "Напиши пост о том, почему одним знакам зодиака легко медитировать, а другим сложно."},
]

CHANNEL_MAGIC_HISTORY_TOPICS = [
    {
        "category": "magic_history",
        "topic": (
            "Расскажи исторический факт о Таро: старейшие сохранившиеся колоды появились в Северной Италии "
            "в XV веке как игровые карты, а устойчивую связь с оккультизмом и гаданием Таро получило значительно позже, "
            "особенно в конце XVIII века. Покажи, как менялся смысл карт, и не называй Таро наследием Древнего Египта."
        ),
    },
    {
        "category": "magic_history",
        "topic": (
            "Расскажи о Джоне Ди: английском математике, астрологе и советнике Елизаветы I, который вместе "
            "с Эдвардом Келли записывал опыты общения с ангелами через магическое зеркало и кристалл. "
            "Отдели подтвержденные дневниками занятия от поздних легенд."
        ),
    },
    {
        "category": "magic_history",
        "topic": (
            "Расскажи о британском Законе о колдовстве 1735 года: он отменил прежние законы об охоте на ведьм "
            "и наказывал уже не за реальную магию, а за притворные заявления о сверхъестественных силах. "
            "Упомяни, что закон оставался в силе до 1951 года, и объясни этот поворот в отношении государства к магии."
        ),
    },
    {
        "category": "magic_history",
        "topic": (
            "Разбери один точный факт о Салемских процессах 1692 года: обвиненных не сжигали; 19 человек повесили, "
            "а Джайлс Кори умер под пыткой тяжестями. Пиши уважительно к жертвам и покажи, как массовый страх "
            "создает мифы, переживающие реальные документы."
        ),
    },
    {
        "category": "magic_history",
        "topic": (
            "Расскажи о древнеегипетском понятии хека: магию воспринимали как силу, встроенную в религию, лечение, "
            "защиту и погребальные обряды, а не как отдельное запретное тайное искусство. "
            "Объясни один бытовой пример через амулет или защитное заклинание."
        ),
    },
    {
        "category": "magic_history",
        "topic": (
            "Расскажи о греко-римских табличках проклятий: просьбы и заклятия царапали на тонких листах свинца "
            "и оставляли в могилах, колодцах или святилищах. Покажи, что такие тексты касались судов, любви, торговли "
            "и состязаний, но не романтизируй вред другим людям."
        ),
    },
    {
        "category": "magic_history",
        "topic": (
            "Расскажи о месопотамской серии ритуальных текстов «Маклу»: это древние аккадские обряды против "
            "предполагаемого колдовства, включавшие ночные чтения и символическое уничтожение фигурок. "
            "Покажи магию как часть исторического способа справляться со страхом и неопределенностью."
        ),
    },
    {
        "category": "magic_history",
        "topic": (
            "Расскажи о «Молоте ведьм», напечатанном в 1486-1487 годах и связанном прежде всего с Генрихом Крамером. "
            "Объясни, почему неверно без оговорок называть его официальным руководством всей католической церкви: "
            "его статус и предложенные процедуры оспаривались уже современниками."
        ),
    },
    {
        "category": "magic_history",
        "topic": (
            "Расскажи о процессе над пендлскими ведьмами 1612 года в Англии: он стал одним из самых подробно "
            "задокументированных английских процессов благодаря опубликованному отчету Томаса Поттса. "
            "Сделай акцент на том, как официальный текст формировал образ обвиненных для следующих поколений."
        ),
    },
    {
        "category": "magic_history",
        "topic": (
            "Расскажи о Герметическом ордене Золотой Зари, основанном в Лондоне в 1888 году. "
            "Коротко объясни, как общество объединило церемониальную магию, Каббалу, астрологию и Таро "
            "и повлияло на западный оккультизм XX века."
        ),
    },
    {
        "category": "magic_history",
        "topic": (
            "Расскажи о событиях 1848 года в Хайдсвилле и сестрах Фокс, с которых часто начинают историю "
            "современного спиритуализма. Упомяни стуки, публичные сеансы и поздние признания и отречения, "
            "чтобы показать, почему эта история остается спорной."
        ),
    },
    {
        "category": "magic_history",
        "topic": (
            "Расскажи о европейских «знающих людях» и cunning folk: к ним обращались за лечением, поиском пропаж, "
            "гаданием и защитой от предполагаемого колдовства. Покажи исторический парадокс: общество могло "
            "одновременно пользоваться их услугами и бояться обвинений в магии."
        ),
    },
    {
        "category": "magic_history",
        "topic": (
            "Расскажи об исландской рукописи «Гальдрабок», составленной на рубеже XVI-XVII веков. "
            "Покажи, как в ее заклинаниях соседствуют христианские молитвы, рунические мотивы и магические знаки, "
            "не выдавая поздние интернет-легенды за содержание рукописи."
        ),
    },
    {
        "category": "magic_history",
        "topic": (
            "Расскажи о книге Реджинальда Скота «Открытие колдовства» 1584 года. "
            "Автор сомневался во многих обвинениях против ведьм и описывал приемы фокусников, чтобы показать, "
            "как обман или ошибка могли приниматься за магию. Свяжи факт с ценностью проверки источников."
        ),
    },
]
CHANNEL_POST_TOPICS.extend(CHANNEL_MAGIC_HISTORY_TOPICS)

CHANNEL_POST_EXTRA_TOPICS = [
    ("astrology", "Астрология"),
    ("astrology", "Натальная карта"),
    ("zodiac", "Гороскопы"),
    ("astrology", "Синастрия и совместимость"),
    ("planets", "Прогностика: транзиты, прогрессии, соляры"),
    ("moon", "Лунные циклы и лунный календарь"),
    ("astrology", "Ведическая астрология"),
    ("astrology", "Китайская астрология"),
    ("numerology", "Нумерология"),
    ("tarot", "Таро"),
    ("tarot", "Оракульные карты"),
    ("divination", "Ленорман"),
    ("divination", "Руны"),
    ("divination", "Маятник и биолокация"),
    ("divination", "Хиромантия"),
    ("mystic", "Экстрасенсорика"),
    ("mystic", "Ясновидение"),
    ("mystic", "Яснослышание"),
    ("mystic", "Яснознание"),
    ("mystic", "Интуиция и ее развитие"),
    ("mystic", "Энергетика человека"),
    ("mystic", "Чакры"),
    ("mystic", "Аура"),
    ("mystic", "Энергетическая защита"),
    ("mystic", "Очищение пространства"),
    ("mystic", "Снятие негатива"),
    ("meditation", "Медитации"),
    ("meditation", "Визуализации"),
    ("dreams", "Осознанные сновидения"),
    ("dreams", "Астральные путешествия"),
    ("karma", "Регрессии в прошлые жизни"),
    ("karma", "Реинкарнация"),
    ("karma", "Карма"),
    ("karma", "Родовые программы"),
    ("karma", "Работа с предками"),
    ("mystic", "Духовные практики"),
    ("mystic", "Магия свечей"),
    ("mystic", "Ритуалы и обряды"),
    ("mystic", "Заговоры"),
    ("crystals", "Амулеты и талисманы"),
    ("crystals", "Кристаллы и минералы"),
    ("crystals", "Литотерапия"),
    ("mystic", "Травничество"),
    ("mystic", "Ароматерапия"),
    ("meditation", "Рейки"),
    ("meditation", "Цигун"),
    ("meditation", "Кундалини"),
    ("mystic", "Каббала"),
    ("mystic", "Алхимия"),
    ("mystic", "Герметизм"),
    ("mystic", "Шаманизм"),
    ("mystic", "Тотемные животные"),
    ("mystic", "Ангелология"),
    ("mystic", "Работа с духовными наставниками"),
    ("mystic", "Ченнелинг"),
    ("mystic", "Спиритизм"),
    ("mystic", "Контакт с тонким миром"),
    ("numerology", "Символика и сакральная геометрия"),
    ("mystic", "Фэншуй"),
    ("mystic", "Васту"),
    ("meditation", "Мандалы"),
    ("meditation", "Мантры"),
    ("mystic", "Сакральные тексты"),
    ("mystic", "Энергетические практики для дома"),
    ("mystic", "Практики исполнения желаний"),
    ("mystic", "Закон притяжения"),
    ("mystic", "Тета-хилинг"),
    ("numerology", "Матрица судьбы"),
    ("mystic", "Дизайн человека"),
    ("mystic", "Психосоматика в эзотерическом подходе"),
]

CHANNEL_POST_TOPICS.extend(
    {
        "category": category,
        "topic": (
            f"Напиши короткий интересный пост на тему: {topic_name}. "
            "Раскрой одну практичную мысль, добавь живой пример или наблюдение и мягкий вопрос читателю."
        ),
    }
    for category, topic_name in CHANNEL_POST_EXTRA_TOPICS
)

# Старый интервальный режим отключён: автопостинг теперь идёт по CHANNEL_WEEKLY_POST_SCHEDULE.
# CHANNEL_POST_INTERVAL = 180
CHANNEL_ACTIVE_HOURS = (9, 22)  # посты с 9:00 до 22:30 по МСК
CHANNEL_SCHEDULE_GRACE_MINUTES = 90
CHANNEL_WEEKLY_POST_SCHEDULE = {
    0: [
        {
            "id": "day1_morning_card",
            "time": "09:00",
            "rubric": "Карта дня",
            "category": "tarot",
            "topic": (
                "Сгенерируй утреннюю рубрику 'Карта дня'. Выбери одну карту Таро из Старших Арканов, "
                "дай короткое послание дня и один вопрос для саморефлексии. Не продавай бота."
            ),
        },
        {
            "id": "day1_day_relationship_situation",
            "time": "14:00",
            "rubric": "Ситуация дня",
            "category": "tarot",
            "promo": True,
            "topic": (
                "Разбери типовую ситуацию в отношениях без фразы 'вопрос подписчика': человек отвечает сухо, "
                "исчезает или тянет с ответом. Покажи взгляд через одну карту Таро, отдели факт от тревоги, "
                "заверши мягким вопросом."
            ),
        },
        {
            "id": "day1_evening_pick_card",
            "time": "19:30",
            "rubric": "Выберите карту",
            "category": "tarot",
            "topic": (
                "Сделай интерактив: предложи выбрать карту 1, 2 или 3. Карты задай явно: 1 - Луна, "
                "2 - Умеренность, 3 - Сила. Не раскрывай значения, не зови в бота, только создай атмосферу "
                "и попроси сохранить выбранный номер до завтрашней расшифровки."
            ),
        },
    ],
    1: [
        {
            "id": "day2_morning_astro_mood",
            "time": "09:00",
            "rubric": "Астрологический настрой",
            "category": "moon",
            "topic": (
                "Дай утренний астрологический настрой дня через Луну, Венеру, Марс или Меркурий как метафору "
                "состояния. Нужен один практичный ориентир на день без запугивания."
            ),
        },
        {
            "id": "day2_day_myth_or_sign",
            "time": "14:00",
            "rubric": "Миф или знак",
            "category": "mystic",
            "topic": (
                "Разбери популярный эзотерический миф в формате 'миф или знак': одинаковые числа на часах, "
                "повторяющиеся фразы или случайная встреча. Покажи, где знак, а где фокус внимания."
            ),
        },
        {
            "id": "day2_evening_card_decode",
            "time": "19:30",
            "rubric": "Расшифровка выбора",
            "category": "tarot",
            "topic": (
                "Расшифруй вчерашний интерактив с картами: 1 - Луна, 2 - Умеренность, 3 - Сила. "
                "Для каждой карты дай 2-3 живых предложения: что заметить в себе сегодня и какой маленький шаг сделать. "
                "Не продавай бота."
            ),
        },
    ],
    2: [
        {
            "id": "day3_morning_dream_symbol",
            "time": "09:00",
            "rubric": "Сон и символ",
            "category": "dreams",
            "topic": (
                "Выбери один образ из сна: вода, дверь, лестница, бывший, дом или дорога. "
                "Разбери его не как сонник, а как символ внутреннего состояния."
            ),
        },
        {
            "id": "day3_day_one_minute_practice",
            "time": "14:00",
            "rubric": "Практика на минуту",
            "category": "meditation",
            "topic": (
                "Дай простую мини-практику на 1 минуту: дыхание, пауза, запись одной мысли или наблюдение за телом. "
                "Без обещаний чудес, с мягким объяснением, зачем это нужно."
            ),
        },
        {
            "id": "day3_evening_relationship_archetype",
            "time": "19:30",
            "rubric": "Архетип отношений",
            "category": "tarot",
            "topic": (
                "Разбери один архетип отношений: женщина, которая ждёт; человек, который исчезает; "
                "тот, кто боится выбрать; тот, кто держит дверь приоткрытой. Свяжи с одной картой Таро."
            ),
        },
    ],
    3: [
        {
            "id": "day4_morning_one_sign",
            "time": "09:00",
            "rubric": "Один знак дня",
            "category": "mystic",
            "topic": (
                "Рубрика 'Один знак дня': число, повторяющаяся фраза, случайная встреча или телесное ощущение. "
                "Покажи, как заметить знак без ухода в тревожную мнительность."
            ),
        },
        {
            "id": "day4_day_tarot_life_card",
            "time": "14:00",
            "rubric": "Таро в жизни",
            "category": "tarot",
            "topic": (
                "Разбери одну карту Таро не как учебное значение, а как бытовую сцену из жизни. "
                "Выбери карту сам, дай неожиданный, но ясный вывод."
            ),
        },
        {
            "id": "day4_evening_soft_bot",
            "time": "19:30",
            "rubric": "Когда общего прогноза мало",
            "category": "astrology",
            "promo": True,
            "topic": (
                "Сделай мягкий вечерний продающий пост: объясни, когда общий прогноз или общий расклад уже не помогает "
                "и нужен личный вопрос. Не дави, не обещай точных чудес, покажи разницу между общей подсказкой "
                "и личным разбором."
            ),
        },
    ],
    4: [
        {
            "id": "day5_morning_astrology_without_fear",
            "time": "09:00",
            "rubric": "Астрология без страха",
            "category": "astrology",
            "topic": (
                "Объясни один астрологический элемент простыми словами: дом, аспект, транзит, Луна или Венера. "
                "Главный акцент: это не приговор, а способ заметить паттерн."
            ),
        },
        {
            "id": "day5_day_reality_check",
            "time": "14:00",
            "rubric": "Проверка реальности",
            "category": "mystic",
            "promo": True,
            "topic": (
                "Пост 'Проверка реальности': отдели интуицию от тревоги. Дай 3 узнаваемых признака без списка, "
                "через связный текст: как звучит тревога, как звучит тихая интуиция, что проверить перед выводом."
            ),
        },
        {
            "id": "day5_evening_pick_phrase",
            "time": "19:30",
            "rubric": "Выберите фразу",
            "category": "mystic",
            "topic": (
                "Сделай интерактив: предложи выбрать фразу, которая сильнее откликается. "
                "Варианты: 1 - 'я устала ждать', 2 - 'я боюсь ошибиться', 3 - 'я уже знаю ответ'. "
                "Не раскрывай значения до завтрашней расшифровки."
            ),
        },
    ],
    5: [
        {
            "id": "day6_morning_phrase_decode",
            "time": "09:00",
            "rubric": "Расшифровка фраз",
            "category": "mystic",
            "topic": (
                "Расшифруй вчерашний выбор фраз: 1 - 'я устала ждать', 2 - 'я боюсь ошибиться', "
                "3 - 'я уже знаю ответ'. Для каждой фразы дай короткий психологично-эзотерический смысл "
                "и маленькое действие на день."
            ),
        },
        {
            "id": "day6_day_choice_situation",
            "time": "14:00",
            "rubric": "Ситуация дня",
            "category": "tarot",
            "promo": True,
            "topic": (
                "Разбери ситуацию выбора без выдуманного подписчика: написать или не писать, уйти или остаться, "
                "ждать или отпустить. Свяжи с одной картой Таро и дай честный ориентир без давления."
            ),
        },
        {
            "id": "day6_evening_sleep_ritual",
            "time": "19:30",
            "rubric": "Ритуал перед сном",
            "category": "meditation",
            "topic": (
                "Напиши атмосферный вечерний пост про маленький ритуал перед сном: вода, свет, запись мысли, "
                "тишина или дыхание. Не мистифицируй чрезмерно, дай ощущение опоры."
            ),
        },
    ],
    6: [
        {
            "id": "day7_morning_weekly_spread",
            "time": "09:00",
            "rubric": "Недельный расклад",
            "category": "tarot",
            "topic": (
                "Сделай общий недельный расклад из трёх смыслов: что отпустить, что увидеть, куда направить внимание. "
                "Пиши цельно, можно упомянуть 3 карты, но без сухого списка."
            ),
        },
        {
            "id": "day7_day_magic_history",
            "time": "14:00",
            "rubric": "Магия в истории",
            "category": "magic_history",
            "topic_pool": CHANNEL_MAGIC_HISTORY_TOPICS,
            "topic": (
                "Расскажи один проверяемый исторический факт, связанный с магией. "
                "Четко отделяй документированные сведения от легенд и поздних интерпретаций."
            ),
        },
        {
            "id": "day7_evening_week_summary",
            "time": "19:30",
            "rubric": "Итог недели",
            "category": "astrology",
            "promo": True,
            "topic": (
                "Сделай итог недели: что могло повторяться в знаках, снах, отношениях и внутреннем состоянии. "
                "Заверши мягким приглашением в бота для личного вопроса, без давления и без обещания стопроцентного ответа."
            ),
        },
    ],
}
CHANNEL_SCHEDULE_STYLE_BY_ID = {
    "day1_morning_card": (
        "Дизайн текста: утренняя таро-карточка. Первая строка - короткое название карты или настроения, "
        "дальше 2-3 воздушных абзаца: образ карты, послание дня, один вопрос. Без истории и без продажи."
    ),
    "day1_day_relationship_situation": (
        "Дизайн текста: разбор сцены из переписки. Начни с узнаваемой детали вроде короткого ответа или паузы, "
        "потом покажи, что видно по карте, и отдели факт от тревожной догадки. Ритм живой, почти разговорный."
    ),
    "day1_evening_pick_card": (
        "Дизайн текста: интерактив-выбор. Пост должен быть коротким и визуально простым: вступление, затем три строки "
        "с вариантами 1, 2, 3, затем просьба запомнить выбор. Значения не раскрывать."
    ),
    "day2_morning_astro_mood": (
        "Дизайн текста: астрологическая погода. Пиши так, будто описываешь фон дня: планета как ветер, давление или свет, "
        "а не как приговор. В финале один практичный ориентир."
    ),
    "day2_day_myth_or_sign": (
        "Дизайн текста: мини-колонка 'миф против реальности'. Начни с популярной фразы, мягко разверни ее, "
        "затем дай более трезвый взгляд без обесценивания мистики."
    ),
    "day2_evening_card_decode": (
        "Дизайн текста: расшифровка оракула. Разрешены три коротких блока 1, 2, 3. У каждого выбора свой ритм: "
        "один про эмоции, второй про действие, третий про внутреннюю силу."
    ),
    "day3_morning_dream_symbol": (
        "Дизайн текста: дневник сна. Начни с чувственной сцены после пробуждения, затем раскрой один символ как "
        "внутреннее состояние. Тон медленный, образный, без сонника."
    ),
    "day3_day_one_minute_practice": (
        "Дизайн текста: практическая карточка. Минимум мистики, больше опоры: что сделать, зачем это работает, "
        "что заметить после. Фразы короткие, ясные."
    ),
    "day3_evening_relationship_archetype": (
        "Дизайн текста: психологический портрет. Назови архетип, покажи его внутренний монолог, затем привяжи "
        "к одной карте Таро. Должно ощущаться как узнавание себя."
    ),
    "day4_morning_one_sign": (
        "Дизайн текста: заметка-наблюдение. Один знак дня подается как маленькая деталь из реальности, "
        "а не как громкое пророчество. Финал должен возвращать читателя к себе."
    ),
    "day4_day_tarot_life_card": (
        "Дизайн текста: бытовая сцена через карту. Сначала обычный момент из жизни, затем карта как линза, "
        "потом неожиданный вывод. Не используй учебниковую подачу."
    ),
    "day4_evening_soft_bot": (
        "Дизайн текста: мягкая вечерняя продажа через пользу. Сначала честно объясни ограничение общего прогноза, "
        "потом покажи, когда личный разбор уместен. Тон спокойный, без нажима."
    ),
    "day5_morning_astrology_without_fear": (
        "Дизайн текста: перевод с астрологического на человеческий. Возьми один термин и объясни его через "
        "повседневную ситуацию. Главная эмоция поста - облегчение, а не тревога."
    ),
    "day5_day_reality_check": (
        "Дизайн текста: спокойный внутренний чек. Пиши как ясный разговор с человеком, который себя накручивает: "
        "что говорит тревога, как звучит интуиция, какой факт проверить."
    ),
    "day5_evening_pick_phrase": (
        "Дизайн текста: выбор фразы. Пост короткий, почти как зеркало. Дай три фразы отдельными строками "
        "с номерами 1, 2, 3. Не объясняй их до завтрашней расшифровки."
    ),
    "day6_morning_phrase_decode": (
        "Дизайн текста: расшифровка зеркала. Разрешены три блока 1, 2, 3. Каждый блок должен быть не предсказанием, "
        "а маленьким диагнозом состояния и мягким действием."
    ),
    "day6_day_choice_situation": (
        "Дизайн текста: развилка решения. Пост строится вокруг двух дверей: написать или промолчать, уйти или остаться. "
        "Карта Таро должна не решать за человека, а подсветить критерий выбора."
    ),
    "day6_evening_sleep_ritual": (
        "Дизайн текста: вечерний ритуал-атмосфера. Мягкий темп, меньше терминов, больше телесной опоры: свет, вода, "
        "дыхание, тишина. Финал должен успокаивать."
    ),
    "day7_morning_weekly_spread": (
        "Дизайн текста: недельная навигация. Пиши как расклад-компас: что отпустить, что увидеть, куда направить внимание. "
        "Можно дать три смысловых строки, но без сухого списка."
    ),
    "day7_day_magic_history": (
        "Дизайн текста: историческая миниатюра. Начни с конкретной даты, предмета, закона, рукописи или сцены, "
        "затем объясни контекст и отдели документированный факт от популярной легенды. "
        "Не превращай пост в лекцию и не выдавай преследования людей за доказательство существования магии."
    ),
    "day7_evening_week_summary": (
        "Дизайн текста: воскресное письмо канала. Итог недели должен звучать как спокойное подведение черты: "
        "что повторялось, что стало яснее, с чем можно зайти в новую неделю."
    ),
}
CHANNEL_SCHEDULE_VISUAL_BY_ID = {
    "day1_morning_card": {
        "size": "420-620 символов, ощущение короткой утренней карточки",
        "paragraphs": "3 абзаца: карта, смысл, вопрос",
        "formatting": "1 жирный акцент на названии карты, курсив не обязателен, блюр не нужен",
        "emoji": "1 спокойный утренний символ, например карта, солнце или точка света",
        "layout": "много воздуха, без списков, без длинных строк",
        "image_mood": "утренний стол таролога, мягкий свет, одна карта как главный объект",
        "image_queries": ["tarot card morning light", "single tarot card candle", "tarot deck sunrise table"],
    },
    "day1_day_relationship_situation": {
        "size": "650-850 символов, как живой разбор короткой сцены",
        "paragraphs": "4 коротких абзаца: сцена, первая тревожная мысль, карта, проверочный вопрос",
        "formatting": "жирным выделить карту или ключевой вывод, курсивом можно дать внутреннюю фразу, блюр не нужен",
        "emoji": "0-1 эмодзи, настроение сдержанное, без романтической перегрузки",
        "layout": "ритм переписки: короткая первая строка, потом спокойное объяснение",
        "image_mood": "телефон с сообщением без читаемого текста, вечерний стол, чувство паузы",
        "image_queries": ["smartphone on table evening", "phone message blurred screen", "woman holding phone window"],
    },
    "day1_evening_pick_card": {
        "size": "300-480 символов, очень легкий интерактив",
        "paragraphs": "вступление, три отдельные строки 1/2/3, короткий финал",
        "formatting": "жирный заголовок, без курсива, блюр не использовать",
        "emoji": "1 загадочный вечерний эмодзи, настроение выбора",
        "layout": "варианты 1, 2, 3 должны быть визуально видны с первого взгляда",
        "image_mood": "три карты на столе, темный фон, свеча, без текста и цифр на картинке",
        "image_queries": ["three tarot cards candle", "three tarot cards spread", "tarot cards dark table"],
    },
    "day2_morning_astro_mood": {
        "size": "450-700 символов, как прогноз погоды, но астрологический",
        "paragraphs": "3 абзаца: фон, как прожить, вопрос",
        "formatting": "жирным планету или Луну, курсивом одну атмосферную строку, блюр не нужен",
        "emoji": "1 небесный эмодзи, настроение ясное и воздушное",
        "layout": "плавные строки, без резких продажных блоков",
        "image_mood": "астрологический стол, карта неба, утренний или дневной свет",
        "image_queries": ["astrology chart morning desk", "star chart notebook light", "astrologer desk sunlight"],
    },
    "day2_day_myth_or_sign": {
        "size": "600-820 символов, мини-колонка с поворотом мысли",
        "paragraphs": "4 абзаца: миф, почему он цепляет, трезвый взгляд, вопрос",
        "formatting": "жирным 'миф' или главный термин, курсив минимально, блюр можно один раз для неожиданного вывода",
        "emoji": "0-1 эмодзи, настроение умное и чуть ироничное",
        "layout": "контрастные абзацы, не делать список",
        "image_mood": "часы, городская деталь, случайный знак без читаемых слов",
        "image_queries": ["clock night table", "street reflection night", "coincidence city lights"],
    },
    "day2_evening_card_decode": {
        "size": "650-900 символов, расшифровка выбора",
        "paragraphs": "три компактных блока 1/2/3, у каждого 2-3 предложения",
        "formatting": "жирным названия карт, курсив по желанию, блюр не нужен",
        "emoji": "до 2 спокойных эмодзи на весь пост, не в каждом пункте",
        "layout": "ясные блоки выбора, чтобы читатель быстро нашел свой номер",
        "image_mood": "три карты раскрыты на столе, свеча или ткань, фокус на раскладе",
        "image_queries": ["three tarot cards revealed", "tarot card spread candle", "tarot reading three cards"],
    },
    "day3_morning_dream_symbol": {
        "size": "550-800 символов, медленный дневник сна",
        "paragraphs": "3-4 абзаца с большим воздухом",
        "formatting": "курсив для первой атмосферной строки, жирный для символа сна, блюр можно не использовать",
        "emoji": "0-1 лунный или сонный эмодзи, настроение тихое",
        "layout": "мягкое чтение, без учебниковых определений",
        "image_mood": "прикроватный стол, сонник не нужен, дневник сна и лунный свет",
        "image_queries": ["dream journal bedside table", "moonlight bedroom window", "notebook bedside moonlight"],
    },
    "day3_day_one_minute_practice": {
        "size": "350-560 символов, практическая карточка",
        "paragraphs": "короткое действие, зачем оно нужно, вопрос после практики",
        "formatting": "жирным действие, без курсива, без блюра",
        "emoji": "1 земной спокойный эмодзи, настроение опоры",
        "layout": "очень простые строки, можно начать с 'На минуту:'",
        "image_mood": "свеча, вода, дыхание, минимальный спокойный предметный кадр",
        "image_queries": ["meditation candle quiet room", "breathing practice candle", "calm desk candle notebook"],
    },
    "day3_evening_relationship_archetype": {
        "size": "700-950 символов, психологический портрет",
        "paragraphs": "4 абзаца: архетип, внутренний голос, карта, вопрос",
        "formatting": "жирным архетип или карту, курсив для внутренней реплики, блюр один раз можно для скрытого вывода",
        "emoji": "0-1 эмодзи, настроение узнавания, не драматизировать",
        "layout": "плотнее обычного, как маленький портрет",
        "image_mood": "человек у окна или пустой стул, атмосфера ожидания, без лиц крупным планом",
        "image_queries": ["person by window evening", "empty chair window", "woman waiting by window"],
    },
    "day4_morning_one_sign": {
        "size": "420-650 символов, заметка-наблюдение",
        "paragraphs": "2-3 абзаца, один знак и один вывод",
        "formatting": "один жирный акцент, курсив по желанию, блюр не нужен",
        "emoji": "0-1 маленький символ, настроение внимательности",
        "layout": "без заголовка или с очень коротким заголовком",
        "image_mood": "маленькая деталь реальности: тень, отражение, окно, рука, свет",
        "image_queries": ["sunlight shadow wall", "window reflection morning", "small sign reflection street"],
    },
    "day4_day_tarot_life_card": {
        "size": "600-850 символов, бытовая сцена через карту",
        "paragraphs": "сцена, карта как линза, неожиданный вывод, вопрос",
        "formatting": "жирным карту, курсив для бытовой фразы, блюр не нужен",
        "emoji": "1 предметный или таро-эмодзи, настроение живое",
        "layout": "начать не с карты, а с обычной ситуации",
        "image_mood": "карта Таро рядом с бытовым предметом: чашка, ключи, блокнот",
        "image_queries": ["tarot card coffee cup", "tarot card notebook keys", "tarot card everyday table"],
    },
    "day4_evening_soft_bot": {
        "size": "650-900 символов, мягкий продающий пост",
        "paragraphs": "3-4 абзаца: ограничение общего прогноза, когда нужен личный разбор, спокойный CTA",
        "formatting": "жирным пользу личного разбора, курсив минимум, блюр не использовать",
        "emoji": "0-1 спокойный эмодзи, настроение доверия",
        "layout": "без агрессивных офферов, CTA отдельным последним абзацем",
        "image_mood": "астрологический рабочий стол, личная карта, блокнот с вопросом без читаемого текста",
        "image_queries": ["astrology consultation desk", "natal chart notebook candle", "astrology chart question notebook"],
    },
    "day5_morning_astrology_without_fear": {
        "size": "500-760 символов, объяснение простым языком",
        "paragraphs": "3 абзаца: термин, бытовой перевод, вопрос",
        "formatting": "жирным термин, курсив не обязателен, блюр не нужен",
        "emoji": "0-1 светлый эмодзи, настроение облегчения",
        "layout": "понятно даже новичку, без плотной терминологии",
        "image_mood": "астрологическая карта рядом с кофе или ручкой, дневной свет",
        "image_queries": ["natal chart coffee desk", "astrology chart pen daylight", "zodiac wheel notebook desk"],
    },
    "day5_day_reality_check": {
        "size": "650-900 символов, спокойный внутренний чек",
        "paragraphs": "4 коротких абзаца, каждый как шаг к ясности",
        "formatting": "жирным 'тревога' или 'интуиция', курсив для внутренней реплики, блюр можно для проверочного вопроса",
        "emoji": "0 эмодзи или один очень спокойный, настроение трезвое",
        "layout": "меньше мистики, больше ясности и пауз",
        "image_mood": "телефон, блокнот, чашка, спокойная проверка фактов",
        "image_queries": ["phone notebook coffee table", "journal pen calm desk", "person writing notebook window"],
    },
    "day5_evening_pick_phrase": {
        "size": "280-430 символов, короткое зеркало",
        "paragraphs": "вступление, три строки 1/2/3, финал",
        "formatting": "жирный короткий заголовок, без курсива, без блюра",
        "emoji": "0-1 эмодзи, настроение внутреннего выбора",
        "layout": "очень много воздуха, фразы отдельными строками",
        "image_mood": "зеркало, блокнот, свеча, три маленькие бумажки без читаемого текста",
        "image_queries": ["mirror candle notebook", "three blank notes candle", "journal candle mirror"],
    },
    "day6_morning_phrase_decode": {
        "size": "650-900 символов, расшифровка зеркала",
        "paragraphs": "три блока 1/2/3, каждый короткий и самостоятельный",
        "formatting": "жирным ключевую фразу каждого выбора, курсив по желанию, блюр не нужен",
        "emoji": "0-2 спокойных эмодзи на весь пост",
        "layout": "читатель должен быстро найти свой номер",
        "image_mood": "открытый дневник, три заметки или три карты, утренний свет",
        "image_queries": ["open journal three notes", "three cards journal morning", "notebook three blank papers"],
    },
    "day6_day_choice_situation": {
        "size": "650-900 символов, развилка решения",
        "paragraphs": "4 абзаца: две двери, карта, критерий выбора, вопрос",
        "formatting": "жирным карту или критерий, курсив для фразы выбора, блюр можно для мягкого вывода",
        "emoji": "0-1 эмодзи, настроение честной развилки",
        "layout": "ощущение двух направлений, но без списка плюсов и минусов",
        "image_mood": "развилка дороги, две двери, ключи или перекресток",
        "image_queries": ["two doors hallway", "crossroads path fog", "keys on table decision"],
    },
    "day6_evening_sleep_ritual": {
        "size": "420-650 символов, вечерняя атмосфера",
        "paragraphs": "3 мягких абзаца: предмет, действие, отпускание дня",
        "formatting": "курсив для первой строки, жирный для ритуального действия, блюр не нужен",
        "emoji": "1 ночной или водный эмодзи, настроение убаюкивающее",
        "layout": "медленный темп, короткие строки, без терминов",
        "image_mood": "стакан воды у кровати, свеча, лунный свет, тишина",
        "image_queries": ["glass of water bedside moonlight", "bedside candle night", "moonlight bedroom water glass"],
    },
    "day7_morning_weekly_spread": {
        "size": "650-900 символов, расклад-компас",
        "paragraphs": "можно 3 смысловые строки или 3 коротких абзаца: отпустить, увидеть, направить",
        "formatting": "жирным три ключевых глагола, курсив умеренно, блюр не нужен",
        "emoji": "1 навигационный или звездный эмодзи, настроение начала недели",
        "layout": "структурно, но не сухо; три части должны различаться визуально",
        "image_mood": "три карты, компас, блокнот, ощущение навигации",
        "image_queries": ["tarot cards compass notebook", "three tarot cards compass", "weekly tarot spread desk"],
    },
    "day7_day_magic_history": {
        "size": "650-900 символов, историческая миниатюра с одним главным фактом",
        "paragraphs": "3-4 абзаца: деталь эпохи, факт, контекст, короткий вывод",
        "formatting": "жирным имя, дату или название источника; курсивом можно выделить легенду, блюр не нужен",
        "emoji": "0-1 уместный исторический символ, без театральной мистики",
        "layout": "не список и не энциклопедическая справка; одна история читается как маленькая сцена",
        "image_mood": "историческая рукопись, старинная гравюра, архивный документ, музейный магический предмет",
        "image_queries": [
            "historic magic manuscript museum",
            "medieval manuscript archive",
            "antique occult book museum",
            "historic witchcraft document",
        ],
    },
    "day7_evening_week_summary": {
        "size": "650-900 символов, воскресное письмо",
        "paragraphs": "4 абзаца: итог, повторяющийся мотив, что взять дальше, мягкий CTA",
        "formatting": "жирным итоговый смысл, курсив для теплой строки, блюр не использовать",
        "emoji": "1 вечерний эмодзи, настроение спокойного завершения",
        "layout": "как письмо канала, без списков и без давления",
        "image_mood": "вечерний дневник, свеча, окно со звездами, закрытие недели",
        "image_queries": ["evening journal candle window", "sunday night notebook candle", "starry window journal candle"],
    },
}
CHANNEL_GENERATED_IMAGE_SYMBOLS = {
    "tarot": ["XVII", "VI", "XI", "☽", "✦", "◆"],
    "astrology": ["☉", "☽", "☿", "♀", "♂", "♃", "♄"],
    "zodiac": ["♈", "♉", "♊", "♋", "♌", "♍", "♎", "♏", "♐", "♑", "♒", "♓"],
    "moon": ["☽", "○", "◐", "●", "◑", "✦"],
    "planets": ["☉", "☿", "♀", "♂", "♃", "♄"],
    "numerology": ["11:11", "22:22", "7", "3", "9", "12"],
    "crystals": ["◇", "△", "✧", "✦", "◆", "◈"],
    "divination": ["✦", "☽", "◆", "◇", "☉", "✧"],
    "mystic": ["✦", "☽", "◇", "✧", "☉", "◆"],
    "magic_history": ["XV", "XVI", "XVII", "✦", "☽", "◆"],
    "default": ["✦", "☽", "◇", "✧", "☉", "◆"],
}

CHANNEL_GENERATED_IMAGE_PALETTES = [
    {"bg": (38, 29, 48), "mid": (112, 73, 92), "accent": (230, 187, 128), "ink": (255, 246, 228)},
    {"bg": (22, 47, 54), "mid": (51, 103, 102), "accent": (226, 183, 92), "ink": (239, 252, 247)},
    {"bg": (55, 36, 27), "mid": (126, 82, 62), "accent": (238, 196, 136), "ink": (255, 244, 232)},
    {"bg": (28, 37, 63), "mid": (81, 92, 132), "accent": (217, 190, 116), "ink": (244, 247, 255)},
    {"bg": (34, 56, 42), "mid": (86, 122, 83), "accent": (222, 197, 121), "ink": (246, 250, 237)},
]

CHANNEL_IMAGE_SCENES_BY_CATEGORY = {
    "tarot": ["tarot_spread", "crystal_ball", "pendulum_map"],
    "divination": ["crystal_ball", "tarot_spread", "pendulum_map"],
    "astrology": ["natal_chart", "astrolabe", "moon_window"],
    "zodiac": ["natal_chart", "astrolabe"],
    "planets": ["natal_chart", "astrolabe", "moon_window"],
    "moon": ["moon_window", "moon_window", "moon_calendar", "crystal_ball"],
    "numerology": ["number_journal", "pocket_watch"],
    "crystals": ["crystal_altar", "crystal_ball"],
    "dreams": ["dream_journal", "moon_window"],
    "elements": ["four_elements", "crystal_altar"],
    "karma": ["thread_and_scales", "pendulum_map"],
    "meditation": ["singing_bowl", "moon_window"],
    "mystic": ["singing_bowl", "crystal_ball", "tarot_spread", "crystal_altar", "pendulum_map"],
    "magic_history": ["pocket_watch", "number_journal", "pendulum_map", "tarot_spread"],
    "default": ["crystal_ball", "tarot_spread", "natal_chart", "moon_window"],
}

MAX_RECENT_PROMOS = 5
CHANNEL_IMAGE_ASSET_DIR = "generated_channel_images"
MAX_GENERATED_CHANNEL_IMAGES = 80
CHANNEL_IMAGE_PROVIDER = getenv("CHANNEL_IMAGE_PROVIDER", "stock").strip().lower()
PEXELS_API_KEY = getenv("PEXELS_API_KEY", "")
UNSPLASH_ACCESS_KEY = getenv("UNSPLASH_ACCESS_KEY", getenv("UNSPLASH_API_KEY", ""))
CHANNEL_STOCK_IMAGE_TIMEOUT = int(getenv("CHANNEL_STOCK_IMAGE_TIMEOUT", "35"))
CHANNEL_STOCK_IMAGE_PAGE_SIZE = int(getenv("CHANNEL_STOCK_IMAGE_PAGE_SIZE", "20"))
CHANNEL_STOCK_IMAGE_MAX_PAGE = int(getenv("CHANNEL_STOCK_IMAGE_MAX_PAGE", "18"))
CHANNEL_STOCK_IMAGE_POOL_TARGET = int(getenv("CHANNEL_STOCK_IMAGE_POOL_TARGET", "36"))
CHANNEL_STOCK_IMAGE_QUERY_ATTEMPTS = int(getenv("CHANNEL_STOCK_IMAGE_QUERY_ATTEMPTS", "10"))
CHANNEL_USE_AI_IMAGE_BRIEF = getenv("CHANNEL_USE_AI_IMAGE_BRIEF", "true").strip().lower() in {"1", "true", "yes", "on"}
CHANNEL_IMAGE_MIN_BRIEF_SCORE = int(getenv("CHANNEL_IMAGE_MIN_BRIEF_SCORE", "32"))
CHANNEL_STOCK_IMAGE_MAX_BYTES = int(getenv("CHANNEL_STOCK_IMAGE_MAX_BYTES", "9500000"))
CHANNEL_STOCK_IMAGE_MIN_BYTES = int(getenv("CHANNEL_STOCK_IMAGE_MIN_BYTES", "6000"))
CHANNEL_TELEGRAM_IMAGE_MAX_SIDE = int(getenv("CHANNEL_TELEGRAM_IMAGE_MAX_SIDE", "2560"))
CHANNEL_TELEGRAM_IMAGE_MAX_PIXELS = int(getenv("CHANNEL_TELEGRAM_IMAGE_MAX_PIXELS", "40000000"))
MAX_RECENT_CHANNEL_IMAGE_KEYS = int(getenv("MAX_RECENT_CHANNEL_IMAGE_KEYS", "700"))
CHANNEL_REQUIRE_IMAGE = getenv("CHANNEL_REQUIRE_IMAGE", "true").strip().lower() in {"1", "true", "yes", "on"}
CHANNEL_REQUIRE_REAL_PHOTO = getenv("CHANNEL_REQUIRE_REAL_PHOTO", "true").strip().lower() in {"1", "true", "yes", "on"}
CHANNEL_ALLOW_AI_IMAGE_FALLBACK = getenv("CHANNEL_ALLOW_AI_IMAGE_FALLBACK", "false").strip().lower() in {"1", "true", "yes", "on"}
CHANNEL_ALLOW_LOCAL_IMAGE_FALLBACK = getenv("CHANNEL_ALLOW_LOCAL_IMAGE_FALLBACK", "false").strip().lower() in {"1", "true", "yes", "on"}
POLLINATIONS_API_KEY = getenv("POLLINATIONS_API_KEY", "")
POLLINATIONS_IMAGE_MODEL = getenv("POLLINATIONS_IMAGE_MODEL", "flux")
POLLINATIONS_IMAGE_TIMEOUT = int(getenv("POLLINATIONS_IMAGE_TIMEOUT", "90"))
TELEGRAM_PHOTO_CAPTION_LIMIT = 1024
RECENT_TOPIC_KEYS: list[str] = []
MAX_RECENT_TOPICS = int(getenv("MAX_RECENT_TOPICS", "48"))
RECENT_CONTENT_SIGNATURES: list[str] = []
MAX_RECENT_CONTENT_SIGNATURES = int(getenv("MAX_RECENT_CONTENT_SIGNATURES", "84"))
RECENT_CHANNEL_POST_SAMPLES: list[str] = []
MAX_RECENT_CHANNEL_POST_SAMPLES = int(getenv("MAX_RECENT_CHANNEL_POST_SAMPLES", "60"))
MAX_RECENT_CHANNEL_POST_RECORDS = int(getenv("MAX_RECENT_CHANNEL_POST_RECORDS", "160"))
CHANNEL_POST_HISTORY_DAYS = int(getenv("CHANNEL_POST_HISTORY_DAYS", "45"))
CHANNEL_TEXT_SIMILARITY_THRESHOLD = float(getenv("CHANNEL_TEXT_SIMILARITY_THRESHOLD", "0.58"))
MAX_RECENT_CHANNEL_IMAGE_HASHES = int(getenv("MAX_RECENT_CHANNEL_IMAGE_HASHES", "900"))
CHANNEL_IMAGE_HASH_HISTORY_DAYS = int(getenv("CHANNEL_IMAGE_HASH_HISTORY_DAYS", "90"))
CHANNEL_IMAGE_DHASH_DISTANCE = int(getenv("CHANNEL_IMAGE_DHASH_DISTANCE", "7"))
CHANNEL_IMAGE_AHASH_DISTANCE = int(getenv("CHANNEL_IMAGE_AHASH_DISTANCE", "5"))


def load_channel_state() -> dict:
    try:
        with open(CHANNEL_STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def save_channel_state(state: dict) -> None:
    try:
        with open(CHANNEL_STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False)
    except Exception as e:
        print(f"[channel_state] save error: {e}")


CHANNEL_POST_STOPWORDS = {
    "это", "как", "что", "чтобы", "если", "или", "для", "про", "при", "над", "под", "без",
    "уже", "еще", "ещё", "только", "когда", "где", "вам", "вас", "тебя", "тебе", "твое",
    "твоя", "твой", "свои", "себя", "очень", "можно", "нужно", "один", "одна", "одно",
    "два", "три", "день", "сегодня", "внутри", "будет", "были", "был", "была", "есть",
    "нет", "чем", "все", "всё", "всех", "вот", "так", "тоже", "между", "после", "перед",
    "голос", "звезд", "звёзд", "таро", "астрология", "бот", "личный", "вопрос", "разбор",
    "пост", "написан", "тарологом", "астрологом",
}


def _channel_state_recent_cutoff(days: int) -> datetime:
    return _msk_now() - timedelta(days=max(1, days))


def _parse_channel_state_datetime(value: str) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(str(value or ""))
        if parsed.tzinfo is not None:
            parsed = parsed.astimezone(timezone(timedelta(hours=3))).replace(tzinfo=None)
        return parsed
    except Exception:
        return None


def _plain_channel_post_text(text: str) -> str:
    text = re.sub(r"<br\s*/?>", " ", text or "", flags=re.IGNORECASE)
    text = re.sub(r"</?[a-zA-Z][a-zA-Z0-9\-]*(?:\s[^>]*)?>", " ", text)
    text = html.unescape(text)
    return re.sub(r"\s+", " ", text).strip()


def _channel_text_tokens(text: str) -> list[str]:
    plain = _plain_channel_post_text(text).lower().replace("ё", "е")
    tokens = re.findall(r"[а-яa-z0-9]{3,}", plain)
    return [token for token in tokens if token not in CHANNEL_POST_STOPWORDS]


def _channel_text_signature_tokens(text: str, limit: int = 18) -> list[str]:
    counts: dict[str, int] = {}
    for token in _channel_text_tokens(text):
        counts[token] = counts.get(token, 0) + 1
    ranked = sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    return [token for token, _count in ranked[:limit]]


def _channel_token_similarity(left: list[str] | set[str], right: list[str] | set[str]) -> float:
    left_set = set(left)
    right_set = set(right)
    if not left_set or not right_set:
        return 0.0
    return len(left_set & right_set) / len(left_set | right_set)


def _channel_text_similarity(text: str, record: dict) -> float:
    tokens = _channel_text_signature_tokens(text, limit=24)
    record_tokens = record.get("tokens") or []
    token_score = _channel_token_similarity(tokens, record_tokens)

    sample = str(record.get("sample") or "")
    current_start = _plain_channel_post_text(text)[:120].lower().replace("ё", "е")
    record_start = _plain_channel_post_text(sample)[:120].lower().replace("ё", "е")
    start_score = 0.0
    if current_start and record_start:
        current_words = set(re.findall(r"[а-яa-z0-9]{3,}", current_start))
        record_words = set(re.findall(r"[а-яa-z0-9]{3,}", record_start))
        start_score = _channel_token_similarity(current_words, record_words)

    return max(token_score, (token_score * 0.7) + (start_score * 0.3))


def _channel_recent_post_records(state: dict | None = None, days: int | None = None) -> list[dict]:
    state = state if state is not None else load_channel_state()
    cutoff = _channel_state_recent_cutoff(days or CHANNEL_POST_HISTORY_DAYS)
    records = []
    for item in state.get("recent_post_records", []) or []:
        if not isinstance(item, dict):
            continue
        created_at = _parse_channel_state_datetime(str(item.get("at") or ""))
        if created_at and created_at < cutoff:
            continue
        records.append(item)
    return records[-MAX_RECENT_CHANNEL_POST_RECORDS:]


def _channel_trim_post_records(records: list[dict]) -> list[dict]:
    cutoff = _channel_state_recent_cutoff(CHANNEL_POST_HISTORY_DAYS)
    trimmed = []
    for item in records or []:
        created_at = _parse_channel_state_datetime(str(item.get("at") or ""))
        if created_at and created_at < cutoff:
            continue
        if isinstance(item, dict):
            trimmed.append(item)
    return trimmed[-MAX_RECENT_CHANNEL_POST_RECORDS:]


def _channel_similar_recent_post(text: str, content_plan: dict | None = None) -> tuple[float, dict | None]:
    schedule = (content_plan or {}).get("schedule") or {}
    category = (content_plan or {}).get("category") or ""
    best_score = 0.0
    best_record = None
    for record in _channel_recent_post_records():
        score = _channel_text_similarity(text, record)
        if schedule.get("id") and record.get("schedule_id") == schedule.get("id"):
            score += 0.08
        if category and record.get("category") == category:
            score += 0.04
        if score > best_score:
            best_score = score
            best_record = record
    return best_score, best_record


def _channel_image_font(size: int, bold: bool = False):
    try:
        from PIL import ImageFont
    except Exception:
        return None

    candidates = [
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf" if bold else "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "/usr/share/fonts/truetype/liberation2/LiberationSans-Bold.ttf" if bold else "/usr/share/fonts/truetype/liberation2/LiberationSans-Regular.ttf",
        "C:/Windows/Fonts/arialbd.ttf" if bold else "C:/Windows/Fonts/arial.ttf",
        "C:/Windows/Fonts/segoeuib.ttf" if bold else "C:/Windows/Fonts/segoeui.ttf",
    ]
    for path in candidates:
        if path and os.path.exists(path):
            return ImageFont.truetype(path, size=size)
    return ImageFont.load_default()


def _draw_centered_text(draw, box: tuple[int, int, int, int], text: str, font, fill, spacing: int = 8) -> None:
    import textwrap

    x1, y1, x2, y2 = box
    max_width = x2 - x1
    words = (text or "").split()
    lines = []
    current = ""
    for word in words:
        candidate = f"{current} {word}".strip()
        width = draw.textbbox((0, 0), candidate, font=font)[2]
        if width <= max_width or not current:
            current = candidate
        else:
            lines.append(current)
            current = word
    if current:
        lines.append(current)
    if not lines:
        lines = textwrap.wrap(text or "", width=18) or [""]

    heights = [draw.textbbox((0, 0), line, font=font)[3] for line in lines]
    total_height = sum(heights) + spacing * max(0, len(lines) - 1)
    y = y1 + max(0, (y2 - y1 - total_height) // 2)
    for idx, line in enumerate(lines):
        bbox = draw.textbbox((0, 0), line, font=font)
        width = bbox[2] - bbox[0]
        draw.text((x1 + (max_width - width) // 2, y), line, font=font, fill=fill)
        y += heights[idx] + spacing


def _cleanup_generated_channel_images() -> None:
    try:
        if not os.path.isdir(CHANNEL_IMAGE_ASSET_DIR):
            return
        files = [
            os.path.join(CHANNEL_IMAGE_ASSET_DIR, name)
            for name in os.listdir(CHANNEL_IMAGE_ASSET_DIR)
            if name.lower().endswith((".png", ".jpg", ".jpeg", ".webp"))
        ]
        files.sort(key=lambda path: os.path.getmtime(path))
        for old_path in files[:-MAX_GENERATED_CHANNEL_IMAGES]:
            try:
                os.remove(old_path)
            except Exception:
                pass
    except Exception as e:
        print(f"[channel_image] cleanup error: {e}")


def _channel_image_label(category: str) -> str:
    labels = {
        "tarot": "ТАРО",
        "astrology": "АСТРОЛОГИЯ",
        "zodiac": "ЗОДИАК",
        "moon": "ЛУННЫЙ РИТМ",
        "planets": "ПЛАНЕТЫ",
        "numerology": "НУМЕРОЛОГИЯ",
        "crystals": "КРИСТАЛЛЫ",
        "divination": "ГАДАНИЕ",
        "karma": "КАРМА",
        "dreams": "СНЫ",
        "elements": "СТИХИИ",
        "mystic": "ПРАКТИКА",
        "meditation": "НАСТРОЙКА",
        "magic_history": "МАГИЯ В ИСТОРИИ",
    }
    return labels.get(category, "ГОЛОС ЗВЕЗД")


def _channel_image_scene_options(category: str) -> list[str]:
    return CHANNEL_IMAGE_SCENES_BY_CATEGORY.get(category) or CHANNEL_IMAGE_SCENES_BY_CATEGORY["default"]


CHANNEL_STOCK_IMAGE_QUERIES = {
    "tarot": [
        "tarot cards candle",
        "tarot deck table",
        "fortune telling cards",
        "divination cards candle",
        "mystic cards still life",
    ],
    "astrology": [
        "astrology chart",
        "constellation map",
        "night sky stars",
        "telescope stars",
        "astronomy chart",
    ],
    "zodiac": [
        "zodiac constellation",
        "star constellation",
        "night sky stars",
        "astronomy chart",
        "celestial map",
    ],
    "moon": [
        "moon night sky",
        "crescent moon",
        "full moon",
        "moon phases",
        "lunar eclipse",
    ],
    "planets": [
        "planet space",
        "solar system",
        "telescope observatory",
        "astronomy space",
        "mercury planet",
    ],
    "numerology": [
        "numbers notebook",
        "old notebook numbers",
        "pocket watch notebook",
        "handwritten notes desk",
        "calendar numbers",
    ],
    "crystals": [
        "amethyst crystal",
        "quartz crystal",
        "crystals close up",
        "crystal stones",
        "minerals still life",
    ],
    "divination": [
        "pendulum divination",
        "crystal ball candle",
        "mystic table candle",
        "old map candle",
        "fortune telling table",
    ],
    "dreams": [
        "moon bedroom window",
        "dream journal",
        "night window moon",
        "misty lake night",
        "sleep notebook",
    ],
    "elements": [
        "fire water earth air",
        "candle water stone feather",
        "natural elements still life",
        "candle and water",
        "stone feather candle",
    ],
    "karma": [
        "old letters candle",
        "balance scales",
        "red thread hands",
        "vintage clock",
        "crossroads path",
    ],
    "meditation": [
        "meditation candle",
        "singing bowl",
        "incense candle",
        "calm room candle",
        "meditation stones",
    ],
    "mystic": [
        "mystic candle",
        "spiritual practice candle",
        "incense smoke candle",
        "crystal ball candle",
        "moonlight candle",
    ],
    "magic_history": [
        "historic magic manuscript museum",
        "medieval manuscript archive",
        "antique occult book museum",
        "historic witchcraft document",
        "renaissance magic engraving",
    ],
    "default": [
        "moon night sky",
        "candle notebook",
        "stars sky",
        "mystic table candle",
        "crystals candle",
    ],
}

CHANNEL_EMERGENCY_STOCK_IMAGE_QUERIES = [
    "moon",
    "candle",
    "water glass",
    "notebook",
    "night sky",
    "night sky stars",
    "starry sky",
    "galaxy stars",
    "deep space nebula",
    "moon night sky",
    "northern lights sky",
    "abstract night sky",
    "space background stars",
]
CHANNEL_BROAD_REAL_PHOTO_QUERIES = [
    "moon",
    "candle",
    "notebook",
    "water glass",
    "night sky stars",
    "starry sky",
    "window",
    "keys",
    "door",
    "hands",
]
CHANNEL_SPACE_IMAGE_CATEGORIES = {"moon", "planets", "fallback_space"}
OPENVERSE_IMAGES_URL = "https://api.openverse.org/v1/images/"
PEXELS_SEARCH_URL = "https://api.pexels.com/v1/search"
UNSPLASH_SEARCH_URL = "https://api.unsplash.com/search/photos"
NASA_IMAGE_SEARCH_URL = "https://images-api.nasa.gov/search"
CHANNEL_IMAGE_USER_AGENT = f"VoiceOfTheStarsBot/1.0 ({MAIN_BOT_URL})"
CHANNEL_GENERIC_IMAGE_QUERIES = {
    "moon",
    "candle",
    "notebook",
    "water glass",
    "night sky",
    "night sky stars",
    "starry sky",
    "stars sky",
    "galaxy stars",
    "deep space nebula",
    "moon night sky",
    "northern lights sky",
    "abstract night sky",
    "space background stars",
    "window",
    "hands",
}
CHANNEL_ALWAYS_BAD_IMAGE_TERMS = (
    "quote", "typography", "words", "text", "poster", "sign", "book cover",
    "stamps", "coins", "postcard", "postcards", "love is pain", "meme",
    "logo", "font", "lettering", "caption",
)
CHANNEL_IMAGE_SCENE_RULES = [
    {
        "id": "magic_history",
        "needles": (
            "истори", "рукопис", "манускрипт", "архив", "закон о колдовстве",
            "процесс", "салем", "пендл", "джон ди", "маклу", "гальдрабок",
        ),
        "queries": [
            "historic magic manuscript museum",
            "medieval manuscript archive",
            "antique occult book museum",
            "historic witchcraft document",
            "renaissance magic engraving",
        ],
        "required_groups": [
            ("manuscript", "archive", "document", "book", "engraving"),
            ("historic", "medieval", "antique", "renaissance", "museum"),
        ],
        "preferred_terms": ("parchment", "library", "collection", "old", "illustration"),
        "negative_terms": ("modern witch", "costume", "halloween", "cosplay", "poster"),
        "allow_nasa": False,
    },
    {
        "id": "relationship_message",
        "needles": (
            "сообщ", "переписк", "ответ", "отвечает", "молчит", "исчез",
            "жд", "сухо", "бывш", "отношен", "любов", "пара", "границ",
        ),
        "queries": [
            "smartphone on table evening window",
            "person holding phone by window evening",
            "phone message blurred screen table",
            "woman waiting by window phone",
            "mobile phone coffee table evening",
        ],
        "required_groups": [("phone", "smartphone", "mobile"), ("window", "table", "hand", "person")],
        "preferred_terms": ("message", "waiting", "evening", "coffee", "screen", "window"),
        "negative_terms": ("galaxy", "nebula", "zodiac", "constellation"),
        "allow_nasa": False,
    },
    {
        "id": "choice_or_crossroads",
        "needles": ("выбор", "выбрать", "развил", "двер", "ключ", "решени", "ошиб", "направлен"),
        "queries": [
            "two doors hallway decision",
            "crossroads path fog",
            "keys on table decision",
            "person standing at crossroads",
            "open door light hallway",
        ],
        "required_groups": [("door", "crossroads", "path", "keys"), ("decision", "choice", "person", "hallway")],
        "preferred_terms": ("fog", "light", "standing", "open", "way"),
        "negative_terms": ("galaxy", "zodiac", "tarot deck"),
        "allow_nasa": False,
    },
    {
        "id": "tarot_cards",
        "needles": ("таро", "карта дня", "аркан", "расклад", "колод", "умеренность", "сила"),
        "queries": [
            "tarot cards on table natural light",
            "single tarot card on table",
            "three tarot cards spread table",
            "tarot deck coffee cup",
            "tarot cards notebook keys",
        ],
        "required_groups": [("tarot", "cards", "card", "deck")],
        "preferred_terms": ("table", "spread", "notebook", "coffee", "candle"),
        "negative_terms": ("playing cards", "pokemon", "business card", "credit card"),
        "allow_nasa": False,
    },
    {
        "id": "astrology_chart",
        "needles": (
            "наталь", "астролог", "зодиак", "гороскоп", "аспект",
            "транзит", "синастр", "венер", "меркур", "марс", "сатурн", "соляр",
        ),
        "queries": [
            "astrology chart desk daylight",
            "natal chart notebook pen",
            "zodiac wheel chart on desk",
            "star chart notebook candle",
            "astrologer desk sunlight",
        ],
        "required_groups": [("astrology", "zodiac", "natal", "chart", "horoscope"), ("desk", "notebook", "pen", "paper")],
        "preferred_terms": ("sunlight", "wheel", "consultation", "star", "map"),
        "negative_terms": ("galaxy", "nebula", "telescope photo"),
        "allow_nasa": False,
    },
    {
        "id": "moon_or_space",
        "needles": ("луна", "лунн", "новолун", "полнолун", "лилит", "затмени", "планет", "созвезд", "звезд"),
        "queries": [
            "moon night sky",
            "crescent moon dark sky",
            "full moon night sky",
            "moonlight window",
            "constellation night sky",
        ],
        "required_groups": [("moon", "lunar", "night", "sky", "constellation", "stars", "planet")],
        "preferred_terms": ("crescent", "full", "window", "space", "astronomy"),
        "negative_terms": ("quote", "poster", "typography"),
        "allow_nasa": True,
    },
    {
        "id": "dream_journal",
        "needles": ("сон", "снится", "сновид", "кровать", "подуш", "ноч", "засып"),
        "queries": [
            "dream journal bedside table",
            "moonlight bedroom window notebook",
            "notebook on bedside table night",
            "rainy window notebook night",
            "glass of water bedside moonlight",
        ],
        "required_groups": [("bedroom", "bedside", "bed", "window", "night"), ("journal", "notebook", "water", "rain")],
        "preferred_terms": ("moonlight", "sleep", "dream", "quiet", "pillow"),
        "negative_terms": ("galaxy", "tarot", "zodiac wheel"),
        "allow_nasa": False,
    },
    {
        "id": "meditation_practice",
        "needles": ("медитац", "дых", "пауза", "тело", "мантр", "рейки", "настройк", "практик", "минута"),
        "queries": [
            "meditation candle quiet room",
            "person breathing by window",
            "hands on notebook calm room",
            "singing bowl candle",
            "calm desk candle notebook",
        ],
        "required_groups": [("meditation", "breathing", "hands", "person", "room"), ("candle", "notebook", "bowl", "window")],
        "preferred_terms": ("calm", "quiet", "practice", "mindful", "light"),
        "negative_terms": ("galaxy", "zodiac", "poster"),
        "allow_nasa": False,
    },
    {
        "id": "intuition_reality_check",
        "needles": ("интуиц", "тревог", "провер", "факт", "ясност", "знак", "совпад", "случайн", "повторя"),
        "queries": [
            "phone notebook coffee table",
            "person writing notebook by window",
            "window reflection morning",
            "clock night table",
            "street reflection night",
        ],
        "required_groups": [("notebook", "journal", "phone", "clock", "window"), ("writing", "reflection", "table", "street", "coffee")],
        "preferred_terms": ("calm", "check", "facts", "morning", "night"),
        "negative_terms": ("galaxy", "nebula", "zodiac wheel"),
        "allow_nasa": False,
    },
    {
        "id": "family_ancestors",
        "needles": ("родов", "предк", "семейн", "фотограф", "письм", "дед", "бабуш", "чемодан", "комод", "ящик"),
        "queries": [
            "old family photo album",
            "vintage family photographs on table",
            "old letters family photos wooden drawer",
            "antique suitcase family photos",
            "old wooden dresser drawer photographs",
        ],
        "required_groups": [("family", "photo", "photographs", "album", "letters"), ("old", "vintage", "drawer", "suitcase")],
        "preferred_terms": ("wooden", "memory", "ancestor", "paper", "portrait"),
        "negative_terms": ("stamps", "coins", "postcard", "christmas"),
        "allow_nasa": False,
    },
    {
        "id": "numerology_numbers",
        "needles": ("нумер", "числ", "11:11", "22:22", "дата", "матриц", "час"),
        "queries": [
            "clock numbers notebook table",
            "digital clock 11 11 bedside",
            "calendar numbers notebook pen",
            "handwritten numbers notebook",
            "pocket watch notebook numbers",
        ],
        "required_groups": [("clock", "numbers", "calendar", "watch"), ("notebook", "table", "pen", "bedside")],
        "preferred_terms": ("time", "digital", "handwritten", "date", "paper"),
        "negative_terms": ("stock market", "calculator", "poster", "quote"),
        "allow_nasa": False,
    },
    {
        "id": "crystals",
        "needles": ("кристалл", "камн", "кварц", "аметист", "изумруд", "минерал", "талисман", "амулет"),
        "queries": [
            "amethyst crystal close up",
            "quartz crystal on table",
            "crystal cluster natural light",
            "person holding crystal",
            "minerals still life",
        ],
        "required_groups": [("crystal", "quartz", "amethyst", "gem", "stone", "mineral")],
        "preferred_terms": ("close", "table", "hand", "natural", "cluster"),
        "negative_terms": ("jewelry store", "poster", "quote"),
        "allow_nasa": False,
    },
    {
        "id": "coffee_divination",
        "needles": ("кофе", "кофейн", "гуще", "чашк"),
        "queries": [
            "coffee cup on table close up",
            "coffee grounds cup table",
            "empty coffee cup saucer",
            "coffee cup notebook candle",
            "turkish coffee cup table",
        ],
        "required_groups": [("coffee", "cup", "mug"), ("table", "saucer", "grounds", "notebook")],
        "preferred_terms": ("close", "morning", "candle", "reading", "empty"),
        "negative_terms": ("logo", "poster", "quote"),
        "allow_nasa": False,
    },
]


def _plain_channel_text_for_image(text: str) -> str:
    text = re.sub(r"<br\s*/?>", " ", text or "", flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", " ", text)
    return re.sub(r"\s+", " ", text).strip().lower()


def _channel_unique_texts(items, limit: int | None = None) -> list[str]:
    seen = set()
    unique = []
    for item in items or []:
        text = re.sub(r"\s+", " ", str(item or "")).strip()
        if not text:
            continue
        key = text.lower()
        if key in seen:
            continue
        seen.add(key)
        unique.append(text)
        if limit and len(unique) >= limit:
            break
    return unique


def _channel_image_query_clean(query: str) -> str:
    query = re.sub(r"\s+", " ", str(query or "")).strip()
    query = re.sub(
        r"\b(?:no|without|без)\s+(?:text|words|letters|logo|caption|watermark|текста)\b",
        "",
        query,
        flags=re.IGNORECASE,
    )
    return re.sub(r"\s+", " ", query).strip()


def _channel_image_term_groups(terms) -> list[tuple[str, ...]]:
    groups = []
    for item in terms or []:
        if isinstance(item, (list, tuple, set)):
            group = tuple(
                re.sub(r"\s+", " ", str(term or "")).strip().lower()
                for term in item
                if str(term or "").strip()
            )
        else:
            group = (re.sub(r"\s+", " ", str(item or "")).strip().lower(),)
        group = tuple(term for term in group if term)
        if group and group not in groups:
            groups.append(group)
    return groups


def _channel_brief_template() -> dict:
    return {
        "scene_ru": "",
        "queries": [],
        "required_groups": [],
        "preferred_terms": [],
        "negative_terms": list(CHANNEL_ALWAYS_BAD_IMAGE_TERMS),
        "allow_nasa": False,
        "strict": False,
        "source": "rules",
    }


def _normalise_channel_image_brief(raw: dict | None) -> dict:
    brief = _channel_brief_template()
    if not isinstance(raw, dict):
        return brief

    brief["scene_ru"] = str(raw.get("scene_ru") or raw.get("scene") or "").strip()
    brief["queries"] = _channel_unique_texts(
        _channel_image_query_clean(query)
        for query in (raw.get("queries") or raw.get("search_queries") or raw.get("image_queries") or [])
    )[:12]

    required_groups = raw.get("required_groups")
    if not required_groups:
        required_groups = raw.get("required_terms") or raw.get("must_have_terms") or []
    brief["required_groups"] = _channel_image_term_groups(required_groups)[:8]
    brief["preferred_terms"] = _channel_unique_texts(
        str(term).lower()
        for term in (raw.get("preferred_terms") or raw.get("keywords") or raw.get("soft_terms") or [])
    )[:16]
    brief["negative_terms"] = _channel_unique_texts(
        list(CHANNEL_ALWAYS_BAD_IMAGE_TERMS)
        + [str(term).lower() for term in (raw.get("negative_terms") or raw.get("avoid_terms") or [])]
    )[:36]
    brief["allow_nasa"] = bool(raw.get("allow_nasa"))
    brief["strict"] = bool(raw.get("strict") or brief["queries"] or brief["required_groups"])
    brief["source"] = str(raw.get("source") or "ai").strip() or "ai"
    return brief


def _merge_channel_image_briefs(primary: dict, fallback: dict) -> dict:
    merged = _channel_brief_template()
    merged["scene_ru"] = primary.get("scene_ru") or fallback.get("scene_ru") or ""
    merged["queries"] = _channel_unique_texts(
        list(primary.get("queries") or []) + list(fallback.get("queries") or []),
        limit=14,
    )
    merged["required_groups"] = _channel_image_term_groups(
        list(primary.get("required_groups") or []) + list(fallback.get("required_groups") or [])
    )[:9]
    merged["preferred_terms"] = _channel_unique_texts(
        list(primary.get("preferred_terms") or []) + list(fallback.get("preferred_terms") or []),
        limit=22,
    )
    merged["negative_terms"] = _channel_unique_texts(
        list(CHANNEL_ALWAYS_BAD_IMAGE_TERMS)
        + list(primary.get("negative_terms") or [])
        + list(fallback.get("negative_terms") or []),
        limit=40,
    )
    merged["allow_nasa"] = bool(primary.get("allow_nasa") or fallback.get("allow_nasa"))
    merged["strict"] = bool(primary.get("strict") or fallback.get("strict"))
    merged["source"] = "+".join(
        part for part in (primary.get("source"), fallback.get("source")) if part
    ) or "rules"
    return merged


def _channel_image_brief_from_rules(
    topic_info: dict,
    content_plan: dict | None = None,
    post_text: str = "",
) -> dict:
    brief = _channel_brief_template()
    category = topic_info.get("category", "")
    visual = ((content_plan or {}).get("schedule") or topic_info.get("schedule") or {}).get("visual") or {}
    combined = _plain_channel_text_for_image(
        f"{topic_info.get('topic', '')} {visual.get('image_mood', '')} {post_text}"
    )

    visual_queries = [
        _channel_image_query_clean(query)
        for query in visual.get("image_queries", []) or []
        if str(query).strip()
    ]
    if visual.get("image_mood"):
        brief["scene_ru"] = str(visual.get("image_mood") or "").strip()
    if visual_queries:
        brief["queries"].extend(visual_queries)
        brief["strict"] = True

    matched_rules = []
    for rule in CHANNEL_IMAGE_SCENE_RULES:
        if any(needle in combined for needle in rule.get("needles", ())):
            matched_rules.append(rule)

    # For broad astrology/moon topics, keep the category guard even when the final text is abstract.
    if not matched_rules:
        category_rule_ids = {
            "tarot": {"tarot_cards"},
            "divination": {"tarot_cards", "coffee_divination"},
            "astrology": {"astrology_chart"},
            "zodiac": {"astrology_chart"},
            "moon": {"moon_or_space"},
            "planets": {"moon_or_space", "astrology_chart"},
            "dreams": {"dream_journal"},
            "meditation": {"meditation_practice"},
            "numerology": {"numerology_numbers"},
            "crystals": {"crystals"},
            "karma": {"family_ancestors"},
        }.get(category, set())
        matched_rules = [
            rule for rule in CHANNEL_IMAGE_SCENE_RULES
            if rule.get("id") in category_rule_ids
        ]

    rule_queries = []
    required_groups = []
    preferred_terms = []
    negative_terms = []
    allow_nasa = False
    for rule in matched_rules:
        rule_queries.extend(rule.get("queries", []))
        required_groups.extend(rule.get("required_groups", []))
        preferred_terms.extend(rule.get("preferred_terms", []))
        negative_terms.extend(rule.get("negative_terms", []))
        allow_nasa = allow_nasa or bool(rule.get("allow_nasa"))

    # Rule queries are more semantic than schedule mood, so put them first.
    brief["queries"] = _channel_unique_texts(
        [_channel_image_query_clean(query) for query in rule_queries]
        + list(brief["queries"]),
        limit=10,
    )
    brief["required_groups"] = _channel_image_term_groups(required_groups)[:7]
    brief["preferred_terms"] = _channel_unique_texts(preferred_terms, limit=18)
    brief["negative_terms"] = _channel_unique_texts(
        list(CHANNEL_ALWAYS_BAD_IMAGE_TERMS) + negative_terms,
        limit=34,
    )
    brief["allow_nasa"] = allow_nasa or category in CHANNEL_SPACE_IMAGE_CATEGORIES
    brief["strict"] = bool(brief["strict"] or matched_rules)
    return brief


async def _build_channel_image_brief(
    topic_info: dict,
    content_plan: dict | None = None,
    post_text: str = "",
) -> dict:
    rule_brief = _channel_image_brief_from_rules(topic_info, content_plan, post_text)
    if not CHANNEL_USE_AI_IMAGE_BRIEF or not OPENROUTER_KEY or not post_text.strip():
        return rule_brief

    visual = ((content_plan or {}).get("schedule") or topic_info.get("schedule") or {}).get("visual") or {}
    post_plain = _plain_channel_text_for_image(post_text)[:2800]
    prompt = (
        "Ты подбираешь brief для поиска реальной фотографии к посту Telegram-канала.\n"
        "Готовый текст поста - главный источник. Тема и расписание вторичны.\n"
        "Нужно не мистическое настроение вообще, а конкретная сцена, которая видимо отражает смысл именно этого текста.\n"
        "Вытащи из текста предметы, место, действие, эмоциональную ситуацию и главный образ. "
        "Предпочитай бытовые предметы, место и действие. Таро, Луну, свечи, звезды и карты используй только если они реально центральны в посте.\n"
        "Запросы должны быть конкретными: не 'moon' и не 'candle', а сцена из 4-8 слов с предметом, местом и действием.\n"
        "Ответь строго JSON без markdown и без пояснений.\n\n"
        "Поля JSON:\n"
        "- scene_ru: короткое описание сцены на русском.\n"
        "- search_queries: 6-8 английских запросов для Pexels/Unsplash/Openverse, каждый 4-8 слов, без абстрактной эзотерики.\n"
        "- required_terms: 4-8 английских слов или коротких фраз, которые должны быть в метаданных или запросе.\n"
        "- preferred_terms: 6-12 английских слов для мягкого ранжирования.\n"
        "- negative_terms: 5-12 английских слов, чего избегать.\n"
        "- allow_nasa: true только если нужна реальная Луна, планеты, космос, созвездия или астрономия.\n"
        "- strict: true.\n\n"
        "Запреты для картинки: readable text, quotes, posters, typography, logos, memes, screenshots.\n\n"
        f"Категория: {topic_info.get('category', '')}\n"
        f"Тема: {topic_info.get('topic', '')[:500]}\n"
        f"Настроение картинки из расписания: {visual.get('image_mood', '')}\n"
        f"Готовый пост: {post_plain}\n"
    )
    try:
        answer = await ask_ai(prompt, max_tokens=550)
        ai_brief = _normalise_channel_image_brief(extract_json_from_text(answer))
        if not ai_brief.get("queries") and not ai_brief.get("required_groups"):
            return rule_brief
        merged = _merge_channel_image_briefs(ai_brief, rule_brief)
        print(
            "[channel_image] visual brief "
            f"source={merged.get('source')} scene={merged.get('scene_ru')[:120]} "
            f"queries={merged.get('queries')[:3]}"
        )
        return merged
    except Exception as e:
        print(f"[channel_image] visual brief error: {e}")
        return rule_brief


def _specific_channel_image_queries(text: str) -> list[str]:
    rules = [
        (
            ("комод", "ящик", "чемодан", "предк", "семейн", "фотограф", "письм", "дед"),
            [
                "old family photo album",
                "old family photographs",
                "vintage family photo album",
                "vintage family photographs",
                "antique suitcase family photos",
                "old wooden dresser drawer",
            ],
        ),
        (
            ("дизайн человека", "генератор", "отклик", "стратег", "стратегия"),
            [
                "person at crossroads",
                "lighthouse in fog",
                "person looking out window",
                "person walking path fog",
                "compass in hand",
            ],
        ),
        (
            ("кристалл", "кварц", "изумруд", "аметист", "минерал"),
            [
                "quartz crystal close up",
                "emerald crystal close up",
                "amethyst quartz crystals",
                "crystal cluster on table",
                "person holding crystal",
            ],
        ),
        (
            ("сон", "сновид", "подушка", "кровать"),
            [
                "dream journal bedside table",
                "moonlight bedroom window",
                "notebook on bedside table",
                "night window moon",
                "glass of water bedside table",
            ],
        ),
        (
            ("сообщени", "переписк", "телефон", "ответ", "молчани", "ок"),
            [
                "smartphone on table evening window",
                "person holding phone by window",
                "phone message blurred screen table",
                "mobile phone coffee table evening",
                "woman waiting by window phone",
            ],
        ),
        (
            ("метро", "поезд", "станци", "вагон", "платформ"),
            [
                "subway platform morning people",
                "empty metro train station",
                "person standing subway platform",
                "commuter train window reflection",
            ],
        ),
        (
            ("чайник", "кухн", "окно", "чашк", "подоконник"),
            [
                "kettle steam kitchen window",
                "person standing by kitchen window",
                "tea cup windowsill morning",
                "kitchen table cup window light",
            ],
        ),
        (
            ("двер", "порог", "ключ", "развил", "выбор", "решени"),
            [
                "open door hallway light",
                "keys on table decision",
                "person standing at crossroads",
                "two doors hallway decision",
            ],
        ),
        (
            ("вода", "стакан", "лужа", "дожд", "река"),
            [
                "glass of water on table light",
                "rainy window water glass",
                "water reflection on floor",
                "misty river morning path",
            ],
        ),
        (
            ("медитац", "дыхани", "мантр", "рейки", "настройк"),
            [
                "meditation candle",
                "singing bowl candle",
                "incense smoke candle",
                "quiet meditation room",
            ],
        ),
        (
            ("таро", "карта дня", "расклад", "колода", "аркан"),
            [
                "tarot cards on table",
                "tarot deck candle",
                "three tarot cards spread",
                "fortune telling cards candle",
            ],
        ),
        (
            ("луна", "лунн", "новолуние", "полнолуние", "лилит"),
            [
                "crescent moon night sky",
                "full moon night sky",
                "moon phases",
                "moonlight window",
            ],
        ),
        (
            ("наталь", "зодиак", "созвезд", "планет", "меркур", "сатурн"),
            [
                "astrology chart desk",
                "constellation map",
                "telescope night sky",
                "star chart notebook",
            ],
        ),
    ]
    queries = []
    for needles, candidates in rules:
        if any(needle in text for needle in needles):
            queries.extend(candidates)
    seen = set()
    unique = []
    for query in queries:
        if query not in seen:
            seen.add(query)
            unique.append(query)
    return unique


def _has_concrete_channel_image_queries(
    topic_info: dict,
    post_text: str = "",
    visual_brief: dict | None = None,
) -> bool:
    if visual_brief and (visual_brief.get("queries") or visual_brief.get("required_groups")):
        return True
    visual = (topic_info.get("schedule") or {}).get("visual") or {}
    if visual.get("image_queries"):
        return True
    combined = _plain_channel_text_for_image(f"{topic_info.get('topic', '')} {post_text}")
    return bool(_specific_channel_image_queries(combined))


def _channel_stock_image_queries(
    topic_info: dict,
    provider: str,
    post_text: str = "",
    visual_brief: dict | None = None,
) -> list[str]:
    category = topic_info.get("category", "")
    combined_text = _plain_channel_text_for_image(f"{topic_info.get('topic', '')} {post_text}")
    visual = (topic_info.get("schedule") or {}).get("visual") or {}
    brief_queries = [
        _channel_image_query_clean(query)
        for query in (visual_brief or {}).get("queries", []) or []
        if str(query).strip()
    ]
    visual_queries = [
        str(query).strip()
        for query in visual.get("image_queries", []) or []
        if str(query).strip()
    ]
    random.shuffle(visual_queries)
    if category == "fallback_space":
        queries = list(CHANNEL_EMERGENCY_STOCK_IMAGE_QUERIES)
    else:
        specific_queries = _specific_channel_image_queries(combined_text)
        category_queries = list(CHANNEL_STOCK_IMAGE_QUERIES.get(category) or CHANNEL_STOCK_IMAGE_QUERIES["default"])
        random.shuffle(category_queries)
        if visual_brief and visual_brief.get("strict") and brief_queries:
            queries = brief_queries + visual_queries + specific_queries
            if len(queries) < 6:
                queries.extend(category_queries[:max(0, 6 - len(queries))])
        else:
            queries = brief_queries + visual_queries + specific_queries + category_queries
        if provider in {"openverse", "wikimedia"} and not (visual_brief or {}).get("strict"):
            queries.extend(CHANNEL_BROAD_REAL_PHOTO_QUERIES)
    if provider == "nasa":
        if visual_brief and visual_brief.get("allow_nasa") and visual_brief.get("queries"):
            space_queries = [
                query for query in visual_brief.get("queries", [])
                if any(token in query.lower() for token in ("moon", "lunar", "planet", "space", "star", "constellation", "galaxy", "nebula"))
            ]
            queries = space_queries or ["moon", "night sky stars", "star field", "galaxy", "nebula"]
        elif category == "fallback_space":
            queries = ["galaxy", "nebula", "star field", "night sky stars", "moon"]
        elif category == "moon":
            queries = ["moon surface", "lunar eclipse", "crescent moon", "full moon", "moon night sky"]
        elif category == "planets":
            queries = ["planet space", "solar system", "mercury planet", "mars planet", "saturn planet"]
        elif category in {"astrology", "zodiac"}:
            queries = ["constellation", "night sky stars", "astronomy chart", "celestial map", "telescope stars"]
        else:
            queries = ["moon", "night sky stars", "star field", "galaxy", "nebula"]
    elif provider == "wikimedia" and category == "fallback_space":
        queries = ["moon", "night sky stars", "galaxy", "starry sky", "candle"]
    seen = set()
    unique = []
    for query in queries:
        if query not in seen:
            seen.add(query)
            unique.append(query)
    queries = unique
    return queries


def _is_space_image_topic(topic_info: dict) -> bool:
    return topic_info.get("category") in CHANNEL_SPACE_IMAGE_CATEGORIES


def _channel_stock_provider_order(
    topic_info: dict,
    provider: str | None = None,
    post_text: str = "",
    visual_brief: dict | None = None,
) -> list[str]:
    provider = (provider or CHANNEL_IMAGE_PROVIDER).strip().lower()
    if provider in {"openverse", "pexels", "unsplash", "nasa", "wikimedia"}:
        return [provider]
    if provider not in {"stock", "photo", "photos", "auto", "all"}:
        return []

    providers = []
    if _has_concrete_channel_image_queries(topic_info, post_text, visual_brief):
        if PEXELS_API_KEY:
            providers.append("pexels")
        if UNSPLASH_ACCESS_KEY:
            providers.append("unsplash")
    providers.append("openverse")
    providers.append("wikimedia")
    if _is_space_image_topic(topic_info) or bool((visual_brief or {}).get("allow_nasa")):
        providers.append("nasa")
    if PEXELS_API_KEY:
        providers.append("pexels")
    if UNSPLASH_ACCESS_KEY:
        providers.append("unsplash")
    providers = [provider for idx, provider in enumerate(providers) if provider not in providers[:idx]]
    return providers


def _channel_image_hash_distance(left: str, right: str) -> int:
    try:
        return (int(str(left), 16) ^ int(str(right), 16)).bit_count()
    except Exception:
        return 999


def _channel_image_hashes_from_image(image) -> dict[str, str]:
    try:
        from PIL import Image, ImageOps

        gray = ImageOps.exif_transpose(image).convert("L")
        dhash_image = gray.resize((9, 8), Image.Resampling.LANCZOS)
        pixels = list(dhash_image.getdata())
        dhash_value = 0
        for y in range(8):
            row = pixels[y * 9:(y + 1) * 9]
            for x in range(8):
                dhash_value = (dhash_value << 1) | (1 if row[x] > row[x + 1] else 0)

        ahash_image = gray.resize((8, 8), Image.Resampling.LANCZOS)
        ahash_pixels = list(ahash_image.getdata())
        average = sum(ahash_pixels) / max(1, len(ahash_pixels))
        ahash_value = 0
        for pixel in ahash_pixels:
            ahash_value = (ahash_value << 1) | (1 if pixel > average else 0)
        return {
            "dhash": f"{dhash_value:016x}",
            "ahash": f"{ahash_value:016x}",
        }
    except Exception as e:
        print(f"[channel_image] hash error: {e}")
        return {}


def _recent_channel_image_hashes() -> list[dict]:
    state = load_channel_state()
    cutoff = _channel_state_recent_cutoff(CHANNEL_IMAGE_HASH_HISTORY_DAYS)
    hashes = []
    for item in state.get("recent_image_hashes", []) or []:
        if not isinstance(item, dict):
            continue
        created_at = _parse_channel_state_datetime(str(item.get("at") or ""))
        if created_at and created_at < cutoff:
            continue
        if item.get("dhash") or item.get("ahash"):
            hashes.append(item)
    return hashes[-MAX_RECENT_CHANNEL_IMAGE_HASHES:]


def _is_recent_channel_image_hash(image_hashes: dict[str, str]) -> bool:
    if not image_hashes:
        return False
    for item in _recent_channel_image_hashes():
        dhash_distance = 999
        if image_hashes.get("dhash") and item.get("dhash"):
            dhash_distance = _channel_image_hash_distance(image_hashes["dhash"], item["dhash"])
            if dhash_distance <= CHANNEL_IMAGE_DHASH_DISTANCE:
                return True
        if image_hashes.get("ahash") and item.get("ahash"):
            ahash_distance = _channel_image_hash_distance(image_hashes["ahash"], item["ahash"])
            if ahash_distance <= CHANNEL_IMAGE_AHASH_DISTANCE and dhash_distance <= CHANNEL_IMAGE_DHASH_DISTANCE * 2:
                return True
    return False


def _remember_channel_image_hashes(image_hashes: dict[str, str], source: str = "") -> None:
    if not image_hashes:
        return
    state = load_channel_state()
    recent = _recent_channel_image_hashes()
    recent.append({
        "at": _msk_now().isoformat(),
        "source": str(source or "")[:80],
        "dhash": image_hashes.get("dhash", ""),
        "ahash": image_hashes.get("ahash", ""),
    })
    state["recent_image_hashes"] = recent[-MAX_RECENT_CHANNEL_IMAGE_HASHES:]
    save_channel_state(state)


def _recent_channel_image_keys() -> set[str]:
    state = load_channel_state()
    keys = state.get("recent_image_keys", []) or []
    return {str(key) for key in keys if str(key).strip()}


def _stock_image_key(candidate: dict) -> str:
    source = str(candidate.get("source") or "unknown").strip().lower()
    raw_key = (
        candidate.get("dedupe_key")
        or candidate.get("id")
        or candidate.get("page_url")
        or candidate.get("download_url")
        or uuid.uuid4().hex
    )
    return f"{source}:{str(raw_key).strip()}"


def _remember_channel_image(candidate: dict) -> None:
    key = _stock_image_key(candidate)
    if not key:
        return
    state = load_channel_state()
    recent = [
        str(item)
        for item in state.get("recent_image_keys", []) or []
        if str(item).strip() and str(item) != key
    ]
    recent.append(key)
    state["recent_image_keys"] = recent[-MAX_RECENT_CHANNEL_IMAGE_KEYS:]
    save_channel_state(state)


def _store_telegram_channel_photo(data: bytes, source: str) -> str:
    """Decode an external image and store a Telegram-safe RGB JPEG."""
    try:
        from PIL import Image, ImageOps

        with Image.open(BytesIO(data)) as source_image:
            width, height = source_image.size
            if width <= 0 or height <= 0:
                raise ValueError(f"invalid dimensions {width}x{height}")
            if width * height > CHANNEL_TELEGRAM_IMAGE_MAX_PIXELS:
                raise ValueError(f"image has too many pixels: {width}x{height}")
            if max(width, height) / min(width, height) > 20:
                raise ValueError(f"unsupported aspect ratio: {width}x{height}")

            if getattr(source_image, "is_animated", False):
                source_image.seek(0)
            image = ImageOps.exif_transpose(source_image)
            image.load()

        if image.mode in {"RGBA", "LA"} or (image.mode == "P" and "transparency" in image.info):
            rgba = image.convert("RGBA")
            background = Image.new("RGB", rgba.size, (255, 255, 255))
            background.paste(rgba, mask=rgba.getchannel("A"))
            image = background
        elif image.mode != "RGB":
            image = image.convert("RGB")

        max_side = max(512, min(CHANNEL_TELEGRAM_IMAGE_MAX_SIDE, 4096))
        image.thumbnail((max_side, max_side), Image.Resampling.LANCZOS)
        image_hashes = _channel_image_hashes_from_image(image)
        if _is_recent_channel_image_hash(image_hashes):
            print(f"[channel_image] rejected visually repeated image from {source}")
            return ""

        encoded = b""
        for quality in (90, 84, 76):
            output = BytesIO()
            image.save(output, format="JPEG", quality=quality, optimize=True, progressive=True)
            encoded = output.getvalue()
            if len(encoded) <= CHANNEL_STOCK_IMAGE_MAX_BYTES:
                break
        if not encoded or len(encoded) > CHANNEL_STOCK_IMAGE_MAX_BYTES:
            raise ValueError(f"normalized JPEG is too large: {len(encoded)} bytes")

        os.makedirs(CHANNEL_IMAGE_ASSET_DIR, exist_ok=True)
        path = os.path.join(CHANNEL_IMAGE_ASSET_DIR, f"channel_{uuid.uuid4().hex}.jpg")
        with open(path, "wb") as f:
            f.write(encoded)
        _remember_channel_image_hashes(image_hashes, source)
        _cleanup_generated_channel_images()
        return path
    except Exception as e:
        print(f"[channel_image] invalid image from {source}: {e}")
        return ""


def _stock_json_headers(provider: str) -> dict:
    headers = {"Accept": "application/json", "User-Agent": CHANNEL_IMAGE_USER_AGENT}
    if provider == "pexels" and PEXELS_API_KEY:
        headers["Authorization"] = PEXELS_API_KEY
    if provider == "unsplash" and UNSPLASH_ACCESS_KEY:
        headers["Authorization"] = f"Client-ID {UNSPLASH_ACCESS_KEY}"
        headers["Accept-Version"] = "v1"
    return headers


async def _get_stock_json(
    session: aiohttp.ClientSession,
    provider: str,
    url: str,
    params: dict | None = None,
) -> dict | list | None:
    try:
        async with session.get(
            url,
            params=params,
            headers=_stock_json_headers(provider),
            proxy=PROXY_URL or None,
        ) as response:
            if response.status != 200:
                preview = (await response.text())[:180]
                print(f"[channel_image] {provider} status {response.status}: {preview}")
                return None
            return await response.json(content_type=None)
    except Exception as e:
        print(f"[channel_image] {provider} json error: {e}")
        return None


def _dedupe_stock_candidates(candidates: list[dict]) -> list[dict]:
    seen = set()
    unique = []
    for candidate in candidates:
        key = _stock_image_key(candidate)
        if key in seen:
            continue
        seen.add(key)
        unique.append(candidate)
    return unique


def _channel_image_contains_any(haystack: str, terms) -> bool:
    return any(str(term or "").lower() in haystack for term in terms if str(term or "").strip())


def _stock_candidate_relevance_score(candidate: dict, visual_brief: dict | None = None) -> int:
    query = str(candidate.get("query") or "").lower()
    title_haystack = str(candidate.get("title") or "").lower()
    metadata_haystack = " ".join(
        str(candidate.get(field) or "").lower()
        for field in ("title", "page_url", "download_url")
    )
    full_haystack = f"{metadata_haystack} {query}"
    score = 30 - int(candidate.get("query_rank", 99)) * 5
    source = str(candidate.get("source") or "").lower()
    if source in {"pexels", "unsplash"}:
        score += 3

    score -= sum(10 for token in CHANNEL_ALWAYS_BAD_IMAGE_TERMS if token in full_haystack)

    if visual_brief:
        if source == "nasa" and not visual_brief.get("allow_nasa"):
            score -= 45
        if visual_brief.get("strict") and query.strip() in CHANNEL_GENERIC_IMAGE_QUERIES:
            score -= 38

        required_hits = 0
        for group in (visual_brief.get("required_groups") or [])[:9]:
            terms = [str(term or "").lower() for term in group if str(term or "").strip()]
            if not terms:
                continue
            if _channel_image_contains_any(metadata_haystack, terms):
                score += 24
                required_hits += 1
            elif _channel_image_contains_any(query, terms):
                score += 10
                required_hits += 1
            else:
                score -= 18
        if visual_brief.get("strict") and visual_brief.get("required_groups") and required_hits == 0:
            score -= 20

        preferred_hits = 0
        for term in (visual_brief.get("preferred_terms") or [])[:22]:
            term = str(term or "").lower().strip()
            if not term:
                continue
            if term in metadata_haystack:
                score += 6
                preferred_hits += 1
            elif term in query:
                score += 3
                preferred_hits += 1
        score += min(preferred_hits, 7)

        for term in (visual_brief.get("negative_terms") or [])[:40]:
            term = str(term or "").lower().strip()
            if not term:
                continue
            if term in metadata_haystack:
                score -= 18
            elif term in query:
                score -= 8

    if any(token in query for token in ("family", "photo", "album", "suitcase", "dresser", "drawer")):
        good = ("family", "photo", "photos", "album", "scrapbook", "vintage", "elderly", "portrait", "drawer", "dresser", "suitcase")
        bad = ("stamps", "coins", "postcard", "postcards", "dog", "books", "tableware", "christmas", "love", "pain")
        score += sum(5 for token in good if token in title_haystack)
        score -= sum(12 for token in bad if token in full_haystack)

    if any(token in query for token in ("crystal", "quartz", "emerald", "amethyst")):
        good = ("crystal", "quartz", "emerald", "amethyst", "gem", "stone", "mineral")
        score += sum(5 for token in good if token in title_haystack)

    if any(token in query for token in ("crossroads", "lighthouse", "fog", "window", "path", "compass")):
        good = ("crossroads", "lighthouse", "fog", "window", "path", "person", "walking", "compass")
        bad = ("book", "page", "quote", "text", "typography", "poster", "sign")
        score += sum(5 for token in good if token in title_haystack)
        score -= sum(10 for token in bad if token in full_haystack)

    return score


def _openverse_candidates(data: dict, source_label: str) -> list[dict]:
    candidates = []
    for item in data.get("results", []) or []:
        if not isinstance(item, dict):
            continue
        extension = str(item.get("extension") or "").lower()
        if extension and extension not in {"jpg", "jpeg", "png", "webp"}:
            continue
        download_url = item.get("url") or item.get("thumbnail")
        if not download_url:
            continue
        image_source = source_label if source_label != "openverse-wikimedia" else "wikimedia"
        candidates.append({
            "source": image_source,
            "id": item.get("id"),
            "dedupe_key": item.get("id") or download_url,
            "download_url": download_url,
            "fallback_urls": [url for url in [item.get("thumbnail")] if url and url != download_url],
            "page_url": item.get("foreign_landing_url") or item.get("url"),
            "title": item.get("title") or "",
            "license": item.get("license") or "",
        })
    return candidates


async def _query_openverse_images(
    session: aiohttp.ClientSession,
    query: str,
    page: int,
    source: str | None = None,
) -> list[dict]:
    page_size = max(5, min(CHANNEL_STOCK_IMAGE_PAGE_SIZE, 20))
    params = {
        "q": query,
        "page": page,
        "page_size": page_size,
        "size": "large",
        "license": "cc0,pdm",
        "mature": "false",
    }
    if source:
        params["source"] = source
    data = await _get_stock_json(session, "openverse", OPENVERSE_IMAGES_URL, params)
    if not isinstance(data, dict):
        return []
    return _openverse_candidates(data, "openverse-wikimedia" if source == "wikimedia" else "openverse")


async def _query_pexels_images(session: aiohttp.ClientSession, query: str, page: int) -> list[dict]:
    if not PEXELS_API_KEY:
        return []
    page_size = max(5, min(CHANNEL_STOCK_IMAGE_PAGE_SIZE, 80))
    params = {"query": query, "page": page, "per_page": page_size, "orientation": "square"}
    data = await _get_stock_json(session, "pexels", PEXELS_SEARCH_URL, params)
    if not isinstance(data, dict):
        return []

    candidates = []
    for item in data.get("photos", []) or []:
        src = item.get("src") or {}
        download_url = src.get("large2x") or src.get("large") or src.get("original")
        if not download_url:
            continue
        fallbacks = [url for url in [src.get("large"), src.get("original"), src.get("medium")] if url and url != download_url]
        candidates.append({
            "source": "pexels",
            "id": item.get("id"),
            "dedupe_key": item.get("id") or download_url,
            "download_url": download_url,
            "fallback_urls": fallbacks,
            "page_url": item.get("url") or "",
            "title": item.get("alt") or "",
        })
    return candidates


async def _query_unsplash_images(session: aiohttp.ClientSession, query: str, page: int) -> list[dict]:
    if not UNSPLASH_ACCESS_KEY:
        return []
    page_size = max(5, min(CHANNEL_STOCK_IMAGE_PAGE_SIZE, 30))
    params = {
        "query": query,
        "page": page,
        "per_page": page_size,
        "orientation": "squarish",
        "content_filter": "high",
    }
    data = await _get_stock_json(session, "unsplash", UNSPLASH_SEARCH_URL, params)
    if not isinstance(data, dict):
        return []

    candidates = []
    for item in data.get("results", []) or []:
        urls = item.get("urls") or {}
        links = item.get("links") or {}
        download_url = urls.get("regular") or urls.get("full") or urls.get("small")
        if not download_url:
            continue
        fallbacks = [url for url in [urls.get("full"), urls.get("small"), urls.get("thumb")] if url and url != download_url]
        candidates.append({
            "source": "unsplash",
            "id": item.get("id"),
            "dedupe_key": item.get("id") or download_url,
            "download_url": download_url,
            "fallback_urls": fallbacks,
            "page_url": links.get("html") or "",
            "download_location": links.get("download_location") or "",
            "title": item.get("alt_description") or item.get("description") or "",
        })
    return candidates


async def _query_nasa_images(session: aiohttp.ClientSession, query: str, page: int) -> list[dict]:
    page_size = max(5, min(CHANNEL_STOCK_IMAGE_PAGE_SIZE, 100))
    params = {"q": query, "media_type": "image", "page": page, "page_size": page_size}
    data = await _get_stock_json(session, "nasa", NASA_IMAGE_SEARCH_URL, params)
    if not isinstance(data, dict):
        return []

    candidates = []
    for item in ((data.get("collection") or {}).get("items") or []):
        data_items = item.get("data") or []
        meta = data_items[0] if data_items and isinstance(data_items[0], dict) else {}
        links = item.get("links") or []
        image_links = [
            link.get("href")
            for link in links
            if isinstance(link, dict) and link.get("href") and link.get("render") == "image"
        ]
        download_url = image_links[0] if image_links else ""
        if not download_url:
            continue
        candidates.append({
            "source": "nasa",
            "id": meta.get("nasa_id"),
            "dedupe_key": meta.get("nasa_id") or download_url,
            "download_url": download_url,
            "fallback_urls": image_links[1:],
            "asset_manifest_url": item.get("href") or "",
            "page_url": f"https://images.nasa.gov/details/{meta.get('nasa_id')}" if meta.get("nasa_id") else "",
            "title": meta.get("title") or "",
        })
    return candidates


async def _query_stock_provider(
    session: aiohttp.ClientSession,
    provider: str,
    topic_info: dict,
    post_text: str = "",
    visual_brief: dict | None = None,
) -> list[dict]:
    queries = _channel_stock_image_queries(topic_info, provider, post_text, visual_brief)
    combined_text = _plain_channel_text_for_image(f"{topic_info.get('topic', '')} {post_text}")
    specific_count = len((visual_brief or {}).get("queries") or []) + len(_specific_channel_image_queries(combined_text))
    candidates = []
    max_page = max(1, CHANNEL_STOCK_IMAGE_MAX_PAGE)
    query_attempts = CHANNEL_STOCK_IMAGE_QUERY_ATTEMPTS
    if provider in {"openverse", "wikimedia", "nasa"}:
        query_attempts = max(query_attempts, 10)
    if visual_brief and visual_brief.get("strict"):
        query_attempts = max(query_attempts, 8)
    attempts = max(1, min(query_attempts, len(queries)))

    for rank, query in enumerate(queries[:attempts]):
        page_limit = min(max_page, 4) if rank >= specific_count else 1
        page = random.randint(1, page_limit)
        if provider == "openverse":
            batch = await _query_openverse_images(session, query, page)
        elif provider == "wikimedia":
            batch = await _query_openverse_images(session, query, page, source="wikimedia")
        elif provider == "pexels":
            batch = await _query_pexels_images(session, query, page)
        elif provider == "unsplash":
            batch = await _query_unsplash_images(session, query, page)
        elif provider == "nasa":
            batch = await _query_nasa_images(session, query, page)
        else:
            batch = []
        if not batch:
            print(f"[channel_image] {provider} no candidates for query '{query}' page {page}")
        for candidate in batch:
            candidate["query"] = query
            candidate["query_rank"] = rank
        candidates.extend(batch)
        if len(candidates) >= CHANNEL_STOCK_IMAGE_POOL_TARGET:
            break

    return _dedupe_stock_candidates(candidates)


async def _nasa_asset_urls(session: aiohttp.ClientSession, candidate: dict) -> list[str]:
    manifest_url = candidate.get("asset_manifest_url")
    if not manifest_url:
        return []
    data = await _get_stock_json(session, "nasa", manifest_url)
    if not isinstance(data, list):
        return []
    image_urls = [
        url for url in data
        if isinstance(url, str) and url.lower().split("?", 1)[0].endswith((".jpg", ".jpeg", ".png", ".webp"))
    ]
    image_urls.sort(key=lambda url: (
        0 if "~orig" in url.lower() else
        1 if "~large" in url.lower() else
        2 if "~medium" in url.lower() else
        3 if "~small" in url.lower() else
        4
    ))
    return image_urls[:4]


async def _notify_unsplash_download(session: aiohttp.ClientSession, candidate: dict) -> None:
    download_location = candidate.get("download_location")
    if not download_location or not UNSPLASH_ACCESS_KEY:
        return
    try:
        async with session.get(
            download_location,
            headers=_stock_json_headers("unsplash"),
            proxy=PROXY_URL or None,
        ) as response:
            await response.read()
    except Exception as e:
        print(f"[channel_image] unsplash download notice error: {e}")


async def _download_stock_image_candidate(
    session: aiohttp.ClientSession,
    candidate: dict,
) -> str:
    os.makedirs(CHANNEL_IMAGE_ASSET_DIR, exist_ok=True)
    urls = []
    if candidate.get("source") == "nasa":
        urls.extend(await _nasa_asset_urls(session, candidate))
    urls.append(candidate.get("download_url"))
    urls.extend(candidate.get("fallback_urls") or [])
    urls = [url for url in urls if isinstance(url, str) and url.strip()]

    if candidate.get("source") == "unsplash":
        await _notify_unsplash_download(session, candidate)

    headers = {
        "Accept": "image/jpeg,image/png,image/webp,*/*",
        "User-Agent": CHANNEL_IMAGE_USER_AGENT,
    }
    for url in urls[:5]:
        try:
            async with session.get(url, headers=headers, proxy=PROXY_URL or None) as response:
                content_type = response.headers.get("Content-Type", "").lower()
                content_length = int(response.headers.get("Content-Length") or 0)
                if response.status != 200:
                    print(f"[channel_image] download status {response.status} from {candidate.get('source')}: {url[:120]}")
                    continue
                if content_length and content_length > CHANNEL_STOCK_IMAGE_MAX_BYTES:
                    print(f"[channel_image] download too large from {candidate.get('source')}: {content_length} bytes")
                    continue
                data = await response.read()
                if len(data) < CHANNEL_STOCK_IMAGE_MIN_BYTES or len(data) > CHANNEL_STOCK_IMAGE_MAX_BYTES:
                    print(f"[channel_image] download rejected from {candidate.get('source')}: {len(data)} bytes")
                    continue
                if content_type and not content_type.startswith("image/"):
                    print(f"[channel_image] download non-image from {candidate.get('source')}: {content_type}")
                    continue
                path = _store_telegram_channel_photo(data, candidate.get("source") or url[:120])
                if path:
                    return path
        except Exception as e:
            print(f"[channel_image] download error from {candidate.get('source')}: {e}")
    return ""


async def _generate_stock_channel_image_asset(
    topic_info: dict,
    author_info: dict | None = None,
    content_plan: dict | None = None,
    provider: str | None = None,
    post_text: str = "",
    visual_brief: dict | None = None,
) -> str:
    providers = _channel_stock_provider_order(topic_info, provider, post_text, visual_brief)
    if not providers:
        return ""

    used_keys = _recent_channel_image_keys()
    timeout = aiohttp.ClientTimeout(total=CHANNEL_STOCK_IMAGE_TIMEOUT)
    async with aiohttp.ClientSession(timeout=timeout) as image_session:
        for provider in providers:
            pool = await _query_stock_provider(image_session, provider, topic_info, post_text, visual_brief)
            if not pool:
                print(f"[channel_image] {provider} returned empty pool")
                continue
            unused_pool = [candidate for candidate in pool if _stock_image_key(candidate) not in used_keys]
            if not unused_pool:
                print(f"[channel_image] {provider} pool exhausted by recent image dedupe ({len(pool)} candidates)")
                continue
            scored_pool = [
                (_stock_candidate_relevance_score(candidate, visual_brief), candidate)
                for candidate in unused_pool
            ]
            scored_pool.sort(key=lambda item: (-item[0], random.random()))
            if visual_brief and visual_brief.get("strict"):
                strong_pool = [
                    (score, candidate)
                    for score, candidate in scored_pool
                    if score >= CHANNEL_IMAGE_MIN_BRIEF_SCORE
                ]
                if not strong_pool:
                    best_score = scored_pool[0][0] if scored_pool else "-"
                    print(f"[channel_image] {provider} no strong visual matches (best={best_score})")
                    continue
                scored_pool = strong_pool
            for score, candidate in scored_pool[:8]:
                image_path = await _download_stock_image_candidate(image_session, candidate)
                if image_path:
                    _remember_channel_image(candidate)
                    print(
                        f"[channel_image] selected {candidate.get('source')} image "
                        f"{candidate.get('id') or candidate.get('page_url') or ''} score={score}"
                    )
                    return image_path
    return ""


async def _generate_emergency_stock_channel_image_asset() -> str:
    topic_info = {
        "category": "fallback_space",
        "topic": " ".join(CHANNEL_EMERGENCY_STOCK_IMAGE_QUERIES),
    }
    image_path = await _generate_stock_channel_image_asset(
        topic_info,
        provider="stock",
        post_text=topic_info["topic"],
    )
    if image_path:
        print("[channel_image] selected emergency stock fallback image")
    return image_path


def _blend_rgb(a: tuple[int, int, int], b: tuple[int, int, int], ratio: float) -> tuple[int, int, int]:
    ratio = max(0, min(1, ratio))
    return tuple(int(a[i] * (1 - ratio) + b[i] * ratio) for i in range(3))


def _adjust_rgb(color: tuple[int, int, int], amount: float) -> tuple[int, int, int]:
    if amount >= 0:
        return _blend_rgb(color, (255, 255, 255), amount)
    return _blend_rgb(color, (0, 0, 0), abs(amount))


def _rgba(color: tuple[int, int, int], alpha: int) -> tuple[int, int, int, int]:
    return color + (max(0, min(255, alpha)),)


def _draw_shadowed_round(draw, box, radius: int, fill, outline=None, width: int = 2, shadow: int = 70) -> None:
    x1, y1, x2, y2 = box
    draw.rounded_rectangle((x1 + 12, y1 + 14, x2 + 12, y2 + 14), radius=radius, fill=(0, 0, 0, shadow))
    draw.rounded_rectangle(box, radius=radius, fill=fill, outline=outline, width=width)


def _draw_scene_background(draw, width: int, height: int, palette: dict, rng) -> None:
    top = _adjust_rgb(palette["bg"], rng.uniform(-0.08, 0.08))
    bottom = _adjust_rgb(palette["mid"], rng.uniform(-0.02, 0.12))
    for y in range(height):
        draw.line([(0, y), (width, y)], fill=_rgba(_blend_rgb(top, bottom, y / height), 255))

    horizon = rng.randint(600, 700)
    table = _adjust_rgb(palette["mid"], -0.12)
    draw.polygon([(0, horizon), (width, horizon - rng.randint(15, 55)), (width, height), (0, height)], fill=_rgba(table, 235))
    for offset in range(-width, width * 2, 145):
        draw.line((offset, horizon + 10, offset + 260, height), fill=_rgba(palette["accent"], 35), width=2)
    for y in range(horizon + 110, height, 110):
        draw.line((0, y, width, y - 22), fill=(255, 255, 255, 18), width=2)


def _draw_small_star(draw, x: int, y: int, radius: int, color) -> None:
    draw.line((x - radius, y, x + radius, y), fill=color, width=max(1, radius // 5))
    draw.line((x, y - radius, x, y + radius), fill=color, width=max(1, radius // 5))
    draw.ellipse((x - 2, y - 2, x + 2, y + 2), fill=color)


def _draw_constellation(draw, rng, box, palette: dict, count: int = 7) -> None:
    x1, y1, x2, y2 = box
    points = [(rng.randint(x1, x2), rng.randint(y1, y2)) for _ in range(count)]
    points.sort()
    for a, b in zip(points, points[1:]):
        draw.line((a[0], a[1], b[0], b[1]), fill=_rgba(palette["ink"], 90), width=2)
    for x, y in points:
        r = rng.randint(3, 7)
        draw.ellipse((x - r, y - r, x + r, y + r), fill=_rgba(palette["ink"], 210))


def _draw_candle(draw, x: int, y: int, scale: float, palette: dict, rng) -> None:
    w = int(62 * scale)
    h = int(165 * scale)
    wax = _blend_rgb(palette["ink"], (250, 231, 195), 0.55)
    draw.ellipse((x - w, y + h - 15, x + w, y + h + 22), fill=(0, 0, 0, 45))
    draw.rounded_rectangle((x - w // 2, y, x + w // 2, y + h), radius=max(8, w // 5), fill=_rgba(wax, 235), outline=_rgba(palette["accent"], 190), width=2)
    draw.ellipse((x - w // 2, y - 9, x + w // 2, y + 17), fill=_rgba(_adjust_rgb(wax, 0.15), 245), outline=_rgba(palette["accent"], 170), width=2)
    for drip in range(rng.randint(1, 3)):
        dx = rng.randint(-w // 3, w // 3)
        drip_h = rng.randint(24, 55)
        draw.rounded_rectangle((x + dx - 5, y + 8, x + dx + 5, y + 8 + drip_h), radius=5, fill=_rgba(_adjust_rgb(wax, 0.08), 205))
    draw.line((x, y - 2, x, y - 25), fill=(55, 38, 30, 210), width=3)
    flame_outer = [(x, y - int(86 * scale)), (x - int(30 * scale), y - int(28 * scale)), (x, y - int(8 * scale)), (x + int(30 * scale), y - int(28 * scale))]
    draw.polygon(flame_outer, fill=(239, 150, 59, 210))
    flame_inner = [(x, y - int(62 * scale)), (x - int(14 * scale), y - int(28 * scale)), (x, y - int(14 * scale)), (x + int(14 * scale), y - int(28 * scale))]
    draw.polygon(flame_inner, fill=(255, 236, 158, 235))


def _draw_crystal_cluster(draw, x: int, y: int, scale: float, palette: dict, rng) -> None:
    colors = [
        _blend_rgb(palette["accent"], palette["ink"], 0.35),
        _adjust_rgb(palette["accent"], 0.16),
        _blend_rgb(palette["mid"], palette["ink"], 0.36),
    ]
    for i in range(5):
        base = int((48 + i * 10) * scale)
        height = int(rng.randint(95, 175) * scale)
        cx = x + int((i - 2) * 42 * scale) + rng.randint(-8, 8)
        color = colors[i % len(colors)]
        points = [
            (cx, y - height),
            (cx - base // 2, y - int(18 * scale)),
            (cx - base // 3, y + int(18 * scale)),
            (cx + base // 3, y + int(18 * scale)),
            (cx + base // 2, y - int(18 * scale)),
        ]
        draw.polygon(points, fill=_rgba(color, 215), outline=_rgba(palette["ink"], 110))
        draw.line((cx, y - height, cx, y + int(14 * scale)), fill=_rgba(palette["ink"], 75), width=2)
        draw.line((cx, y - int(height * 0.58), cx + base // 2, y - int(18 * scale)), fill=_rgba(palette["ink"], 55), width=2)
    draw.ellipse((x - int(150 * scale), y + int(5 * scale), x + int(150 * scale), y + int(45 * scale)), fill=(0, 0, 0, 50))


def _draw_tarot_card(draw, box, palette: dict, symbol: str, font, rng, fill_alpha: int = 235) -> None:
    x1, y1, x2, y2 = box
    card_fill = _blend_rgb(palette["ink"], (244, 218, 183), 0.62)
    _draw_shadowed_round(draw, box, 24, _rgba(card_fill, fill_alpha), _rgba(palette["accent"], 210), width=3, shadow=52)
    draw.rounded_rectangle((x1 + 18, y1 + 18, x2 - 18, y2 - 18), radius=16, outline=_rgba(palette["mid"], 130), width=2)
    draw.text((x1 + 24, y1 + 18), symbol, font=font, fill=_rgba(palette["bg"], 190))
    cx = (x1 + x2) // 2
    cy = (y1 + y2) // 2
    draw.ellipse((cx - 46, cy - 46, cx + 46, cy + 46), outline=_rgba(palette["accent"], 185), width=4)
    if rng.random() < 0.5:
        draw.arc((cx - 31, cy - 38, cx + 35, cy + 38), 78, 282, fill=_rgba(palette["bg"], 170), width=8)
    else:
        _draw_small_star(draw, cx, cy, 43, _rgba(palette["bg"], 165))
    draw.line((x1 + 48, y2 - 56, x2 - 48, y2 - 56), fill=_rgba(palette["mid"], 110), width=2)


def _draw_tarot_spread_scene(draw, width: int, height: int, palette: dict, rng, symbol_font, small_font) -> None:
    symbols = rng.sample((CHANNEL_GENERATED_IMAGE_SYMBOLS["tarot"] * 3), 3)
    card_w, card_h = 205, 330
    positions = [(236, 326), (440, 292), (642, 342)]
    for (x, y), symbol in zip(positions, symbols):
        _draw_tarot_card(draw, (x, y, x + card_w, y + card_h), palette, symbol, symbol_font, rng)
    _draw_candle(draw, 205, 655, 1.0, palette, rng)
    _draw_crystal_cluster(draw, 848, 710, 0.74, palette, rng)
    draw.rounded_rectangle((430, 724, 665, 805), radius=20, fill=_rgba(_adjust_rgb(palette["bg"], 0.12), 210), outline=_rgba(palette["accent"], 155), width=3)
    for i in range(4):
        draw.line((455 + i * 42, 740, 455 + i * 42, 790), fill=_rgba(palette["accent"], 120), width=2)
    _draw_constellation(draw, rng, (710, 170, 940, 285), palette, 6)


def _draw_crystal_ball_scene(draw, width: int, height: int, palette: dict, rng, symbol_font, small_font) -> None:
    cx, cy, r = 540, 490, 225
    draw.ellipse((cx - r + 18, cy - r + 28, cx + r + 18, cy + r + 28), fill=(0, 0, 0, 65))
    draw.ellipse((cx - r, cy - r, cx + r, cy + r), fill=_rgba(_blend_rgb(palette["mid"], palette["ink"], 0.28), 155), outline=_rgba(palette["ink"], 170), width=4)
    draw.ellipse((cx - 150, cy - 176, cx - 40, cy - 60), fill=(255, 255, 255, 45))
    draw.arc((cx - 90, cy - 105, cx + 90, cy + 105), 70, 290, fill=_rgba(palette["ink"], 155), width=11)
    _draw_constellation(draw, rng, (cx - 110, cy - 55, cx + 125, cy + 100), palette, 8)
    draw.rounded_rectangle((cx - 175, cy + r - 5, cx + 175, cy + r + 70), radius=34, fill=_rgba(_adjust_rgb(palette["accent"], -0.1), 215), outline=_rgba(palette["ink"], 120), width=3)
    draw.ellipse((cx - 240, cy + r + 50, cx + 240, cy + r + 110), fill=(0, 0, 0, 60))
    _draw_candle(draw, 210, 655, 0.9, palette, rng)
    _draw_tarot_card(draw, (725, 655, 890, 905), palette, rng.choice(CHANNEL_GENERATED_IMAGE_SYMBOLS["tarot"]), symbol_font, rng, fill_alpha=220)


def _draw_pendulum_map_scene(draw, width: int, height: int, palette: dict, rng, symbol_font, small_font) -> None:
    paper = (175, 255, 905, 820)
    parchment = _blend_rgb(palette["ink"], (229, 200, 154), 0.54)
    _draw_shadowed_round(draw, paper, 28, _rgba(parchment, 232), _rgba(palette["accent"], 170), width=3, shadow=62)
    for _ in range(5):
        x1 = rng.randint(230, 770)
        y1 = rng.randint(325, 720)
        x2 = x1 + rng.randint(-110, 130)
        y2 = y1 + rng.randint(-95, 115)
        draw.line((x1, y1, x2, y2), fill=_rgba(palette["mid"], 105), width=4)
        draw.ellipse((x1 - 8, y1 - 8, x1 + 8, y1 + 8), fill=_rgba(palette["accent"], 155))
    draw.ellipse((405, 420, 675, 690), outline=_rgba(palette["bg"], 145), width=4)
    for angle in range(0, 360, 45):
        rad = math.radians(angle)
        draw.line((540, 555, 540 + int(122 * math.cos(rad)), 555 + int(122 * math.sin(rad))), fill=_rgba(palette["bg"], 80), width=2)
    draw.line((540, 120, 540, 370), fill=_rgba(palette["ink"], 160), width=3)
    draw.polygon([(540, 370), (494, 475), (540, 540), (586, 475)], fill=_rgba(_adjust_rgb(palette["accent"], 0.12), 225), outline=_rgba(palette["ink"], 140))
    _draw_candle(draw, 840, 650, 0.75, palette, rng)


def _draw_chart_wheel(draw, cx: int, cy: int, radius: int, palette: dict, rng, symbol_font) -> None:
    for r, alpha in ((radius, 185), (int(radius * 0.72), 135), (int(radius * 0.42), 95)):
        draw.ellipse((cx - r, cy - r, cx + r, cy + r), outline=_rgba(palette["accent"], alpha), width=3)
    zodiac = CHANNEL_GENERATED_IMAGE_SYMBOLS["zodiac"]
    planets = CHANNEL_GENERATED_IMAGE_SYMBOLS["planets"]
    for i in range(12):
        angle = math.radians(i * 30 - 90)
        x = cx + int(radius * math.cos(angle))
        y = cy + int(radius * math.sin(angle))
        draw.line((cx, cy, x, y), fill=_rgba(palette["mid"], 105), width=2)
        tx = cx + int((radius + 38) * math.cos(angle)) - 17
        ty = cy + int((radius + 38) * math.sin(angle)) - 23
        draw.text((tx, ty), zodiac[i], font=symbol_font, fill=_rgba(palette["bg"], 180))
    for i in range(7):
        a = math.radians(rng.randint(0, 359))
        dist = rng.randint(int(radius * 0.2), int(radius * 0.68))
        x = cx + int(dist * math.cos(a))
        y = cy + int(dist * math.sin(a))
        draw.text((x - 13, y - 17), planets[i % len(planets)], font=symbol_font, fill=_rgba(palette["accent"], 190))


def _draw_natal_chart_scene(draw, width: int, height: int, palette: dict, rng, symbol_font, small_font) -> None:
    paper = (190, 170, 890, 890)
    parchment = _blend_rgb(palette["ink"], (241, 224, 190), 0.55)
    _draw_shadowed_round(draw, paper, 34, _rgba(parchment, 232), _rgba(palette["accent"], 180), width=3, shadow=70)
    _draw_chart_wheel(draw, 540, 530, 270, palette, rng, symbol_font)
    draw.line((750, 770, 910, 920), fill=_rgba(_adjust_rgb(palette["bg"], 0.05), 210), width=18)
    draw.polygon([(895, 902), (940, 948), (878, 925)], fill=_rgba(palette["accent"], 220))
    _draw_candle(draw, 153, 645, 0.82, palette, rng)


def _draw_astrolabe_scene(draw, width: int, height: int, palette: dict, rng, symbol_font, small_font) -> None:
    cx, cy = 540, 535
    draw.ellipse((220, 220, 860, 860), fill=(0, 0, 0, 48))
    for r, alpha in ((310, 220), (250, 175), (180, 135), (95, 115)):
        draw.ellipse((cx - r, cy - r, cx + r, cy + r), outline=_rgba(palette["accent"], alpha), width=5 if r == 310 else 3)
    for angle in range(0, 360, 15):
        rad = math.radians(angle)
        outer = 310
        inner = 282 if angle % 45 else 260
        draw.line((cx + int(inner * math.cos(rad)), cy + int(inner * math.sin(rad)), cx + int(outer * math.cos(rad)), cy + int(outer * math.sin(rad))), fill=_rgba(palette["ink"], 120), width=2)
    pointer_angle = math.radians(rng.choice([25, 70, 118, 205, 294]))
    draw.line((cx - int(250 * math.cos(pointer_angle)), cy - int(250 * math.sin(pointer_angle)), cx + int(250 * math.cos(pointer_angle)), cy + int(250 * math.sin(pointer_angle))), fill=_rgba(palette["ink"], 180), width=6)
    draw.ellipse((cx - 18, cy - 18, cx + 18, cy + 18), fill=_rgba(palette["accent"], 240))
    _draw_constellation(draw, rng, (140, 145, 360, 315), palette, 7)
    _draw_crystal_cluster(draw, 835, 825, 0.55, palette, rng)


def _draw_moon_window_scene(draw, width: int, height: int, palette: dict, rng, symbol_font, small_font) -> None:
    frame = _adjust_rgb(palette["bg"], -0.04)
    draw.rounded_rectangle((180, 120, 900, 690), radius=38, fill=_rgba(frame, 235), outline=_rgba(palette["accent"], 140), width=4)
    draw.rounded_rectangle((225, 165, 855, 635), radius=26, fill=_rgba((16, 24, 46), 230), outline=_rgba(palette["ink"], 75), width=2)
    draw.line((540, 165, 540, 635), fill=_rgba(palette["accent"], 85), width=3)
    draw.line((225, 400, 855, 400), fill=_rgba(palette["accent"], 85), width=3)
    moon_x, moon_y = rng.choice([(350, 270), (710, 285), (610, 235)])
    draw.ellipse((moon_x - 70, moon_y - 70, moon_x + 70, moon_y + 70), fill=_rgba((242, 229, 188), 220))
    draw.ellipse((moon_x - 35, moon_y - 82, moon_x + 95, moon_y + 58), fill=_rgba((16, 24, 46), 235))
    _draw_constellation(draw, rng, (280, 210, 805, 565), palette, 10)
    draw.rounded_rectangle((260, 730, 660, 930), radius=24, fill=_rgba(_blend_rgb(palette["ink"], (235, 210, 176), 0.55), 230), outline=_rgba(palette["accent"], 165), width=3)
    phases = ["○", "◔", "◐", "●", "◑", "◕"]
    for i, phase in enumerate(phases):
        draw.text((300 + i * 56, 770), phase, font=symbol_font, fill=_rgba(palette["bg"], 180))
        draw.line((300 + i * 56, 842, 330 + i * 56, 842), fill=_rgba(palette["mid"], 90), width=2)
    _draw_candle(draw, 790, 745, 0.82, palette, rng)


def _draw_moon_calendar_scene(draw, width: int, height: int, palette: dict, rng, symbol_font, small_font) -> None:
    draw.rounded_rectangle((190, 200, 890, 860), radius=34, fill=_rgba(_blend_rgb(palette["ink"], (238, 219, 184), 0.5), 232), outline=_rgba(palette["accent"], 185), width=3)
    for y in range(345, 785, 95):
        draw.line((260, y, 820, y), fill=_rgba(palette["mid"], 95), width=2)
    phases = ["○", "◔", "◐", "●", "◑", "◕", "○", "●"]
    for i, phase in enumerate(phases):
        x = 285 + (i % 4) * 138
        y = 245 + (i // 4) * 210
        draw.text((x, y), phase, font=symbol_font, fill=_rgba(palette["bg"], 188))
        draw.rounded_rectangle((x - 20, y + 82, x + 90, y + 112), radius=12, fill=_rgba(palette["accent"], 70))
    _draw_crystal_cluster(draw, 790, 890, 0.5, palette, rng)
    _draw_candle(draw, 195, 735, 0.72, palette, rng)


def _draw_number_journal_scene(draw, width: int, height: int, palette: dict, rng, symbol_font, small_font) -> None:
    page = _blend_rgb(palette["ink"], (240, 222, 188), 0.55)
    _draw_shadowed_round(draw, (170, 240, 910, 820), 28, _rgba(page, 235), _rgba(palette["accent"], 170), width=3, shadow=70)
    draw.line((540, 260, 540, 805), fill=_rgba(palette["mid"], 95), width=3)
    values = ["11:11", "22", "7", "3", "9", "12"]
    for i, value in enumerate(rng.sample(values, 4)):
        x = 250 + (i % 2) * 340
        y = 330 + (i // 2) * 190
        draw.text((x, y), value, font=symbol_font, fill=_rgba(palette["bg"], 180))
        draw.line((x, y + 82, x + 215, y + 82), fill=_rgba(palette["mid"], 105), width=2)
        draw.line((x, y + 128, x + 180, y + 128), fill=_rgba(palette["mid"], 80), width=2)
    draw.ellipse((715, 680, 875, 840), fill=_rgba(_adjust_rgb(palette["accent"], -0.05), 210), outline=_rgba(palette["ink"], 150), width=4)
    draw.ellipse((752, 717, 838, 803), outline=_rgba(palette["ink"], 145), width=3)
    draw.line((795, 760, 795, 728), fill=_rgba(palette["ink"], 150), width=3)
    draw.line((795, 760, 824, 780), fill=_rgba(palette["ink"], 150), width=3)


def _draw_pocket_watch_scene(draw, width: int, height: int, palette: dict, rng, symbol_font, small_font) -> None:
    _draw_number_journal_scene(draw, width, height, palette, rng, symbol_font, small_font)
    draw.arc((410, 110, 800, 500), 195, 350, fill=_rgba(palette["accent"], 145), width=5)
    for i in range(6):
        draw.ellipse((620 + i * 18, 120 + i * 9, 632 + i * 18, 132 + i * 9), outline=_rgba(palette["accent"], 145), width=2)


def _draw_crystal_altar_scene(draw, width: int, height: int, palette: dict, rng, symbol_font, small_font) -> None:
    draw.rounded_rectangle((195, 685, 885, 870), radius=36, fill=_rgba(_adjust_rgb(palette["bg"], 0.08), 210), outline=_rgba(palette["accent"], 130), width=3)
    _draw_crystal_cluster(draw, 540, 690, 1.25, palette, rng)
    _draw_candle(draw, 260, 575, 0.92, palette, rng)
    draw.ellipse((700, 610, 875, 765), fill=_rgba(_adjust_rgb(palette["mid"], 0.1), 220), outline=_rgba(palette["accent"], 150), width=3)
    draw.ellipse((725, 630, 850, 735), fill=_rgba(_blend_rgb(palette["ink"], palette["mid"], 0.32), 190))
    for i in range(5):
        _draw_small_star(draw, 340 + i * 95, 270 + rng.randint(-30, 45), rng.randint(14, 28), _rgba(palette["ink"], 145))


def _draw_four_elements_scene(draw, width: int, height: int, palette: dict, rng, symbol_font, small_font) -> None:
    spots = [(310, 385), (735, 385), (310, 740), (735, 740)]
    for x, y in spots:
        draw.ellipse((x - 145, y - 80, x + 145, y + 88), fill=(0, 0, 0, 52))
        draw.ellipse((x - 125, y - 75, x + 125, y + 75), fill=_rgba(_blend_rgb(palette["ink"], palette["mid"], 0.38), 220), outline=_rgba(palette["accent"], 160), width=3)
    _draw_candle(draw, 310, 325, 0.65, palette, rng)
    draw.arc((675, 325, 795, 445), 15, 165, fill=_rgba((105, 192, 219), 205), width=10)
    draw.arc((690, 350, 815, 475), 15, 165, fill=_rgba((105, 192, 219), 160), width=8)
    draw.polygon([(260, 730), (315, 650), (370, 730), (337, 805), (283, 805)], fill=_rgba(_adjust_rgb(palette["accent"], -0.05), 225), outline=_rgba(palette["ink"], 135))
    draw.line((682, 780, 790, 675), fill=_rgba(palette["ink"], 180), width=5)
    for i in range(5):
        draw.arc((650 + i * 18, 650 + i * 8, 790 + i * 16, 800 + i * 10), 205, 330, fill=_rgba(palette["ink"], 115), width=3)


def _draw_dream_journal_scene(draw, width: int, height: int, palette: dict, rng, symbol_font, small_font) -> None:
    _draw_moon_window_scene(draw, width, height, palette, rng, symbol_font, small_font)
    pillow = (200, 790, 480, 940)
    draw.rounded_rectangle(pillow, radius=42, fill=_rgba(_blend_rgb(palette["ink"], palette["mid"], 0.35), 225), outline=_rgba(palette["accent"], 120), width=2)
    draw.rounded_rectangle((510, 760, 840, 935), radius=28, fill=_rgba(_blend_rgb(palette["ink"], (238, 219, 184), 0.55), 232), outline=_rgba(palette["accent"], 160), width=3)
    draw.arc((610, 800, 720, 910), 75, 280, fill=_rgba(palette["bg"], 170), width=8)
    draw.line((560, 890, 790, 890), fill=_rgba(palette["mid"], 85), width=2)


def _draw_singing_bowl_scene(draw, width: int, height: int, palette: dict, rng, symbol_font, small_font) -> None:
    draw.ellipse((295, 665, 785, 805), fill=(0, 0, 0, 60))
    draw.ellipse((320, 530, 760, 790), fill=_rgba(_adjust_rgb(palette["accent"], -0.08), 230), outline=_rgba(palette["ink"], 140), width=4)
    draw.ellipse((370, 535, 710, 665), fill=_rgba(_blend_rgb(palette["ink"], palette["mid"], 0.3), 190), outline=_rgba(palette["ink"], 100), width=2)
    draw.rounded_rectangle((710, 475, 925, 500), radius=13, fill=_rgba(_adjust_rgb(palette["ink"], -0.05), 230), outline=_rgba(palette["accent"], 140), width=2)
    for i in range(4):
        x = 250 + i * 110
        draw.arc((x, 235 - i * 22, x + 210, 580 - i * 12), 210, 320, fill=_rgba(palette["ink"], 68), width=4)
    for i in range(18):
        angle = math.radians(i * 20)
        x = 540 + int(320 * math.cos(angle))
        y = 850 + int(72 * math.sin(angle))
        draw.ellipse((x - 12, y - 12, x + 12, y + 12), fill=_rgba(_adjust_rgb(palette["accent"], 0.08), 210))
    _draw_candle(draw, 210, 610, 0.8, palette, rng)


def _draw_thread_and_scales_scene(draw, width: int, height: int, palette: dict, rng, symbol_font, small_font) -> None:
    draw.line((540, 210, 540, 410), fill=_rgba(palette["ink"], 170), width=5)
    draw.polygon([(500, 410), (580, 410), (540, 360)], fill=_rgba(_adjust_rgb(palette["accent"], -0.04), 225), outline=_rgba(palette["ink"], 130))
    draw.line((335, 450, 745, 450), fill=_rgba(palette["ink"], 170), width=6)
    for x in (365, 715):
        draw.line((x, 450, x - 70, 620), fill=_rgba(palette["ink"], 135), width=3)
        draw.line((x, 450, x + 70, 620), fill=_rgba(palette["ink"], 135), width=3)
        draw.ellipse((x - 105, 605, x + 105, 710), fill=_rgba(_adjust_rgb(palette["accent"], -0.1), 218), outline=_rgba(palette["ink"], 125), width=3)
    draw.arc((220, 720, 880, 955), 190, 350, fill=(188, 47, 61, 205), width=8)
    draw.ellipse((210, 805, 350, 945), fill=_rgba(_adjust_rgb(palette["mid"], 0.08), 230), outline=_rgba(palette["accent"], 155), width=3)
    draw.ellipse((725, 805, 865, 945), fill=_rgba(_adjust_rgb(palette["mid"], 0.08), 230), outline=_rgba(palette["accent"], 155), width=3)
    _draw_constellation(draw, rng, (420, 140, 765, 285), palette, 6)


def _draw_brand_mark(draw, width: int, height: int, palette: dict, font) -> None:
    text = "Голос Звезд"
    bbox = draw.textbbox((0, 0), text, font=font)
    tw = bbox[2] - bbox[0]
    th = bbox[3] - bbox[1]
    x = width - tw - 58
    y = height - th - 48
    draw.rounded_rectangle((x - 22, y - 13, width - 34, height - 31), radius=18, fill=(0, 0, 0, 45), outline=_rgba(palette["accent"], 70), width=1)
    draw.text((x, y), text, font=font, fill=_rgba(palette["ink"], 170))


def _generate_local_channel_image_asset(
    topic_info: dict,
    author_info: dict | None = None,
    content_plan: dict | None = None,
) -> str:
    """Резервная локальная PNG-иллюстрация, если внешний генератор недоступен."""
    try:
        from PIL import Image, ImageDraw
    except Exception as e:
        print(f"[channel_image] Pillow unavailable: {e}")
        return ""

    try:
        os.makedirs(CHANNEL_IMAGE_ASSET_DIR, exist_ok=True)
        rng = random.Random(uuid.uuid4().hex)
        palette = rng.choice(CHANNEL_GENERATED_IMAGE_PALETTES)
        category = topic_info.get("category", "")
        topic_text = (topic_info.get("topic") or "").lower()
        scene_pool = list(_channel_image_scene_options(category))

        if author_info:
            if author_info.get("type") == "tarot":
                scene_pool.extend(["tarot_spread", "crystal_ball", "pendulum_map"])
            elif author_info.get("type") == "astro":
                scene_pool.extend(["natal_chart", "astrolabe", "moon_window"])

        keyword_scenes = [
            (("истори", "архив", "рукопис", "манускрипт", "суд", "закон"), ["pocket_watch", "number_journal", "pendulum_map"]),
            (("луна", "лунн", "сон", "сновид"), ["moon_window", "moon_calendar", "dream_journal"]),
            (("наталь", "зодиак", "планет", "астро"), ["natal_chart", "astrolabe"]),
            (("таро", "гадани", "расклад", "оракул"), ["tarot_spread", "crystal_ball", "pendulum_map"]),
            (("числ", "нумер", "матриц"), ["number_journal", "pocket_watch"]),
            (("кристалл", "камн", "минерал"), ["crystal_altar", "crystal_ball"]),
            (("стихи", "огонь", "вода", "земля", "воздух"), ["four_elements"]),
            (("медитац", "дыхани", "практик"), ["singing_bowl", "crystal_altar", "four_elements"]),
        ]
        for needles, scenes in keyword_scenes:
            if any(needle in topic_text for needle in needles):
                scene_pool.extend(scenes * 2)

        scene = rng.choice(scene_pool or CHANNEL_IMAGE_SCENES_BY_CATEGORY["default"])

        width = height = 1080
        img = Image.new("RGB", (width, height), palette["bg"])
        draw = ImageDraw.Draw(img, "RGBA")

        _draw_scene_background(draw, width, height, palette, rng)

        symbol_font = _channel_image_font(46, bold=True)
        small_font = _channel_image_font(30, bold=False)
        scene_drawers = {
            "tarot_spread": _draw_tarot_spread_scene,
            "crystal_ball": _draw_crystal_ball_scene,
            "pendulum_map": _draw_pendulum_map_scene,
            "natal_chart": _draw_natal_chart_scene,
            "astrolabe": _draw_astrolabe_scene,
            "moon_window": _draw_moon_window_scene,
            "moon_calendar": _draw_moon_calendar_scene,
            "number_journal": _draw_number_journal_scene,
            "pocket_watch": _draw_pocket_watch_scene,
            "crystal_altar": _draw_crystal_altar_scene,
            "four_elements": _draw_four_elements_scene,
            "dream_journal": _draw_dream_journal_scene,
            "singing_bowl": _draw_singing_bowl_scene,
            "thread_and_scales": _draw_thread_and_scales_scene,
        }
        scene_drawers.get(scene, _draw_crystal_ball_scene)(draw, width, height, palette, rng, symbol_font, small_font)

        draw.rounded_rectangle((42, 42, width - 42, height - 42), radius=46, outline=_rgba(palette["accent"], 100), width=2)
        _draw_brand_mark(draw, width, height, palette, small_font)

        path = os.path.join(CHANNEL_IMAGE_ASSET_DIR, f"channel_{uuid.uuid4().hex}.png")
        img.save(path, format="PNG", optimize=True)
        _cleanup_generated_channel_images()
        return path
    except Exception as e:
        print(f"[channel_image] generation error: {e}")
        return ""


def _channel_image_required(provider: str | None = None) -> bool:
    provider = (provider or CHANNEL_IMAGE_PROVIDER).strip().lower()
    return CHANNEL_REQUIRE_IMAGE and provider not in {"off", "none"}


def _channel_real_photo_required(provider: str | None = None) -> bool:
    provider = (provider or CHANNEL_IMAGE_PROVIDER).strip().lower()
    return CHANNEL_REQUIRE_REAL_PHOTO and provider not in {"off", "none"}


def _should_use_local_channel_image_fallback(provider: str | None = None) -> bool:
    provider = (provider or CHANNEL_IMAGE_PROVIDER).strip().lower()
    return (
        provider not in {"off", "none"}
        and not _channel_real_photo_required(provider)
        and (
            provider in {"local", "pillow"}
            or CHANNEL_ALLOW_LOCAL_IMAGE_FALLBACK
            or _channel_image_required(provider)
        )
    )


def _select_ai_image_scene(topic_info: dict, author_info: dict | None = None) -> str:
    rng = random.Random(uuid.uuid4().hex)
    category = topic_info.get("category", "")
    topic_text = (topic_info.get("topic") or "").lower()
    scene_pool = list(_channel_image_scene_options(category))

    if author_info:
        if author_info.get("type") == "tarot":
            scene_pool.extend(["tarot_spread", "crystal_ball", "pendulum_map", "tarot_deck_box"])
        elif author_info.get("type") == "astro":
            scene_pool.extend(["natal_chart", "astrolabe", "telescope_starmap", "moon_window"])

    keyword_scenes = [
        (("истори", "архив", "рукопис", "манускрипт", "суд", "закон"), ["pocket_watch", "number_journal", "pendulum_map"]),
        (("луна", "лунн", "сон", "сновид"), ["moon_window", "telescope_starmap", "dream_journal"]),
        (("наталь", "зодиак", "планет", "астро", "созвезд"), ["natal_chart", "astrolabe", "telescope_starmap"]),
        (("таро", "гадани", "расклад", "оракул", "карты"), ["tarot_spread", "tarot_deck_box", "crystal_ball", "pendulum_map"]),
        (("числ", "нумер", "матриц"), ["number_journal", "pocket_watch"]),
        (("кристалл", "камн", "минерал"), ["crystal_altar", "crystal_ball"]),
        (("стихи", "огонь", "вода", "земля", "воздух"), ["four_elements"]),
        (("медитац", "дыхани", "практик"), ["singing_bowl", "incense_altar", "crystal_altar"]),
        (("карма", "выбор", "решени"), ["thread_and_scales", "pendulum_map", "pocket_watch"]),
    ]
    for needles, scenes in keyword_scenes:
        if any(needle in topic_text for needle in needles):
            scene_pool.extend(scenes * 3)
    return rng.choice(scene_pool or CHANNEL_IMAGE_SCENES_BY_CATEGORY["default"])


def _build_ai_channel_image_prompt(
    scene: str,
    topic_info: dict,
    author_info: dict | None = None,
    content_plan: dict | None = None,
    visual_brief: dict | None = None,
) -> str:
    rng = random.Random(uuid.uuid4().hex)
    scene_variants = {
        "tarot_spread": [
            "three worn tarot cards spread on a dark walnut table, a velvet cloth underneath, a beeswax candle with melted wax, a small quartz point, an old deck box half open",
            "a Celtic-cross style tarot spread on linen fabric, brass candle holder, smoky quartz, dried lavender, a real wooden table with scratches and warm shadows",
            "two tarot cards face up and one card turned over, a black ceramic cup, candlelight, small moonstone, folded cloth, close tabletop still life",
        ],
        "tarot_deck_box": [
            "an old tarot deck box open on a wooden table, several illustrated cards sliding out, wax seal, candle stump, linen pouch and tiny brass key",
            "a compact tarot deck tied with a ribbon, one card peeking out, a ceramic candle holder, quartz crystal and worn notebook beside it",
        ],
        "crystal_ball": [
            "a real crystal ball on a brass stand, reflections of a crescent moon in the glass, tarot cards and a candle around it, dark wooden table",
            "glass divination sphere on velvet cloth, tiny bubbles and reflections inside the glass, pendulum chain nearby, candle glow and quartz pieces",
        ],
        "pendulum_map": [
            "a brass pendulum hanging over a hand drawn star map on parchment, constellation dots, candle wax, compass, dark table, soft dramatic light",
            "silver pendulum resting on an old astrological chart, chain curled naturally, small stones, fountain pen, realistic paper texture",
        ],
        "natal_chart": [
            "a printed natal chart with zodiac wheel on cream paper, brass compass, fountain pen, candle, coffee cup, real desk surface, astrology workspace",
            "an astrologer's desk with a natal chart sheet, ephemeris book, magnifying glass, brass ruler, small candle, textured paper and shadows",
        ],
        "astrolabe": [
            "a brass astrolabe lying on dark fabric, star map partially underneath, small telescope lens, candlelight, engraved metal texture",
            "antique astrolabe and compass on a wooden desk, folded constellation map, warm lamp light, tiny scratches on brass",
        ],
        "telescope_starmap": [
            "small brass telescope on a windowsill, open constellation map on the table, night sky visible through the window, a few stars and moonlight",
            "folded star atlas beside a compact telescope, ceramic mug, pencil, moonlight across a real wooden desk, quiet night atmosphere",
        ],
        "moon_window": [
            "crescent moon visible through a window, moonlight falling onto a table with a candle, tarot deck, silver pendant and open notebook",
            "night window with moonlight, a real desk holding a small candle, moonstone, folded paper star map and a cup of tea",
        ],
        "moon_calendar": [
            "open notebook with hand drawn moon phases, not a poster, the notebook lies on a wooden table with a candle, pen, moonstone and fabric bookmark",
            "paper lunar calendar in a real notebook, pen marks and smudges, candle, crystal, warm desk light, close still life",
        ],
        "number_journal": [
            "open numerology notebook with handwritten numbers and dates, fountain pen, old pocket watch, candle, textured paper, real tabletop",
            "small journal page filled with a few numerology notes, brass ruler, watch, coffee cup, candle glow, realistic paper and ink",
        ],
        "pocket_watch": [
            "antique pocket watch beside an open notebook and tarot card, brass chain, candlelight, wooden table, tactile realistic still life",
            "old pocket watch on dark cloth, handwritten numbers on paper, fountain pen, small crystal, warm shadows",
        ],
        "crystal_altar": [
            "small crystal altar on a wooden shelf, amethyst cluster, clear quartz, candle, ceramic bowl, dried herbs, realistic objects and soft light",
            "cluster of crystals arranged on linen cloth with a candle, tiny dish of salt, brass spoon, dried flowers, tangible tabletop scene",
        ],
        "four_elements": [
            "four small ritual bowls on a table: a candle flame, water bowl, stone with moss, feather, arranged naturally on linen cloth",
            "earth water fire and air represented by real objects: candle, glass bowl of water, smooth stone, feather, wooden table, soft shadows",
        ],
        "dream_journal": [
            "dream journal open beside a bed window, moonlight, pencil, lavender, small crystal, cup of tea, quiet realistic night scene",
            "notebook on bedside table with a crescent moon charm, candle, pillow edge, moonlit window, soft painterly realism",
        ],
        "singing_bowl": [
            "bronze singing bowl on a folded cloth, wooden striker, candle, mala beads, incense smoke, real tabletop, warm shadows",
            "meditation setup with singing bowl, beads, incense holder, candle and small stone, close still life with tactile textures",
        ],
        "thread_and_scales": [
            "small brass balance scales on a desk, red thread, old letter, candle, tarot card corner, realistic symbolic still life",
            "two brass scales with a red thread crossing the table, notebook, candle, small stone, moody realistic arrangement",
        ],
        "incense_altar": [
            "ceramic incense holder with thin smoke, candle, smooth stones, dried herbs and small notebook on dark wooden table",
            "incense stick burning near a candle, crystal, folded cloth, brass tray, warm atmospheric tabletop still life",
        ],
    }

    scene_text = rng.choice(scene_variants.get(scene) or scene_variants["crystal_ball"])
    style = rng.choice([
        "semi realistic editorial illustration, painterly but grounded, real objects with believable perspective",
        "soft realistic digital painting, tactile materials, natural shadows, not photorealistic but physically believable",
        "warm cinematic still life, slightly stylized realism, detailed textures, real tabletop composition",
        "minimal painterly realism, clear real objects, calm mystical mood, no flat icon style",
    ])
    lighting = rng.choice([
        "warm candlelight and subtle moonlight",
        "soft side light, deep but gentle shadows",
        "quiet evening light, amber and blue contrast",
        "low lamp light with small highlights on glass and metal",
    ])
    camera = rng.choice([
        "square 1:1 composition, close view, object focused",
        "square composition, 50mm still life view, shallow depth of field",
        "square crop, slightly top down tabletop angle, balanced negative space",
        "square image, intimate desk scene, clear main subject",
    ])
    topic_hint = (topic_info.get("topic") or "").replace("\n", " ")[:180]
    visual = (topic_info.get("schedule") or {}).get("visual") or {}
    image_mood = str(visual.get("image_mood") or "").strip()
    brief_scene = str((visual_brief or {}).get("scene_ru") or "").strip()
    brief_queries = ", ".join((visual_brief or {}).get("queries", [])[:4])
    brief_required = ", ".join(
        "/".join(group)
        for group in (visual_brief or {}).get("required_groups", [])[:4]
    )
    brief_hint = ""
    if brief_scene or brief_queries or brief_required:
        brief_hint = (
            f"Concrete visual brief: {brief_scene}. "
            f"Search scene keywords: {brief_queries}. "
            f"Must visibly include: {brief_required}. "
        )
    role_hint = ""
    if author_info:
        role_hint = "tarot reader atmosphere" if author_info.get("type") == "tarot" else "astrologer workspace atmosphere"

    return (
        f"{scene_text}. {style}. {lighting}. {camera}. "
        f"{brief_hint}Theme hint: {topic_hint}. Visual mood: {image_mood}. {role_hint}. "
        "No text, no readable words, no letters, no logo, no watermark, no poster, no infographic, no list layout, "
        "no UI card, no flat vector icons, no abstract geometric symbols, no decorative template border."
    ).strip()


async def _generate_pollinations_channel_image(prompt: str) -> str:
    if CHANNEL_IMAGE_PROVIDER in {"local", "pillow", "off", "none"}:
        return ""

    try:
        os.makedirs(CHANNEL_IMAGE_ASSET_DIR, exist_ok=True)
        encoded_prompt = quote(prompt, safe="")
        url = f"https://gen.pollinations.ai/image/{encoded_prompt}"
        params = {
            "width": "1080",
            "height": "1080",
            "model": POLLINATIONS_IMAGE_MODEL,
            "seed": str(random.randint(1, 2_147_483_647)),
            "nologo": "true",
            "safe": "true",
            "private": "true",
        }
        headers = {"Accept": "image/jpeg,image/png,image/webp"}
        if POLLINATIONS_API_KEY:
            headers["Authorization"] = f"Bearer {POLLINATIONS_API_KEY}"

        timeout = aiohttp.ClientTimeout(total=POLLINATIONS_IMAGE_TIMEOUT)
        async with aiohttp.ClientSession(timeout=timeout) as image_session:
            async with image_session.get(url, params=params, headers=headers) as response:
                content_type = response.headers.get("Content-Type", "").lower()
                data = await response.read()
                if response.status != 200:
                    preview = data[:220].decode("utf-8", errors="ignore")
                    print(f"[channel_image] Pollinations status {response.status}: {preview}")
                    return ""
                if not content_type.startswith("image/") or len(data) < 10_000:
                    preview = data[:220].decode("utf-8", errors="ignore")
                    print(f"[channel_image] Pollinations returned non-image: {content_type} {preview}")
                    return ""

        return _store_telegram_channel_photo(data, "pollinations")
    except Exception as e:
        print(f"[channel_image] Pollinations generation error: {e}")
        return ""


async def generate_channel_image_asset(
    topic_info: dict,
    author_info: dict | None = None,
    content_plan: dict | None = None,
    post_text: str = "",
) -> str:
    """Pick a real stock/open image first; AI/local rendering is only a fallback."""
    provider = CHANNEL_IMAGE_PROVIDER.strip().lower()
    if _channel_real_photo_required(provider) and provider in {"local", "pillow", "pollinations", "ai"}:
        provider = "stock"
    if provider in {"pollinations", "ai"} and not CHANNEL_ALLOW_AI_IMAGE_FALLBACK:
        provider = "stock"
    if provider in {"off", "none"}:
        return ""
    visual_brief = await _build_channel_image_brief(topic_info, content_plan, post_text)
    if provider in {"local", "pillow"}:
        return _generate_local_channel_image_asset(topic_info, author_info, content_plan)

    if provider not in {"local", "pillow", "off", "none", "pollinations", "ai"}:
        image_path = await _generate_stock_channel_image_asset(
            topic_info,
            author_info,
            content_plan,
            provider,
            post_text,
            visual_brief,
        )
        if image_path:
            return image_path

    if (
        CHANNEL_ALLOW_AI_IMAGE_FALLBACK
        and not _channel_real_photo_required(provider)
        and provider in {"pollinations", "ai", "all"}
    ):
        scene = _select_ai_image_scene(topic_info, author_info)
        prompt = _build_ai_channel_image_prompt(scene, topic_info, author_info, content_plan, visual_brief)
        image_path = await _generate_pollinations_channel_image(prompt)
        if image_path:
            return image_path

    image_path = await _generate_emergency_stock_channel_image_asset()
    if image_path:
        return image_path

    if _should_use_local_channel_image_fallback(provider):
        print("[channel_image] external image unavailable, using local fallback")
        return _generate_local_channel_image_asset(topic_info, author_info, content_plan)
    return ""


def _topic_key(topic_info: dict) -> str:
    return (topic_info.get("topic") or "").strip()


def _parse_channel_slot_minutes(time_text: str) -> int:
    hour, minute = (int(part) for part in time_text.split(":", 1))
    return hour * 60 + minute


def _channel_schedule_slot_key(msk: datetime, slot: dict) -> str:
    return f"{msk.date().isoformat()}:{slot.get('id', '')}"


def _select_due_channel_schedule_slot(msk: datetime) -> dict | None:
    state = load_channel_state()
    posted_keys = {
        key for key in state.get("posted_schedule_slots", []) or []
        if isinstance(key, str) and key.strip()
    }
    current_minute = msk.hour * 60 + msk.minute
    slots = CHANNEL_WEEKLY_POST_SCHEDULE.get(msk.weekday(), [])
    for slot in slots:
        try:
            target_minute = _parse_channel_slot_minutes(slot["time"])
        except Exception:
            continue
        if current_minute < target_minute:
            continue
        if current_minute > target_minute + CHANNEL_SCHEDULE_GRACE_MINUTES:
            continue

        slot_key = _channel_schedule_slot_key(msk, slot)
        if slot_key in posted_keys:
            continue

        due_slot = dict(slot)
        due_slot["slot_key"] = slot_key
        due_slot["weekday"] = msk.weekday()
        return due_slot
    return None


def next_channel_schedule_slot_after(msk: datetime | None = None) -> dict | None:
    msk = msk or _msk_now()
    for day_offset in range(8):
        target_date = msk.date() + timedelta(days=day_offset)
        weekday = (msk.weekday() + day_offset) % 7
        slots = sorted(
            CHANNEL_WEEKLY_POST_SCHEDULE.get(weekday, []),
            key=lambda item: item.get("time", "99:99"),
        )
        for slot in slots:
            try:
                hour, minute = (int(part) for part in slot["time"].split(":", 1))
            except Exception:
                continue
            target_at = datetime.combine(target_date, datetime.min.time()).replace(hour=hour, minute=minute)
            if target_at > msk:
                return {"at": target_at, "slot": slot}
    return None


def _remember_channel_schedule_slot(slot_key: str) -> None:
    if not slot_key:
        return
    state = load_channel_state()
    posted = [
        key for key in state.get("posted_schedule_slots", []) or []
        if isinstance(key, str) and key.strip()
    ]
    if slot_key in posted:
        posted.remove(slot_key)
    posted.append(slot_key)
    state["posted_schedule_slots"] = posted[-60:]
    save_channel_state(state)


CHANNEL_WEEKLY_INTERACTIVE_SETS = {
    "tarot_card_choice": [
        {
            "id": "moon_temperance_strength",
            "pick": "Сделай интерактив: предложи выбрать карту 1, 2 или 3. Карты задай явно: 1 - Луна, 2 - Умеренность, 3 - Сила. Не раскрывай значения, не зови в бота, только создай атмосферу и попроси сохранить выбранный номер до завтрашней расшифровки.",
            "decode": "Расшифруй вчерашний интерактив с картами: 1 - Луна, 2 - Умеренность, 3 - Сила. Для каждой карты дай 2-3 живых предложения: что заметить в себе сегодня и какой маленький шаг сделать. Не продавай бота.",
        },
        {
            "id": "hermit_star_justice",
            "pick": "Сделай интерактив: предложи выбрать карту 1, 2 или 3. Карты задай явно: 1 - Отшельник, 2 - Звезда, 3 - Справедливость. Не раскрывай значения, не зови в бота, только создай атмосферу и попроси сохранить выбранный номер до завтрашней расшифровки.",
            "decode": "Расшифруй вчерашний интерактив с картами: 1 - Отшельник, 2 - Звезда, 3 - Справедливость. Для каждой карты дай 2-3 живых предложения: что заметить в себе сегодня и какой маленький шаг сделать. Не продавай бота.",
        },
        {
            "id": "magician_priestess_chariot",
            "pick": "Сделай интерактив: предложи выбрать карту 1, 2 или 3. Карты задай явно: 1 - Маг, 2 - Верховная Жрица, 3 - Колесница. Не раскрывай значения, не зови в бота, только создай атмосферу и попроси сохранить выбранный номер до завтрашней расшифровки.",
            "decode": "Расшифруй вчерашний интерактив с картами: 1 - Маг, 2 - Верховная Жрица, 3 - Колесница. Для каждой карты дай 2-3 живых предложения: что заметить в себе сегодня и какой маленький шаг сделать. Не продавай бота.",
        },
        {
            "id": "empress_hanged_sun",
            "pick": "Сделай интерактив: предложи выбрать карту 1, 2 или 3. Карты задай явно: 1 - Императрица, 2 - Повешенный, 3 - Солнце. Не раскрывай значения, не зови в бота, только создай атмосферу и попроси сохранить выбранный номер до завтрашней расшифровки.",
            "decode": "Расшифруй вчерашний интерактив с картами: 1 - Императрица, 2 - Повешенный, 3 - Солнце. Для каждой карты дай 2-3 живых предложения: что заметить в себе сегодня и какой маленький шаг сделать. Не продавай бота.",
        },
        {
            "id": "lovers_wheel_tower",
            "pick": "Сделай интерактив: предложи выбрать карту 1, 2 или 3. Карты задай явно: 1 - Влюбленные, 2 - Колесо Фортуны, 3 - Башня. Не раскрывай значения, не зови в бота, только создай атмосферу и попроси сохранить выбранный номер до завтрашней расшифровки.",
            "decode": "Расшифруй вчерашний интерактив с картами: 1 - Влюбленные, 2 - Колесо Фортуны, 3 - Башня. Для каждой карты дай 2-3 живых предложения: что заметить в себе сегодня и какой маленький шаг сделать. Не продавай бота.",
        },
    ],
    "phrase_choice": [
        {
            "id": "waiting_mistake_answer",
            "pick": "Сделай интерактив: предложи выбрать фразу, которая сильнее откликается. Варианты: 1 - 'я устала ждать', 2 - 'я боюсь ошибиться', 3 - 'я уже знаю ответ'. Не раскрывай значения до завтрашней расшифровки.",
            "decode": "Расшифруй вчерашний выбор фраз: 1 - 'я устала ждать', 2 - 'я боюсь ошибиться', 3 - 'я уже знаю ответ'. Для каждой фразы дай короткий психологично-эзотерический смысл и маленькое действие на день.",
        },
        {
            "id": "hold_past_need_clarity",
            "pick": "Сделай интерактив: предложи выбрать фразу, которая сильнее откликается. Варианты: 1 - 'я держусь за прошлое', 2 - 'мне нужна ясность', 3 - 'я устала решать за двоих'. Не раскрывай значения до завтрашней расшифровки.",
            "decode": "Расшифруй вчерашний выбор фраз: 1 - 'я держусь за прошлое', 2 - 'мне нужна ясность', 3 - 'я устала решать за двоих'. Для каждой фразы дай короткий психологично-эзотерический смысл и маленькое действие на день.",
        },
        {
            "id": "quiet_step_boundary",
            "pick": "Сделай интерактив: предложи выбрать фразу, которая сильнее откликается. Варианты: 1 - 'мне нужна пауза', 2 - 'я хочу сделать шаг', 3 - 'мне пора поставить границу'. Не раскрывай значения до завтрашней расшифровки.",
            "decode": "Расшифруй вчерашний выбор фраз: 1 - 'мне нужна пауза', 2 - 'я хочу сделать шаг', 3 - 'мне пора поставить границу'. Для каждой фразы дай короткий психологично-эзотерический смысл и маленькое действие на день.",
        },
        {
            "id": "trust_stop_choose",
            "pick": "Сделай интерактив: предложи выбрать фразу, которая сильнее откликается. Варианты: 1 - 'я могу себе доверять', 2 - 'пора остановиться', 3 - 'я выбираю себя'. Не раскрывай значения до завтрашней расшифровки.",
            "decode": "Расшифруй вчерашний выбор фраз: 1 - 'я могу себе доверять', 2 - 'пора остановиться', 3 - 'я выбираю себя'. Для каждой фразы дай короткий психологично-эзотерический смысл и маленькое действие на день.",
        },
    ],
}

CHANNEL_SCHEDULE_INTERACTIVE_GROUPS = {
    "day1_evening_pick_card": ("tarot_card_choice", "pick"),
    "day2_evening_card_decode": ("tarot_card_choice", "decode"),
    "day5_evening_pick_phrase": ("phrase_choice", "pick"),
    "day6_morning_phrase_decode": ("phrase_choice", "decode"),
}


def _channel_week_key_from_slot(slot: dict, group_id: str) -> str:
    slot_key = str(slot.get("slot_key") or "")
    date_text = slot_key.split(":", 1)[0] if ":" in slot_key else ""
    try:
        slot_date = datetime.fromisoformat(date_text).date()
    except Exception:
        slot_date = _msk_now().date()
    year, week, _weekday = slot_date.isocalendar()
    return f"{year}-W{week:02d}:{group_id}"


def _select_channel_interactive_variant(slot: dict) -> dict | None:
    group_info = CHANNEL_SCHEDULE_INTERACTIVE_GROUPS.get(slot.get("id", ""))
    if not group_info:
        return None
    group_id, mode = group_info
    variants = CHANNEL_WEEKLY_INTERACTIVE_SETS.get(group_id) or []
    if not variants:
        return None

    state = load_channel_state()
    selections = state.get("weekly_interactive_variants")
    if not isinstance(selections, dict):
        selections = {}
    recent_key = f"recent_interactive_variants:{group_id}"
    recent = [
        str(item)
        for item in state.get(recent_key, []) or []
        if str(item).strip()
    ][-max(1, len(variants) - 1):]

    week_key = _channel_week_key_from_slot(slot, group_id)
    selected_id = selections.get(week_key)
    selected = next((item for item in variants if item.get("id") == selected_id), None)
    if not selected:
        fresh = [item for item in variants if item.get("id") not in recent]
        rng = random.Random(week_key)
        selected = rng.choice(fresh or variants)
        selections[week_key] = selected["id"]
        if selected["id"] in recent:
            recent.remove(selected["id"])
        recent.append(selected["id"])
        state["weekly_interactive_variants"] = {
            key: value
            for key, value in selections.items()
            if isinstance(key, str) and isinstance(value, str)
        }
        state[recent_key] = recent[-max(1, len(variants) - 1):]
        save_channel_state(state)

    return {
        "id": selected.get("id", ""),
        "topic": selected.get(mode, ""),
        "group": group_id,
        "mode": mode,
    }


def _channel_topic_from_schedule_slot(slot: dict) -> dict:
    style = slot.get("style") or CHANNEL_SCHEDULE_STYLE_BY_ID.get(slot.get("id", ""), "")
    visual = slot.get("visual") or CHANNEL_SCHEDULE_VISUAL_BY_ID.get(slot.get("id", ""), {})
    selected_topic = {
        "category": slot.get("category", "mystic"),
        "topic": slot.get("topic", ""),
    }
    topic_pool = [
        item for item in (slot.get("topic_pool") or [])
        if isinstance(item, dict) and (item.get("topic") or "").strip()
    ]
    if topic_pool:
        _sync_recent_topics_from_state()
        fresh_topics = [
            item for item in topic_pool
            if _topic_key(item) not in RECENT_TOPIC_KEYS
        ]
        rng = random.Random(slot.get("slot_key") or slot.get("id", "channel_schedule"))
        selected_topic = rng.choice(fresh_topics or topic_pool)

    variant = _select_channel_interactive_variant(slot)
    if variant and variant.get("topic"):
        selected_topic = {
            "category": slot.get("category", selected_topic.get("category", "mystic")),
            "topic": variant["topic"],
        }

    return {
        "category": selected_topic.get("category", slot.get("category", "mystic")),
        "topic": selected_topic.get("topic", slot.get("topic", "")),
        "promo": bool(slot.get("promo", False)),
        "schedule": {
            "id": slot.get("id", ""),
            "time": slot.get("time", ""),
            "rubric": slot.get("rubric", ""),
            "style": style,
            "visual": visual,
            "slot_key": slot.get("slot_key", ""),
            "weekday": slot.get("weekday"),
            "variant_id": (variant or {}).get("id", ""),
            "variant_group": (variant or {}).get("group", ""),
        },
    }


def _sync_recent_topics_from_state() -> None:
    RECENT_TOPIC_KEYS.clear()
    for key in load_channel_state().get("recent_topics", []) or []:
        if isinstance(key, str) and key.strip():
            RECENT_TOPIC_KEYS.append(key.strip())
    while len(RECENT_TOPIC_KEYS) > MAX_RECENT_TOPICS:
        RECENT_TOPIC_KEYS.pop(0)


def _remember_channel_topic(topic_info: dict) -> None:
    key = _topic_key(topic_info)
    if not key:
        return
    if key in RECENT_TOPIC_KEYS:
        RECENT_TOPIC_KEYS.remove(key)
    RECENT_TOPIC_KEYS.append(key)
    while len(RECENT_TOPIC_KEYS) > MAX_RECENT_TOPICS:
        RECENT_TOPIC_KEYS.pop(0)

    state = load_channel_state()
    state["recent_topics"] = RECENT_TOPIC_KEYS
    save_channel_state(state)


def _sync_recent_content_from_state() -> None:
    state = load_channel_state()
    RECENT_CONTENT_SIGNATURES.clear()
    for signature in state.get("recent_content_signatures", []) or []:
        if isinstance(signature, str) and signature.strip():
            RECENT_CONTENT_SIGNATURES.append(signature.strip())
    while len(RECENT_CONTENT_SIGNATURES) > MAX_RECENT_CONTENT_SIGNATURES:
        RECENT_CONTENT_SIGNATURES.pop(0)

    RECENT_CHANNEL_POST_SAMPLES.clear()
    for sample in state.get("recent_post_samples", []) or []:
        if isinstance(sample, str) and sample.strip():
            RECENT_CHANNEL_POST_SAMPLES.append(sample.strip())
    while len(RECENT_CHANNEL_POST_SAMPLES) > MAX_RECENT_CHANNEL_POST_SAMPLES:
        RECENT_CHANNEL_POST_SAMPLES.pop(0)


def _recent_content_parts() -> dict[str, set[str]]:
    parts = {
        "rubric": set(),
        "format": set(),
        "tone": set(),
        "hook": set(),
        "layout": set(),
        "ending": set(),
    }
    for signature in RECENT_CONTENT_SIGNATURES:
        for chunk in signature.split("|"):
            if ":" not in chunk:
                continue
            key, value = chunk.split(":", 1)
            if key in parts and value:
                parts[key].add(value)
    return parts


def _pick_fresh_channel_variant(options: list[dict], part: str, recent_parts: dict[str, set[str]]) -> dict:
    if not options:
        return {}
    fresh = [item for item in options if item.get("id") not in recent_parts.get(part, set())]
    return random.choice(fresh or options)


def _channel_rubrics_for(author_info: dict | None) -> list[dict]:
    if not author_info:
        return CHANNEL_UNIVERSAL_RUBRICS
    if author_info.get("type") == "tarot":
        return CHANNEL_TAROT_RUBRICS + CHANNEL_UNIVERSAL_RUBRICS
    if author_info.get("type") == "astro":
        return CHANNEL_ASTRO_RUBRICS + CHANNEL_UNIVERSAL_RUBRICS
    return CHANNEL_UNIVERSAL_RUBRICS


def _select_channel_content_plan(topic_info: dict, author_info: dict | None) -> dict:
    _sync_recent_content_from_state()
    recent_parts = _recent_content_parts()
    plan = {
        "rubric": _pick_fresh_channel_variant(_channel_rubrics_for(author_info), "rubric", recent_parts),
        "format": _pick_fresh_channel_variant(CHANNEL_POST_FORMATS, "format", recent_parts),
        "tone": _pick_fresh_channel_variant(CHANNEL_POST_TONES, "tone", recent_parts),
        "hook": _pick_fresh_channel_variant(CHANNEL_POST_HOOKS, "hook", recent_parts),
        "layout": _pick_fresh_channel_variant(CHANNEL_TEXT_LAYOUTS, "layout", recent_parts),
        "ending": _pick_fresh_channel_variant(CHANNEL_POST_ENDINGS, "ending", recent_parts),
        "category": topic_info.get("category", ""),
        "promo": bool(topic_info.get("promo", False)),
        "schedule": topic_info.get("schedule") or {},
    }
    return plan


def _content_signature(content_plan: dict | None) -> str:
    if not content_plan:
        return ""
    parts = []
    schedule = content_plan.get("schedule") or {}
    schedule_id = schedule.get("id") if isinstance(schedule, dict) else ""
    schedule_variant = schedule.get("variant_id") if isinstance(schedule, dict) else ""
    if schedule_id:
        parts.append(f"schedule:{schedule_id}")
    if schedule_variant:
        parts.append(f"variant:{schedule_variant}")
    for key in ("rubric", "format", "tone", "hook", "layout", "ending"):
        value = content_plan.get(key) or {}
        value_id = value.get("id") if isinstance(value, dict) else ""
        if value_id:
            parts.append(f"{key}:{value_id}")
    return "|".join(parts)


def _channel_post_sample(text: str) -> str:
    text = re.sub(r'</?[a-zA-Z][a-zA-Z0-9\-]*(?:\s[^>]*)?>', '', text or "")
    text = re.sub(r"\s+", " ", text).strip()
    return text[:220]


def _remember_channel_content(
    content_plan: dict | None,
    text: str,
    topic_info: dict | None = None,
) -> None:
    _sync_recent_content_from_state()
    signature = _content_signature(content_plan)
    if signature:
        if signature in RECENT_CONTENT_SIGNATURES:
            RECENT_CONTENT_SIGNATURES.remove(signature)
        RECENT_CONTENT_SIGNATURES.append(signature)
        while len(RECENT_CONTENT_SIGNATURES) > MAX_RECENT_CONTENT_SIGNATURES:
            RECENT_CONTENT_SIGNATURES.pop(0)

    sample = _channel_post_sample(text)
    if sample:
        if sample in RECENT_CHANNEL_POST_SAMPLES:
            RECENT_CHANNEL_POST_SAMPLES.remove(sample)
        RECENT_CHANNEL_POST_SAMPLES.append(sample)
        while len(RECENT_CHANNEL_POST_SAMPLES) > MAX_RECENT_CHANNEL_POST_SAMPLES:
            RECENT_CHANNEL_POST_SAMPLES.pop(0)

    state = load_channel_state()
    state["recent_content_signatures"] = RECENT_CONTENT_SIGNATURES
    state["recent_post_samples"] = RECENT_CHANNEL_POST_SAMPLES

    schedule = (content_plan or {}).get("schedule") or (topic_info or {}).get("schedule") or {}
    records = _channel_trim_post_records(state.get("recent_post_records", []) or [])
    plain = _plain_channel_post_text(text)
    if plain:
        record = {
            "at": _msk_now().isoformat(),
            "sample": plain[:360],
            "tokens": _channel_text_signature_tokens(plain, limit=24),
            "topic_key": _topic_key(topic_info or {}),
            "category": (topic_info or {}).get("category") or (content_plan or {}).get("category") or "",
            "schedule_id": schedule.get("id", "") if isinstance(schedule, dict) else "",
            "schedule_rubric": schedule.get("rubric", "") if isinstance(schedule, dict) else "",
            "schedule_variant": schedule.get("variant_id", "") if isinstance(schedule, dict) else "",
            "content_signature": signature,
        }
        records.append(record)
        state["recent_post_records"] = _channel_trim_post_records(records)
    save_channel_state(state)


def _select_channel_topic() -> dict:
    scheduled_slot = _select_due_channel_schedule_slot(_msk_now())
    if scheduled_slot:
        return _channel_topic_from_schedule_slot(scheduled_slot)

    _sync_recent_topics_from_state()
    fresh_topics = [
        topic_info for topic_info in CHANNEL_POST_TOPICS
        if _topic_key(topic_info) not in RECENT_TOPIC_KEYS
    ]
    if fresh_topics:
        return random.choice(fresh_topics)

    # If every topic is in recent history, allow the oldest one back into rotation.
    oldest_key = RECENT_TOPIC_KEYS[0] if RECENT_TOPIC_KEYS else ""
    fallback_topics = [
        topic_info for topic_info in CHANNEL_POST_TOPICS
        if _topic_key(topic_info) == oldest_key
    ]
    return random.choice(fallback_topics or CHANNEL_POST_TOPICS)


def clean_markdown(text: str) -> str:
    """Конвертирует простую markdown-разметку в Telegram HTML и заменяет длинные тире на дефис."""
    text = re.sub(r'#{1,6}\s*', '', text)            # ### заголовки
    text = re.sub(r'\|\|(.+?)\|\|', r'<tg-spoiler>\1</tg-spoiler>', text)  # ||спойлер||
    text = re.sub(r'\*\*(.+?)\*\*', r'<b>\1</b>', text)     # **жирный**
    text = re.sub(r'__(.+?)__', r'<b>\1</b>', text)         # __жирный__
    text = re.sub(r'\*(.+?)\*', r'<i>\1</i>', text)         # *курсив*
    text = re.sub(r'_(.+?)_', r'<i>\1</i>', text)           # _курсив_
    text = re.sub(r'`(.+?)`', r'\1', text)           # `код`
    text = re.sub(r'~~(.+?)~~', r'<s>\1</s>', text)  # ~~зачёркнутый~~
    text = re.sub(r'\[(.+?)\]\(.+?\)', r'\1', text)  # [ссылка](url)
    text = re.sub(r'^[-•]\s', '- ', text, flags=re.MULTILINE)
    # Длинные и средние тире на обычный дефис (анти-ИИ правка)
    text = text.replace('—', '-').replace('–', '-')
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


# Разрешённые теги Telegram HTML (которые мы хотим видеть в постах)
CHANNEL_AI_META_BAN_PROMPT = (
    "\u041d\u0435 \u0434\u043e\u0431\u0430\u0432\u043b\u044f\u0439 "
    "\u0441\u043b\u0443\u0436\u0435\u0431\u043d\u044b\u0435 "
    "\u0444\u0440\u0430\u0437\u044b \u043e \u0442\u043e\u043c, "
    "\u0447\u0442\u043e \u0442\u0435\u043a\u0441\u0442 "
    "\u0441\u0433\u0435\u043d\u0435\u0440\u0438\u0440\u043e\u0432\u0430\u043d "
    "\u0438\u043b\u0438 \u0433\u043e\u0442\u043e\u0432: "
    "'\u0412\u043e\u0442, \u0447\u0442\u043e \u0443 \u043c\u0435\u043d\u044f "
    "\u043f\u043e\u043b\u0443\u0447\u0438\u043b\u043e\u0441\u044c:', "
    "'\u0412\u043e\u0442, \u0447\u0442\u043e \u0443 \u043c\u043d\u0435\u044f "
    "\u043f\u043e\u043b\u0443\u0447\u0438\u043b\u043e\u0441\u044c:', "
    "'\u0413\u043e\u0442\u043e\u0432\u044b\u0439 \u043f\u043e\u0441\u0442:', "
    "'\u0412\u0430\u0440\u0438\u0430\u043d\u0442 \u043f\u043e\u0441\u0442\u0430:'. "
    "\u0421\u0440\u0430\u0437\u0443 \u043d\u0430\u0447\u0438\u043d\u0430\u0439 "
    "\u0441 \u0441\u0430\u043c\u043e\u0433\u043e \u043f\u043e\u0441\u0442\u0430."
)

_CHANNEL_AI_META_PHRASES = (
    "\u0432\u043e\u0442, \u0447\u0442\u043e \u0443 \u043c\u0435\u043d\u044f \u043f\u043e\u043b\u0443\u0447\u0438\u043b\u043e\u0441\u044c",
    "\u0432\u043e\u0442 \u0447\u0442\u043e \u0443 \u043c\u0435\u043d\u044f \u043f\u043e\u043b\u0443\u0447\u0438\u043b\u043e\u0441\u044c",
    "\u0432\u043e\u0442, \u0447\u0442\u043e \u0443 \u043c\u043d\u0435\u044f \u043f\u043e\u043b\u0443\u0447\u0438\u043b\u043e\u0441\u044c",
    "\u0432\u043e\u0442 \u0447\u0442\u043e \u0443 \u043c\u043d\u0435\u044f \u043f\u043e\u043b\u0443\u0447\u0438\u043b\u043e\u0441\u044c",
    "\u0432\u043e\u0442, \u0447\u0442\u043e \u043f\u043e\u043b\u0443\u0447\u0438\u043b\u043e\u0441\u044c",
    "\u0432\u043e\u0442 \u0447\u0442\u043e \u043f\u043e\u043b\u0443\u0447\u0438\u043b\u043e\u0441\u044c",
    "\u0432\u043e\u0442 \u043f\u043e\u0441\u0442",
    "\u0432\u043e\u0442 \u0433\u043e\u0442\u043e\u0432\u044b\u0439 \u043f\u043e\u0441\u0442",
    "\u0432\u043e\u0442 \u0432\u0430\u0440\u0438\u0430\u043d\u0442 \u043f\u043e\u0441\u0442\u0430",
    "\u0432\u043e\u0442 \u0442\u0435\u043a\u0441\u0442 \u043f\u043e\u0441\u0442\u0430",
    "\u0433\u043e\u0442\u043e\u0432\u044b\u0439 \u043f\u043e\u0441\u0442",
    "\u0432\u0430\u0440\u0438\u0430\u043d\u0442 \u043f\u043e\u0441\u0442\u0430",
    "\u0442\u0435\u043a\u0441\u0442 \u043f\u043e\u0441\u0442\u0430",
    "\u043f\u043e\u0441\u0442 \u0434\u043b\u044f \u043a\u0430\u043d\u0430\u043b\u0430",
    "\u043f\u043e\u0441\u0442 \u0433\u043e\u0442\u043e\u0432",
    "\u043f\u043e\u043b\u0443\u0447\u0438\u043b\u0441\u044f \u0442\u0430\u043a\u043e\u0439 \u043f\u043e\u0441\u0442",
    "\u043f\u043e\u043b\u0443\u0447\u0438\u043b\u0441\u044f \u0432\u043e\u0442 \u0442\u0430\u043a\u043e\u0439 \u043f\u043e\u0441\u0442",
    "\u043a\u043e\u043d\u0435\u0447\u043d\u043e, \u0432\u043e\u0442 \u0447\u0442\u043e \u0443 \u043c\u0435\u043d\u044f \u043f\u043e\u043b\u0443\u0447\u0438\u043b\u043e\u0441\u044c",
    "\u043a\u043e\u043d\u0435\u0447\u043d\u043e \u0432\u043e\u0442 \u0447\u0442\u043e \u0443 \u043c\u0435\u043d\u044f \u043f\u043e\u043b\u0443\u0447\u0438\u043b\u043e\u0441\u044c",
    "\u043a\u043e\u043d\u0435\u0447\u043d\u043e, \u0432\u043e\u0442 \u043f\u043e\u0441\u0442",
    "\u043a\u043e\u043d\u0435\u0447\u043d\u043e \u0432\u043e\u0442 \u043f\u043e\u0441\u0442",
    "\u043a\u043e\u043d\u0435\u0447\u043d\u043e, \u0433\u043e\u0442\u043e\u0432\u044b\u0439 \u043f\u043e\u0441\u0442",
    "\u043a\u043e\u043d\u0435\u0447\u043d\u043e \u0433\u043e\u0442\u043e\u0432\u044b\u0439 \u043f\u043e\u0441\u0442",
)
_CHANNEL_HTML_OPEN_TAG_RE = r"<[a-zA-Z][a-zA-Z0-9\-]*(?:\s[^>]*)?>"
_CHANNEL_HTML_CLOSE_TAG_RE = r"</[a-zA-Z][a-zA-Z0-9\-]*\s*>"
_CHANNEL_HTML_TAG_RE = r"(?:" + _CHANNEL_HTML_OPEN_TAG_RE + r"|" + _CHANNEL_HTML_CLOSE_TAG_RE + r")"
_CHANNEL_AI_META_PATTERN = "|".join(re.escape(phrase) for phrase in _CHANNEL_AI_META_PHRASES)
_CHANNEL_AI_META_PREFIX_RE = re.compile(
    r"^\s*(?:" + _CHANNEL_HTML_TAG_RE + r"\s*)*(?:"
    + _CHANNEL_AI_META_PATTERN
    + r")[^\w<]*(?:" + _CHANNEL_HTML_CLOSE_TAG_RE + r"\s*)*",
    flags=re.IGNORECASE,
)
_CHANNEL_AI_META_SUFFIX_RE = re.compile(
    r"(?:" + _CHANNEL_HTML_OPEN_TAG_RE + r"\s*)*(?:"
    + _CHANNEL_AI_META_PATTERN
    + r")[^\w<]*(?:" + _CHANNEL_HTML_CLOSE_TAG_RE + r"\s*)*$",
    flags=re.IGNORECASE,
)


def strip_channel_ai_meta_wrappers(text: str) -> str:
    """Remove model wrapper phrases from the start or end of a channel post."""
    text = (text or "").strip()
    for _ in range(4):
        if not text:
            return ""
        lines = text.splitlines()
        while lines and not lines[0].strip():
            lines.pop(0)
        while lines and not lines[-1].strip():
            lines.pop()
        if not lines:
            return ""

        first = lines[0]
        cleaned_first = _CHANNEL_AI_META_PREFIX_RE.sub("", first, count=1).lstrip()
        if cleaned_first != first:
            if cleaned_first:
                lines[0] = cleaned_first
            else:
                lines.pop(0)
            text = "\n".join(lines).strip()
            continue

        last = lines[-1]
        cleaned_last = _CHANNEL_AI_META_SUFFIX_RE.sub("", last, count=1).rstrip()
        if cleaned_last != last:
            if cleaned_last:
                lines[-1] = cleaned_last
            else:
                lines.pop()
            text = "\n".join(lines).strip()
            continue

        break
    return text


_ALLOWED_HTML_TAGS = {"a", "b", "i", "u", "s", "tg-spoiler"}
TELEGRAM_PHOTO_CAPTION_LIMIT = 1024
CHANNEL_TAROT_AUTHOR_CATEGORIES = {"tarot", "divination", "numerology"}
CHANNEL_ASTRO_AUTHOR_CATEGORIES = {
    "astrology", "zodiac", "moon", "planets", "elements", "karma", "dreams", "crystals"
}
CHANNEL_CATEGORY_AUTHOR_IDS = {
    "numerology": {"tarot": {"vadim"}},
    "karma": {"astro": {"georgiy"}},
}
CHANNEL_BOT_PROMO_INTROS = [
    "Если хочется понять, как это проявляется именно в твоей ситуации, можно прийти за личным разбором.\n\n",
    "Иногда общий знак только намекает, а личная консультация показывает, где именно сейчас точка выбора.\n\n",
    "А если откликнулось и хочется посмотреть глубже, можно разобрать свой вопрос лично.\n\n",
    "Когда тема цепляет не случайно, лучше смотреть её не в общем прогнозе, а через твою ситуацию.\n\n",
    "Если чувствуешь, что это про тебя, можно задать вопрос и получить более точный разбор.\n\n",
    "Общий пост даёт направление, а личный расклад или астрологический разбор помогает увидеть детали.\n\n",
]

CHANNEL_BOT_PROMO_OFFERS = [
    {
        "id": "compact_services",
        "text": (
            "<b>Голос Звёзд</b>: ежедневные прогнозы, совместимость и личные консультации тарологов и астрологов.\n"
            "Первый сеанс после регистрации бесплатный.\n"
            "Бот: {bot_label}"
        ),
    },
    {
        "id": "question_route",
        "text": (
            "В боте <b>Голос Звёзд</b> можно задать свой вопрос тарологу или астрологу и получить личный разбор.\n"
            "Консультация специалиста: {sale_price_label}, первый сеанс бесплатный.\n"
            "Бот: {bot_label}"
        ),
    },
    {
        "id": "astro_tarot_menu",
        "text": (
            "<b>Голос Звёзд</b> внутри Telegram: прогноз по знаку, совместимость, описания знаков, Таро и астрологи.\n"
            "Бот: {bot_label}"
        ),
    },
    {
        "id": "short_personal",
        "text": (
            "Хочешь посмотреть тему лично? В <b>Голосе Звёзд</b> есть тарологи и астрологи для точного разбора.\n"
            "Бот: {bot_label}"
        ),
    },
    {
        "id": "soft_invite",
        "text": (
            "<b>Голос Звёзд</b> помогает смотреть глубже: прогнозы, совместимость и личные консультации специалистов.\n"
            "Бот: {bot_label}"
        ),
    },
]

CHANNEL_AUTHOR_PUBLIC_VOICES = {
    "maya": "теплая, наблюдательная, сильна в отношениях и семейных сценариях, может сказать прямо, но без грубости",
    "boris": "собранный и деловой, любит короткую конкретику, карьерные и финансовые примеры, без лишней романтики",
    "alina": "живая, энергичная, работает с Таро Тота, замечает неожиданные детали и говорит честно",
    "vadim": "неторопливый, связывает Таро с нумерологией, видит несколько слоев и любит бытовые совпадения",
    "svetlana": "зрелая, весомая, марсельская традиция, говорит спокойно и с достоинством",
    "dasha": "молодая, быстрая, искренняя, хорошо чувствует современные ситуации и не прячет прямоту",
    "timur": "образный, восточная традиция, иногда мыслит притчей, но умеет быстро вернуться к сути",
    "vera": "мягкая, духовная, старомодная, говорит бережно и не торопит читателя",
    "inna": "строгая западная астрология, транзиты, прогрессии, аспекты, дома, точность важнее утешений",
    "georgiy": "ведическая астрология, джйотиш, карма, даши, накшатры, спокойная глубина без суеты",
    "kira": "психологическая астрология, синастрии, живой тон, быстро видит эмоциональный паттерн",
    "stanislav": "мунданная и деловая астрология, сухая конкретика, сроки, циклы, практичный вывод",
    "zhanna": "хорарная астрология, точность момента вопроса, загадочность, Луна и дома как главные маркеры",
}

CHANNEL_UNIVERSAL_RUBRICS = [
    {"id": "symbol_detail", "label": "символическая деталь", "instruction": "раскрой один символ или образ, без энциклопедии, через живое наблюдение"},
    {"id": "daily_energy", "label": "энергия дня", "instruction": "дай ощущение текущей энергии и один практичный способ прожить ее мягче"},
    {"id": "small_practice", "label": "мини-практика", "instruction": "предложи короткое действие на 2-5 минут, без обещаний чудес"},
    {"id": "myth_reframe", "label": "миф и реальность", "instruction": "разбери популярное заблуждение и покажи более тонкий взгляд"},
    {"id": "reader_mirror", "label": "зеркало читателя", "instruction": "свяжи тему с узнаваемой бытовой ситуацией читателя"},
    {"id": "soft_warning", "label": "мягкое предупреждение", "instruction": "аккуратно покажи риск и сразу дай экологичный выход"},
]

CHANNEL_TAROT_RUBRICS = [
    {"id": "tarot_card_lens", "label": "карта как линза", "instruction": "выбери одну карту или аркан и смотри на тему через ее образ"},
    {"id": "spread_fragment", "label": "фрагмент расклада", "instruction": "пиши так, будто видишь один фрагмент расклада и объясняешь его смысл"},
    {"id": "shadow_card", "label": "теневая сторона карты", "instruction": "покажи не только красивое значение, но и где человек сам себе мешает"},
    {"id": "choice_advice", "label": "совет на выбор", "instruction": "сфокусируйся на моменте выбора, что карта просит заметить перед решением"},
    {"id": "relationship_signal", "label": "сигнал в отношениях", "instruction": "сделай акцент на чувствах, границах или честном разговоре, если тема позволяет"},
]

CHANNEL_ASTRO_RUBRICS = [
    {"id": "planet_focus", "label": "фокус планеты", "instruction": "выбери планету, аспект, дом или знак и объясни, как это проявляется в жизни"},
    {"id": "transit_mood", "label": "транзитное настроение", "instruction": "пиши как астролог, который видит фон периода и дает осторожный совет"},
    {"id": "zodiac_behavior", "label": "знак в быту", "instruction": "покажи, как астрологическая тема проявляется в поведении, общении или выборе"},
    {"id": "synastry_hint", "label": "синастрический намек", "instruction": "если тема связана с людьми, добавь взгляд на совместимость или динамику пары"},
    {"id": "moon_rhythm", "label": "лунный ритм", "instruction": "свяжи тему с эмоциональным ритмом, телесностью или сменой внутреннего состояния"},
]

CHANNEL_POST_FORMATS = [
    {"id": "micro_story", "label": "мини-история", "instruction": "2 абзаца: сначала маленькая сцена или наблюдение, затем смысл и вопрос. Не начинай с термина"},
    {"id": "question_then_answer", "label": "вопрос и ответ", "instruction": "начни с короткого вопроса отдельной строкой, потом дай ответ в 3-4 предложениях"},
    {"id": "warning_then_care", "label": "предупреждение и опора", "instruction": "1 абзац про риск, 1 абзац про мягкий выход. Без морализаторства"},
    {"id": "one_detail", "label": "одна деталь", "instruction": "один плотный абзац вокруг одной детали. Не перечисляй несколько признаков"},
    {"id": "inner_dialogue", "label": "внутренний диалог", "instruction": "пиши как короткий разговор с читателем: вопрос, ответ, честное уточнение. Без театральности"},
    {"id": "note_from_practice", "label": "заметка из практики", "instruction": "начни с 'В практике часто видно...' или близкой по смыслу фразы, но не раскрывай чужие истории"},
    {"id": "contrast", "label": "контраст", "instruction": "построй текст на противопоставлении: как кажется снаружи и что на самом деле показывает тема"},
    {"id": "micro_ritual", "label": "микро-ритуал", "instruction": "дай одно короткое действие на сегодня, затем объясни его символический смысл и задай вопрос"},
    {"id": "myth_break", "label": "разбор мифа", "instruction": "начни с фразы 'Есть миф, что...', затем мягко разверни более точный взгляд"},
]

CHANNEL_POST_TONES = [
    {"id": "warm", "label": "теплый", "instruction": "поддерживающий, без сахара и пустых обещаний"},
    {"id": "precise", "label": "точный", "instruction": "профессиональный, с одним термином по делу и ясным выводом"},
    {"id": "mystic", "label": "мистический", "instruction": "атмосферный, но не туманный, символы должны вести к смыслу"},
    {"id": "direct", "label": "прямой", "instruction": "честный и немного острый, но без давления на читателя"},
    {"id": "practical", "label": "практичный", "instruction": "земной, с маленьким действием, которое можно сделать сегодня"},
    {"id": "diary", "label": "дневниковый", "instruction": "личное наблюдение, будто короткая запись после консультаций"},
    {"id": "light_irony", "label": "легкая ирония", "instruction": "чуть улыбающийся тон, но тема остается уважительной"},
]

CHANNEL_POST_HOOKS = [
    {"id": "sensory", "label": "сенсорная деталь", "instruction": "начни с предмета, жеста, ощущения или маленькой сцены"},
    {"id": "unexpected", "label": "неожиданный поворот", "instruction": "начни с мысли, которая чуть спорит с привычным взглядом"},
    {"id": "reader_state", "label": "состояние читателя", "instruction": "начни с узнаваемого состояния: усталость, ожидание, сомнение, резкий импульс"},
    {"id": "specialist_observation", "label": "наблюдение специалиста", "instruction": "начни с фразы о том, что автор часто замечает в практике"},
    {"id": "quiet_question", "label": "тихий вопрос", "instruction": "начни с короткого вопроса, который не звучит как кликбейт"},
]

CHANNEL_TEXT_LAYOUTS = [
    {"id": "icon_title", "label": "иконка и заголовок", "instruction": "первая строка: 1 эмодзи и короткий <b>заголовок</b>. Потом 2 коротких абзаца"},
    {"id": "quote_open", "label": "курсивная затравка", "instruction": "начни с отдельной строки <i>короткая атмосферная фраза</i>, затем основной текст"},
    {"id": "signal_blocks", "label": "сигнальные блоки", "instruction": "сделай 2 мини-блока через пустую строку, каждый начинается разным эмодзи по смыслу"},
    {"id": "spoiler_middle", "label": "спойлер в середине", "instruction": "поставь <tg-spoiler>...</tg-spoiler> отдельной короткой строкой в середине поста"},
    {"id": "ritual_card", "label": "карточка действия", "instruction": "сначала эмодзи и действие на сегодня, потом смысл, потом вопрос"},
    {"id": "soft_divider", "label": "мягкий разделитель", "instruction": "используй одну строку-разделитель из символов ✦ ✧ ✦ между двумя смысловыми частями"},
]

CHANNEL_POST_ENDINGS = [
    {"id": "reader_question", "label": "вопрос читателю", "instruction": "заверши одним коротким вопросом, который хочется обдумать"},
    {"id": "micro_action", "label": "микродействие", "instruction": "заверши маленьким действием на сегодня и затем коротким вопросом"},
    {"id": "choice_point", "label": "точка выбора", "instruction": "заверши мыслью о выборе и вопросом к читателю"},
    {"id": "soft_reflection", "label": "мягкое отражение", "instruction": "заверши спокойной фразой и вопросом без давления"},
]

CHANNEL_POST_QUALITY_RULES = (
    "Качество важнее оригинальности любой ценой: не придумывай сомнительные факты, не обещай гарантированных событий, "
    "не запугивай, не делай медицинских, юридических или финансовых советов. "
    "Пост должен быть цельным: одна тема, один главный вывод, один вопрос в конце."
)


def _select_channel_promo_offer(short: bool = False) -> str:
    state = load_channel_state()
    recent = [
        key for key in state.get("recent_promo_keys", []) or []
        if isinstance(key, str) and key.strip()
    ]
    while len(recent) > MAX_RECENT_PROMOS:
        recent.pop(0)

    bot_label = f"@{MAIN_BOT_USERNAME}" if MAIN_BOT_USERNAME else "бот"
    offers = CHANNEL_BOT_PROMO_OFFERS
    if short:
        offers = [offer for offer in offers if offer["id"] in {"short_personal", "soft_invite", "astro_tarot_menu"}]
    fresh = [offer for offer in offers if offer["id"] not in recent]
    offer = random.choice(fresh or offers)

    key = offer["id"]
    if key in recent:
        recent.remove(key)
    recent.append(key)
    while len(recent) > MAX_RECENT_PROMOS:
        recent.pop(0)
    state["recent_promo_keys"] = recent
    save_channel_state(state)
    return offer["text"].format(
        bot_label=bot_label,
        sale_price_label=_ckassa_sale_amount_text(),
    )


def _channel_bot_promo_offer_short() -> str:
    return _select_channel_promo_offer(short=True)


def _channel_bot_promo_offer() -> str:
    return _select_channel_promo_offer()


def _specialist_gender(specialist: dict) -> str:
    personality = (specialist.get("personality") or "").lower()
    if "мужчина" in personality:
        return "male"
    if "женщина" in personality:
        return "female"
    return "female" if str(specialist.get("name", "")).endswith(("а", "я")) else "male"


def _select_channel_author(topic_info: dict) -> dict | None:
    category = topic_info.get("category", "")
    category_author_ids = CHANNEL_CATEGORY_AUTHOR_IDS.get(category, {})
    if category in CHANNEL_TAROT_AUTHOR_CATEGORIES and TAROLOGISTS:
        allowed_ids = category_author_ids.get("tarot")
        candidates = [t for t in TAROLOGISTS if not allowed_ids or t.get("id") in allowed_ids]
        specialist = random.choice(candidates or TAROLOGISTS)
        return {"type": "tarot", "specialist": specialist, "gender": _specialist_gender(specialist)}
    if category in CHANNEL_ASTRO_AUTHOR_CATEGORIES and ASTROLOGERS:
        allowed_ids = category_author_ids.get("astro")
        candidates = [a for a in ASTROLOGERS if not allowed_ids or a.get("id") in allowed_ids]
        specialist = random.choice(candidates or ASTROLOGERS)
        return {"type": "astro", "specialist": specialist, "gender": _specialist_gender(specialist)}
    return None


def _channel_author_prompt(author_info: dict | None, content_plan: dict | None = None) -> str:
    if not author_info:
        return (
            "Автор поста: администратор канала. Пиши нейтрально от лица админа, как раньше, "
            "без подписи специалиста в тексте.\n"
        )

    specialist = author_info["specialist"]
    is_tarot = author_info["type"] == "tarot"
    specialist_label = "таролог" if is_tarot else "астролог"
    topic_focus = "тарологии и карт Таро" if is_tarot else "астрологии, планет, домов, аспектов или знаков зодиака"
    public_voice = CHANNEL_AUTHOR_PUBLIC_VOICES.get(specialist.get("id", ""), "")
    tone_label = ((content_plan or {}).get("tone") or {}).get("label", "")
    gender_rule = (
        "Пиши от лица мужчины и используй формы мужского рода: заметил, видел, понял, советовал."
        if author_info["gender"] == "male"
        else "Пиши от лица женщины и используй формы женского рода: замечала, видела, поняла, советовала."
    )
    voice_rule = (
        f"Публичная манера автора: {public_voice}. " if public_voice else ""
    )
    tone_rule = (
        f"Подстрой этот голос под тон выпуска: {tone_label}. " if tone_label else ""
    )
    return (
        f"Автор поста: {specialist['name']}, {specialist_label} из базы бота. "
        f"{gender_rule} {voice_rule}{tone_rule}Тематика поста должна быть строго о {topic_focus}. "
        "Это публичный пост канала, поэтому стиль чище, чем личная переписка: без намеренных ошибок, без грубости, без хаоса, с ясной мыслью. "
        "Не называй автора в самом тексте и не добавляй подпись: подпись будет добавлена автоматически.\n"
    )


def _channel_author_signature(author_info: dict | None) -> str:
    if not author_info:
        return ""
    specialist = author_info["specialist"]
    role = "тарологом" if author_info["type"] == "tarot" else "астрологом"
    name = specialist["name"]
    payload = _specialist_start_payload(author_info["type"], specialist["id"])
    return f'Пост написан {role} <a href="{_build_main_bot_deeplink(payload)}">{name}</a>.'


def sanitize_html_for_telegram(text: str) -> str:
    """Оставляет только разрешённые Telegram HTML-теги. Удаляет несбалансированные теги,
    чтобы Telegram не отклонил сообщение."""
    # Удаляем любые теги, кроме разрешённых
    def _strip_disallowed(m: re.Match) -> str:
        tag = m.group(1).lower()
        raw = m.group(0)
        if tag not in _ALLOWED_HTML_TAGS:
            return ""
        if tag == "a":
            if raw.startswith("</"):
                return "</a>"
            href = re.search(r'href=(["\'])(https://t\.me/[A-Za-z0-9_/?=&-]+)\1', raw)
            return f'<a href="{href.group(2)}">' if href else ""
        return f"</{tag}>" if raw.startswith("</") else f"<{tag}>"
    text = re.sub(r'</?([a-zA-Z][a-zA-Z0-9\-]*)(?:\s[^>]*)?>', _strip_disallowed, text)
    # Балансировка: если число открытий != числу закрытий, удаляем этот тег
    for tag in _ALLOWED_HTML_TAGS:
        tag_pattern = re.escape(tag)
        opens = len(re.findall(rf'<{tag_pattern}(?:\s[^>]*)?>', text, flags=re.IGNORECASE))
        closes = len(re.findall(rf'</{tag_pattern}>', text, flags=re.IGNORECASE))
        if opens != closes:
            text = re.sub(rf'</?{tag_pattern}(?:\s[^>]*)?>', '', text, flags=re.IGNORECASE)
    return text


def with_channel_bot_promo(text: str, final_suffix: str = "") -> str:
    """Добавляет к посту плавный переход к основному боту и сохраняет лимит подписи Telegram."""
    text = text.strip()
    final_suffix = final_suffix.strip()
    final_block = f"\n\n{final_suffix}" if final_suffix else ""
    bot_mentions = {"@VoiceOfTheStarsBot"}
    if MAIN_BOT_USERNAME:
        bot_mentions.add(f"@{MAIN_BOT_USERNAME}")
    if any(mention in text for mention in bot_mentions):
        full_text = f"{text}{final_block}"
        if len(full_text) <= TELEGRAM_PHOTO_CAPTION_LIMIT:
            return full_text
        available = TELEGRAM_PHOTO_CAPTION_LIMIT - len(final_block) - 3
        if available <= 0:
            return final_suffix[:TELEGRAM_PHOTO_CAPTION_LIMIT]
        trimmed = text[:available].rstrip()
        trimmed = re.sub(r'</?([a-zA-Z][a-zA-Z0-9\-]*)(?:\s[^>]*)?$', '', trimmed).rstrip()
        trimmed = sanitize_html_for_telegram(trimmed)
        return f"{trimmed}...{final_block}"

    promo = random.choice(CHANNEL_BOT_PROMO_INTROS) + _channel_bot_promo_offer()
    suffix = "\n\n" + promo + final_block
    if len(suffix) >= TELEGRAM_PHOTO_CAPTION_LIMIT:
        suffix = final_block
    full_text = f"{text}{suffix}"
    if len(full_text) <= TELEGRAM_PHOTO_CAPTION_LIMIT:
        return full_text

    compact_suffix = "\n\n" + _channel_bot_promo_offer_short() + final_block
    if len(compact_suffix) < len(suffix):
        compact_full_text = f"{text}{compact_suffix}"
        if len(compact_full_text) <= TELEGRAM_PHOTO_CAPTION_LIMIT:
            return compact_full_text
        suffix = compact_suffix

    signature_only = f"{text}{final_block}"
    if final_block and len(signature_only) <= TELEGRAM_PHOTO_CAPTION_LIMIT:
        return signature_only

    available = TELEGRAM_PHOTO_CAPTION_LIMIT - len(suffix) - 3
    if available <= 0:
        fallback = suffix.strip()
        if len(fallback) <= TELEGRAM_PHOTO_CAPTION_LIMIT:
            return fallback
        return final_suffix[:TELEGRAM_PHOTO_CAPTION_LIMIT] if final_suffix else fallback[:TELEGRAM_PHOTO_CAPTION_LIMIT]

    trimmed = text[:available].rstrip()
    trimmed = re.sub(r'</?([a-zA-Z][a-zA-Z0-9\-]*)(?:\s[^>]*)?$', '', trimmed).rstrip()
    trimmed = sanitize_html_for_telegram(trimmed)
    return f"{trimmed}...{suffix}"


def with_channel_final_suffix(text: str, final_suffix: str = "") -> str:
    """Добавляет только подпись автора, без промо бота."""
    text = text.strip()
    final_suffix = final_suffix.strip()
    if not final_suffix:
        return text

    final_block = f"\n\n{final_suffix}"
    full_text = f"{text}{final_block}"
    if len(full_text) <= TELEGRAM_PHOTO_CAPTION_LIMIT:
        return full_text

    available = TELEGRAM_PHOTO_CAPTION_LIMIT - len(final_block) - 3
    if available <= 0:
        return final_suffix[:TELEGRAM_PHOTO_CAPTION_LIMIT]

    trimmed = text[:available].rstrip()
    trimmed = re.sub(r'</?([a-zA-Z][a-zA-Z0-9\-]*)(?:\s[^>]*)?$', '', trimmed).rstrip()
    trimmed = sanitize_html_for_telegram(trimmed)
    return f"{trimmed}...{final_block}"


def _channel_content_plan_prompt(content_plan: dict | None) -> str:
    if not content_plan:
        return ""

    def _line(title: str, item: dict) -> str:
        label = item.get("label", "")
        instruction = item.get("instruction", "")
        return f"{title}: {label}. {instruction}"

    schedule = content_plan.get("schedule") or {}
    schedule_block = ""
    if schedule:
        visual = schedule.get("visual") or {}
        visual_lines = ""
        if visual:
            visual_lines = (
                "Визуальный профиль поста:\n"
                f"- Размер: {visual.get('size', '')}\n"
                f"- Абзацы: {visual.get('paragraphs', '')}\n"
                f"- Жирный, курсив и блюр: {visual.get('formatting', '')}\n"
                f"- Эмодзи: {visual.get('emoji', '')}\n"
                f"- Воздух и раскладка: {visual.get('layout', '')}\n"
                f"- Настроение картинки: {visual.get('image_mood', '')}\n"
                "Этот визуальный профиль важнее случайно выбранной раскладки ниже, если они конфликтуют.\n"
            )
        schedule_block = (
            "РЕДАКЦИОННЫЙ СЛОТ:\n"
            f"Рубрика: {schedule.get('rubric', '')}. Время: {schedule.get('time', '')} МСК.\n"
            f"Уникальный дизайн этого поста: {schedule.get('style', '')}\n"
            f"{visual_lines}"
            "Пиши ровно под эту рубрику и не называй ее служебно, если это не выглядит естественным заголовком.\n"
        )
    promo_block = (
        "CTA В БОТА: в этом посте можно мягко пригласить в @VoiceOfTheStarsBot, без давления и без повторения старых продажных формулировок.\n"
        if content_plan.get("promo")
        else "CTA В БОТА: не добавляй продажу, ссылку на бота, цену, бесплатный сеанс или призыв к консультации в основной текст.\n"
    )

    return (
        "КОНТЕНТ-ПЛАН ЭТОГО ПОСТА:\n"
        f"{schedule_block}"
        f"{promo_block}"
        f"{_line('Рубрика', content_plan.get('rubric') or {})}\n"
        f"{_line('Форма', content_plan.get('format') or {})}\n"
        f"{_line('Тон', content_plan.get('tone') or {})}\n"
        f"{_line('Заход', content_plan.get('hook') or {})}\n"
        f"{_line('Визуальная раскладка текста', content_plan.get('layout') or {})}\n"
        f"{_line('Финал', content_plan.get('ending') or {})}\n"
        "Форма важнее привычного шаблона: сделай пост визуально и ритмически отличимым от обычной схемы 'наблюдение, совет, вопрос'. "
        "Следуй этому плану, но не называй рубрику, форму или тон в самом посте.\n"
    )


def _recent_channel_posts_prompt() -> str:
    _sync_recent_content_from_state()
    records = _channel_recent_post_records()
    samples = [
        str(record.get("sample") or "").strip()
        for record in records[-12:]
        if str(record.get("sample") or "").strip()
    ]
    if not samples and RECENT_CHANNEL_POST_SAMPLES:
        samples = RECENT_CHANNEL_POST_SAMPLES[-12:]
    if not samples:
        return ""

    lines = "\n".join(f"{idx + 1}. {sample[:220]}" for idx, sample in enumerate(samples))
    recent_slots = []
    seen_slots = set()
    for record in reversed(records):
        slot = str(record.get("schedule_rubric") or record.get("schedule_id") or "").strip()
        if not slot or slot in seen_slots:
            continue
        seen_slots.add(slot)
        recent_slots.append(slot)
        if len(recent_slots) >= 10:
            break
    slot_line = ""
    if recent_slots:
        slot_line = "Недавно уже звучали рубрики/мотивы: " + ", ".join(recent_slots) + ".\n"
    return (
        "ПАМЯТЬ КАНАЛА:\n"
        f"Последние посты начинались или звучали примерно так:\n{lines}\n"
        f"{slot_line}"
        "Не повторяй их начало, главную метафору, бытовую сцену, набор вариантов, структуру и финальный вопрос. "
        "Если слот похож на старый, меняй конкретику: карту, предмет, место действия, конфликт, действие дня и образ картинки. "
        "Разнообразие нужно через новый ракурс, а не через ухудшение смысла.\n"
    )


async def generate_channel_post(
    topic: str,
    author_info: dict | None = None,
    content_plan: dict | None = None,
) -> str:
    """Генерирует пост для канала через ИИ по заданной теме."""
    if content_plan is None:
        content_plan = _select_channel_content_plan({"category": "", "topic": topic}, author_info)
    author_prompt = _channel_author_prompt(author_info, content_plan)
    plan_prompt = _channel_content_plan_prompt(content_plan)
    recent_posts_prompt = _recent_channel_posts_prompt()
    prompt = (
        f"ТЕМА ПОСТА:\n{topic}\n\n"
        f"{plan_prompt}\n"
        f"{author_prompt}\n"
        f"{recent_posts_prompt}\n"
        f"ОБЩЕЕ ПРАВИЛО КАЧЕСТВА:\n{CHANNEL_POST_QUALITY_RULES}\n\n"
        "СТРОГИЕ ТРЕБОВАНИЯ К ПОСТУ:\n"
        "1. Длина 350-850 символов. Чередуй короткие посты и средние, но не превращай текст в статью.\n"
        "2. Пиши голосом выбранного автора, но не застревай в одном шаблоне от первого лица. Можно писать от 'я', можно напрямую к читателю, можно как заметку из практики, если это соответствует форме.\n"
        "3. Начни с детали, наблюдения или вопроса. ЗАПРЕЩЕНЫ начала: 'Итак', 'Давайте', 'В этом посте', 'Знаете ли вы', 'Сегодня поговорим', 'Интересный факт', 'Погрузимся'.\n"
        "4. Язык русский, живой разговорный. Чередуй длину предложений, иногда короткое обрывочное. Лёгкая субъективность приветствуется.\n"
        "5. Добавь 0-2 эмодзи по смыслу. Если текст держится без эмодзи, не добавляй их силой.\n"
        "6. В конце один короткий вопрос читателям.\n"
        "7. Визуальная раскладка обязательна: в первую очередь следуй визуальному профилю слота, затем выбранному layout. Используй пустые строки, длину абзацев и эмодзи так, чтобы пост отличался при беглом пролистывании.\n"
        "8. Не повторяй типовой ритм прошлых постов: если обычно получается 'символ значит X, совет Y, вопрос Z', выбери другую форму из контент-плана.\n\n"
        "ФОРМАТИРОВАНИЕ (только Telegram HTML-теги, никакого markdown):\n"
        "- Оформление зависит от визуального профиля слота. Если профиль просит жирный, курсив или блюр, используй их. Если профиль просит без курсива или без блюра, не добавляй их просто ради украшения.\n"
        "- <b>...</b>: выдели 1-3 самых важных слова или термина. Это должны быть ОСМЫСЛЕННЫЕ выделения: название карты Таро, имя планеты, ключевое понятие, суть совета. НЕ выделяй случайные слова, союзы, предлоги.\n"
        "- <i>...</i>: используй 1-2 раза для атмосферной фразы, метафоры или короткого внутреннего наблюдения.\n"
        "- <tg-spoiler>...</tg-spoiler>: используй только если он действительно усиливает интригу интерактива или выбора, максимум один раз.\n"
        "- Не ставь теги вокруг целых абзацев. Оформление должно помогать читать, а не выглядеть как случайная разметка.\n\n"
        "ЗАПРЕЩЕНО:\n"
        "- Хештеги, ссылки, упоминания аккаунтов.\n"
        "- Markdown-разметка (**, ##, *, _, `, ~~). Только HTML-теги выше.\n"
        "- ЛЮБЫЕ тире: ни длинное '—', ни среднее '–'. Вместо тире используй запятую, двоеточие, скобки или разбей на два предложения.\n"
        "- Нумерованные и маркированные списки запрещены, кроме слотов выбора или расшифровки, где редакционный дизайн прямо просит строки 1, 2, 3. В таких слотах используй только короткие строки с номерами 1, 2, 3, без длинных списков.\n"
        "- Канцеляризмы и штампы ИИ: 'важно отметить', 'стоит упомянуть', 'давайте разберёмся', 'таким образом', 'в заключение', 'помните:', 'и напоследок', 'погрузимся', 'не секрет, что', 'как известно'.\n"
        "- Симметричные идеальные тройки ('это, это и это'). Пиши неровно, как живой человек, а не как аналитический отчёт.\n"
        "- Приветствия в начале поста.\n"
    )
    base_prompt = f"{prompt}\n{CHANNEL_AI_META_BAN_PROMPT}\n"
    retry_note = ""
    for attempt in range(3):
        text = await ask_ai(f"{base_prompt}{retry_note}", max_tokens=800)
        if not text:
            return ""
        text = clean_markdown(text)
        text = strip_channel_ai_meta_wrappers(text)
        text = sanitize_html_for_telegram(text)
        text = strip_channel_ai_meta_wrappers(text)
        text = sanitize_html_for_telegram(text)

        similarity, similar_record = _channel_similar_recent_post(text, content_plan)
        if similarity < CHANNEL_TEXT_SIMILARITY_THRESHOLD or attempt == 2:
            if similarity >= CHANNEL_TEXT_SIMILARITY_THRESHOLD:
                print(
                    "[channel_content] accepting closest available text "
                    f"after retries; similarity={similarity:.2f}"
                )
            return text

        sample = str((similar_record or {}).get("sample") or "")[:260]
        print(f"[channel_content] regenerated similar post, similarity={similarity:.2f}")
        retry_note = (
            "\n\nПРЕДЫДУЩИЙ ВАРИАНТ ОТКЛОНЕН КАК СЛИШКОМ ПОХОЖИЙ НА НЕДАВНИЙ ПОСТ.\n"
            f"Похожий недавний пост: {sample}\n"
            "Сгенерируй заново: другая бытовая сцена, другой первый абзац, другой главный образ, "
            "другой финальный вопрос. Не используй тот же набор карт, фраз, предметов или действий.\n"
        )
    return ""


def _mark_channel_post_time(msk) -> None:
    """Фиксирует время последнего поста в персистентном state, чтобы после
    перезапуска бот не постил сразу."""
    state = load_channel_state()
    state["last_post"] = msk.isoformat()
    save_channel_state(state)


def _get_last_channel_post_from_state():
    state = load_channel_state()
    last_post_iso = state.get("last_post")
    if not last_post_iso:
        return None
    try:
        return datetime.fromisoformat(last_post_iso)
    except Exception:
        return None


async def build_channel_post() -> dict | None:
    """Generates a post once so different publishing adapters can use it."""
    topic_info = _select_channel_topic()
    author_info = _select_channel_author(topic_info)
    content_plan = _select_channel_content_plan(topic_info, author_info)
    core_text = await generate_channel_post(topic_info["topic"], author_info, content_plan)
    if not core_text:
        print("[autoposting] AI returned empty post text, skipping")
        return None

    author_signature = _channel_author_signature(author_info)
    if content_plan.get("promo"):
        text = with_channel_bot_promo(core_text, author_signature)
    else:
        text = with_channel_final_suffix(core_text, author_signature)
    image_path = await generate_channel_image_asset(topic_info, author_info, content_plan, core_text)
    return {
        "text": text,
        "core_text": core_text,
        "image_path": image_path,
        "topic_info": topic_info,
        "author_info": author_info,
        "content_plan": content_plan,
        "schedule": topic_info.get("schedule") or {},
    }


_last_channel_publish_alert_at = 0.0
_last_channel_publish_result = {}


def _set_channel_publish_result(result: dict) -> None:
    global _last_channel_publish_result
    _last_channel_publish_result = {
        "configured": dict(result.get("configured") or {}),
        "results": dict(result.get("results") or {}),
        "errors": dict(result.get("errors") or {}),
    }


def get_last_channel_publish_result() -> dict:
    return {
        "configured": dict(_last_channel_publish_result.get("configured") or {}),
        "results": dict(_last_channel_publish_result.get("results") or {}),
        "errors": dict(_last_channel_publish_result.get("errors") or {}),
    }


def _redact_channel_publish_error(text: str) -> str:
    text = str(text or "")
    for secret in (
        TOKEN,
        ADMIN_BOT_TOKEN,
        OPENROUTER_KEY,
        GROQ_API_KEY,
        getenv("VK_ACCESS_TOKEN", ""),
        getenv("OK_ACCESS_TOKEN", ""),
        getenv("OK_SESSION_KEY", ""),
        getenv("OK_SESSION_SECRET_KEY", ""),
        getenv("OK_APPLICATION_SECRET_KEY", ""),
    ):
        if secret:
            text = text.replace(secret, "<secret>")
    return text[:1500]


async def _notify_channel_publish_issue(reason: str) -> None:
    global _last_channel_publish_alert_at
    now = time.monotonic()
    if now - _last_channel_publish_alert_at < CHANNEL_PUBLISH_ALERT_COOLDOWN_SEC:
        return
    _last_channel_publish_alert_at = now
    clean_reason = _redact_channel_publish_error(reason)
    if "IMAGE_PROCESS_FAILED" in clean_reason.upper():
        guidance = (
            "Telegram could not decode the generated photo. "
            "This is an image-file problem, not a CHANNEL_ID or administrator-rights problem."
        )
    else:
        guidance = (
            "Check that the main bot is an administrator of the channel and that "
            "CHANNEL_ID still points to the channel."
        )
    await notify_admin(
        "Channel autoposting failed.\n\n"
        f"CHANNEL_ID: {CHANNEL_ID or '-'}\n"
        f"Reason: {clean_reason}\n\n"
        f"{guidance}"
    )


async def check_channel_publish_access(notify: bool = False) -> bool:
    if not CHANNEL_ID:
        if notify:
            await _notify_channel_publish_issue("CHANNEL_ID is empty")
        return False

    try:
        chat = await bot.get_chat(CHANNEL_ID)
        me = await bot.get_me()
        member = await bot.get_chat_member(chat.id, me.id)
        status = getattr(member, "status", "")
        can_post = getattr(member, "can_post_messages", None)
        if status not in {"creator", "administrator"}:
            reason = f"Bot is not channel administrator; status={status}"
            if notify:
                await _notify_channel_publish_issue(reason)
            return False
        if getattr(chat, "type", "") == "channel" and status == "administrator" and can_post is False:
            reason = "Bot is channel administrator but can_post_messages=False"
            if notify:
                await _notify_channel_publish_issue(reason)
            return False
        return True
    except Exception as e:
        if notify:
            await _notify_channel_publish_issue(f"Cannot access channel {CHANNEL_ID}: {e}")
        return False


def _plain_channel_publish_text(text: str) -> str:
    return re.sub(r'</?[a-zA-Z][a-zA-Z0-9\-]*(?:\s[^>]*)?>', '', text)


def _resolve_channel_post_image_path(post: dict) -> str:
    image_path = post.get("image_path") or ""
    if image_path and os.path.exists(image_path):
        return image_path

    if image_path:
        print(f"[channel_image] generated image is missing on disk: {image_path}")

    provider = CHANNEL_IMAGE_PROVIDER.strip().lower()
    if not _should_use_local_channel_image_fallback(provider):
        return ""

    image_path = _generate_local_channel_image_asset(
        post.get("topic_info") or {},
        post.get("author_info"),
        post.get("content_plan"),
    )
    if image_path and os.path.exists(image_path):
        post["image_path"] = image_path
        return image_path
    return ""


async def _send_channel_post_payload(body: str, parse_mode: str | None, image_path: str = "") -> None:
    if image_path and os.path.exists(image_path):
        photo = FSInputFile(image_path)
        if len(body or "") <= TELEGRAM_PHOTO_CAPTION_LIMIT:
            await bot.send_photo(CHANNEL_ID, photo=photo, caption=body, parse_mode=parse_mode)
            return

        await bot.send_photo(CHANNEL_ID, photo=photo)
        try:
            await bot.send_message(CHANNEL_ID, body, parse_mode=parse_mode)
        except Exception as text_error:
            if parse_mode is None:
                raise
            print(f"[autoposting] Telegram rejected HTML after photo, sending plain text: {text_error}")
            await bot.send_message(CHANNEL_ID, _plain_channel_publish_text(body), parse_mode=None)
        return

    await bot.send_message(CHANNEL_ID, body, parse_mode=parse_mode)


def _is_telegram_image_error(error: Exception) -> bool:
    message = str(error or "").upper()
    return any(code in message for code in (
        "IMAGE_PROCESS_FAILED",
        "PHOTO_INVALID_DIMENSIONS",
        "PHOTO_CONTENT_TYPE_INVALID",
    ))


async def post_to_telegram_channel(post: dict) -> bool:
    if not CHANNEL_ID:
        await _notify_channel_publish_issue("CHANNEL_ID is empty")
        return False

    text = post["text"]
    image_path = _resolve_channel_post_image_path(post)
    if _channel_image_required() and not image_path:
        await _notify_channel_publish_issue("Channel real photo selection failed; post was not published without an image.")
        return False

    try:
        await _send_channel_post_payload(text, "HTML", image_path)
        return True
    except Exception as e:
        if image_path and _is_telegram_image_error(e):
            print(f"[autoposting] Telegram rejected image {image_path}: {e}")
            await _notify_channel_publish_issue(str(e))
            return False
        print(f"[autoposting] Telegram rejected HTML, sending plain text: {e}")
        try:
            plain = _plain_channel_publish_text(text)
            await _send_channel_post_payload(plain, None, image_path)
            return True
        except Exception as plain_error:
            print(f"[autoposting] Telegram publish error: {plain_error}")
            await _notify_channel_publish_issue(str(plain_error))
            return False


def has_configured_publish_target() -> bool:
    return bool(CHANNEL_ID or is_vk_configured() or is_ok_configured())


async def publish_channel_post() -> bool:
    publish_result = {"configured": {}, "results": {}, "errors": {}}
    _set_channel_publish_result(publish_result)
    try:
        telegram_configured = bool(CHANNEL_ID)
        vk_configured = is_vk_configured()
        ok_configured = is_ok_configured()
        ok_config_issue = "" if ok_configured else get_ok_config_issue()
        publish_result["configured"] = {
            "telegram": telegram_configured,
            "vk": vk_configured,
            "ok": ok_configured,
        }
        if not ok_configured and has_ok_env_hint():
            publish_result["errors"]["ok"] = ok_config_issue or "OK is not configured"
        _set_channel_publish_result(publish_result)

        if not (telegram_configured or vk_configured or ok_configured):
            print("[autoposting] no configured publish target")
            publish_result["errors"]["all"] = "No configured publish target"
            _set_channel_publish_result(publish_result)
            return False

        post = await build_channel_post()
        if not post:
            publish_result["errors"]["build"] = "Post generation failed"
            _set_channel_publish_result(publish_result)
            return False

        results = publish_result["results"]
        errors = publish_result["errors"]
        if telegram_configured:
            results["telegram"] = await post_to_telegram_channel(post)
            _set_channel_publish_result(publish_result)
        if vk_configured:
            vk_attempt = await post_channel_payload_to_vk_attempt(post)
            results["vk"] = vk_attempt.ok
            if vk_attempt.error:
                errors["vk"] = vk_attempt.error
            _set_channel_publish_result(publish_result)
        if ok_configured:
            ok_attempt = await post_channel_payload_to_ok_attempt(post)
            results["ok"] = ok_attempt.ok
            if ok_attempt.error:
                errors["ok"] = ok_attempt.error
            _set_channel_publish_result(publish_result)

        ok = any(results.values())
        if ok:
            msk = _msk_now()
            topic_info = post["topic_info"]
            _remember_channel_topic(topic_info)
            _remember_channel_content(
                post.get("content_plan"),
                post.get("core_text", post.get("text", "")),
                post.get("topic_info") or {},
            )
            _remember_channel_schedule_slot((post.get("schedule") or {}).get("slot_key", ""))
            topic_preview = _topic_key(topic_info)[:80]
            targets = ",".join(name for name, posted in results.items() if posted) or "-"
            failed_targets = ",".join(name for name, posted in results.items() if not posted)
            print(
                f"[MSK {msk}] Channel post published "
                f"(targets: {targets}, category: {topic_info.get('category', '-')}, "
                f"topic: {topic_preview}, photo: {'yes' if post.get('image_path') else 'no'})"
            )
            if failed_targets:
                details = "; ".join(f"{target}: {error}" for target, error in errors.items())
                reason = f"Post published only partially; failed targets: {failed_targets}"
                if details:
                    reason = f"{reason}. Details: {details}"
                await _notify_channel_publish_issue(reason)
        _set_channel_publish_result(publish_result)
        return ok
    except Exception as e:
        print(f"[autoposting] error: {e}")
        clean_error = _redact_channel_publish_error(str(e))
        publish_result["errors"]["autoposting"] = clean_error
        _set_channel_publish_result(publish_result)
        await _notify_channel_publish_issue(clean_error)
        return False


async def post_to_channel() -> bool:
    """Генерирует и отправляет пост в канал с подобранной по теме картинкой."""
    if not CHANNEL_ID:
        return False
    try:
        topic_info = _select_channel_topic()
        author_info = _select_channel_author(topic_info)
        content_plan = _select_channel_content_plan(topic_info, author_info)
        core_text = await generate_channel_post(topic_info["topic"], author_info, content_plan)
        if not core_text:
            print("[Автопостинг] ИИ не вернул текст, пропускаю")
            return False

        author_signature = _channel_author_signature(author_info)
        if content_plan.get("promo"):
            text = with_channel_bot_promo(core_text, author_signature)
        else:
            text = with_channel_final_suffix(core_text, author_signature)

        image_path = await generate_channel_image_asset(topic_info, author_info, content_plan, core_text)
        post_payload = {
            "image_path": image_path,
            "topic_info": topic_info,
            "author_info": author_info,
            "content_plan": content_plan,
        }
        image_path = _resolve_channel_post_image_path(post_payload)
        if _channel_image_required() and not image_path:
            await _notify_channel_publish_issue("Channel real photo selection failed; post was not published without an image.")
            return False

        try:
            await _send_channel_post_payload(text, "HTML", image_path)
        except Exception as e:
            if image_path and _is_telegram_image_error(e):
                await _notify_channel_publish_issue(str(e))
                return False
            # Если Telegram не принял HTML (битые теги), шлём чистый текст
            print(f"[Автопостинг] HTML отклонён Telegram, шлю plain: {e}")
            plain = _plain_channel_publish_text(text)
            await _send_channel_post_payload(plain, None, image_path)

        msk = _msk_now()
        _remember_channel_topic(topic_info)
        _remember_channel_content(content_plan, core_text, topic_info)
        schedule_slot_key = ((topic_info.get("schedule") or {}).get("slot_key") or "")
        _remember_channel_schedule_slot(schedule_slot_key)
        topic_preview = _topic_key(topic_info)[:80]
        print(f"[MSK {msk}] Пост отправлен в канал {CHANNEL_ID} "
              f"(категория: {topic_info.get('category', '-')}, тема: {topic_preview}, "
              f"фото: {'да' if image_path else 'нет'})")
        return True
    except Exception as e:
        print(f"[Автопостинг] Ошибка: {e}")
        return False

# ====== ПЛАНИРОВЩИК ======
async def scheduler():
    msk = _msk_now()
    today = msk.date()

    # При старте: обновить прогноз если он устарел
    forecast_data = load_forecast()
    if not forecast_data or forecast_data.get("date") != today.isoformat():
        await update_forecast()

    forecast_updated_date = today
    morning_sent_date = None

    # Восстанавливаем состояние автопостинга после возможного рестарта:
    # recent_topics — чтобы не повторить недавнюю тему,
    # last_post — чтобы не слать пост сразу после перезапуска.
    _sync_recent_topics_from_state()
    last_channel_post = _get_last_channel_post_from_state()

    # Если при старте уже прошло 8 утра — отмечаем как отправленное, чтобы не слать повторно
    if msk.hour >= 8:
        morning_sent_date = today

    while True:
        msk = _msk_now()
        today = msk.date()

        # Обновление прогноза в полночь по МСК
        if msk.hour == 0 and msk.minute == 0 and forecast_updated_date != today:
            forecast_updated_date = today
            await update_forecast()

        # Утренние уведомления в 8:00 по МСК
        # Проверяем весь час 8:xx — чтобы не пропустить из-за перезапуска
        if msk.hour == 8 and morning_sent_date != today:
            morning_sent_date = today
            await send_morning_notifications()

        # Автопостинг по редакционной недельной сетке: 09:00, 14:00, 19:30 МСК.
        if CHANNEL_ACTIVE_HOURS[0] <= msk.hour < CHANNEL_ACTIVE_HOURS[1]:
            if _select_due_channel_schedule_slot(msk):
                posted = await publish_channel_post()
                if posted:
                    last_channel_post = _msk_now()
                    _mark_channel_post_time(last_channel_post)

        next_minute = (msk + timedelta(minutes=1)).replace(second=0, microsecond=0)
        await asyncio.sleep((next_minute - msk).total_seconds())

# ====== ОБРАБОТЧИКИ ======
async def send_and_pin_channel_promo(chat_id: int) -> None:
    """Отправляет промо канала и закрепляет его в личном чате с пользователем."""
    if not CHANNEL_URL:
        return
    text = (
        "✨ *Голос Звёзд в Telegram* ✨\n\n"
        "🔮 Свежие прогнозы для всех знаков\n"
        "🃏 Расклады карт Таро от наших мастеров\n"
        "🌙 Лунные ритуалы и советы астрологов\n"
        "💫 Маленькие подсказки Вселенной каждый день\n\n"
        "Подпишись, чтобы звёзды всегда были рядом 👇"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🌟 Открыть канал", url=CHANNEL_URL)]
    ])
    try:
        msg = await bot.send_message(
            chat_id, text,
            parse_mode="Markdown",
            reply_markup=kb,
            disable_web_page_preview=True,
        )
        await bot.pin_chat_message(chat_id, msg.message_id, disable_notification=True)
    except Exception as e:
        print(f"[channel_promo] {e}")


async def maybe_pin_channel_after_consultation(user_id: int) -> None:
    """Закрепляет промо канала один раз — после первой консультации."""
    if not CHANNEL_URL:
        return
    user_id_str = str(user_id)
    users = load_users()
    if users.get(user_id_str, {}).get("channel_pinned"):
        return
    await send_and_pin_channel_promo(user_id)
    users = load_users()
    users.setdefault(user_id_str, {})["channel_pinned"] = True
    save_users(users)


@dp.message(F.text.regexp(r"^/start(\s|$)"))
async def start(message: Message):
    users = load_users()
    user_id = str(message.from_user.id)
    is_new_user = user_id not in users or "sign" not in users.get(user_id, {})
    # Обновляем имя и username при каждом /start
    users.setdefault(user_id, {})
    users[user_id]["username"] = message.from_user.username or ""
    users[user_id]["full_name"] = message.from_user.full_name or ""
    payload = ""
    if message.text:
        parts = message.text.split(maxsplit=1)
        if len(parts) > 1:
            payload = parts[1].strip()
    specialist_target = _resolve_specialist_start(payload)
    if specialist_target:
        specialist_type, specialist = specialist_target
        if is_new_user:
            users[user_id]["joined_at"] = datetime.now().isoformat()
            users[user_id]["first_day_msk"] = _msk_now().date().isoformat()
            save_users(users)
            asyncio.create_task(_notify_new_user(message))
        else:
            save_users(users)
        card_text, card_kb = _specialist_card_text(specialist_type, specialist, message.from_user.id)
        await message.answer(card_text, parse_mode="Markdown", reply_markup=card_kb)
        return
    # Обработка реферальной ссылки (только для новых пользователей)
    ref_id = None
    if payload.startswith("ref_") and is_new_user:
        ref_id = payload.replace("ref_", "", 1).strip()
        if ref_id == user_id:
            ref_id = None  # нельзя пригласить самого себя
    if user_id in users and "sign" in users[user_id]:
        sign = users[user_id]["sign"]
        save_users(users)
        await message.answer(f"С возвращением! Твой знак: {sign}. 🌟", reply_markup=get_main_keyboard())
    else:
        users[user_id]["joined_at"] = datetime.now().isoformat()
        users[user_id]["first_day_msk"] = _msk_now().date().isoformat()
        save_users(users)
        await message.answer(
            "Привет! Я — Голос Звёзд 🌟\n\n"
            "Задай вопрос тарологу — он разложит карты и подскажет, что тебя ждёт.\n"
            "Ниже — астрологи и главное меню с прогнозом по знаку.",
            reply_markup=get_welcome_keyboard()
        )
        asyncio.create_task(_notify_new_user(message))
    # Сохраняем реферальную связь (бонус начислится после первого сеанса друга)
    if ref_id:
        saved = save_referral_link(ref_id, user_id)
        if saved:
            try:
                await bot.send_message(
                    int(ref_id),
                    "👋 Твой друг перешёл по твоей ссылке\\!\n"
                    "Бонусный сеанс начислится, когда друг пройдёт свою первую консультацию\\. 🌟",
                    parse_mode="MarkdownV2"
                )
            except Exception:
                pass

# Админ-команды (/user, /users, /stats) живут в отдельном боте mainAdmin.py


@dp.message(F.text == "🏠 Главное меню")
async def go_home(message: Message):
    user_id = str(message.from_user.id)
    if user_id in WAITING_TAROT_STORY:
        del WAITING_TAROT_STORY[user_id]
    if user_id in WAITING_ASTRO_STORY:
        del WAITING_ASTRO_STORY[user_id]
    if user_id in WAITING_SIGN_CHANGE:
        del WAITING_SIGN_CHANGE[user_id]
    WAITING_PROMOCODE.discard(user_id)
    WAITING_REVIEW.pop(user_id, None)
    if user_id in ACTIVE_SESSIONS:
        del ACTIVE_SESSIONS[user_id]
        _save_active_sessions()
    SESSION_BUSY.pop(user_id, None)
    SESSION_MSG_QUEUE.pop(user_id, None)
    msg = await message.answer("Главное меню:", reply_markup=get_main_keyboard())

@dp.message(F.text.in_(SIGNS))
async def set_sign(message: Message):
    users = load_users()
    user_id = str(message.from_user.id)
    users.setdefault(user_id, {})["sign"] = message.text
    save_users(users)

    if user_id in WAITING_SIGN_CHANGE:
        del WAITING_SIGN_CHANGE[user_id]
        # Сбрасываем кэш прогноза, чтобы при смене знака показывался новый прогноз
        users = load_users()
        users.setdefault(user_id, {}).pop("forecast_date", None)
        users[user_id].pop("forecast_msg_id", None)
        save_users(users)
        msg = await message.answer(f"Знак зодиака изменён на {message.text}! ✨", reply_markup=get_main_keyboard())
        return

    msg = await message.answer("Твой знак сохранён! Добро пожаловать в Голос Звёзд 🌟", reply_markup=get_main_keyboard())

@dp.message(F.text == "🔮 Прогноз на сегодня")
async def send_forecast(message: Message):
    users = load_users()
    user_id = str(message.from_user.id)
    track_activity(user_id, "forecast")
    user_data = users.get(user_id, {})

    if not user_data or "sign" not in user_data:
        msg = await message.answer("Сначала выбери знак зодиака", reply_markup=get_sign_keyboard())
        return

    sign = user_data["sign"]
    forecast_data = load_forecast()

    if "ru" not in forecast_data or sign not in forecast_data["ru"]:
        msg = await message.answer("Прогноз ещё не готов, попробуй позже 🌙")
        return

    today = datetime.now().date().isoformat()

    # Если пользователь уже смотрел прогноз сегодня — ссылаемся на то сообщение
    if user_data.get("forecast_date") == today and user_data.get("forecast_msg_id"):
        try:
            msg = await message.answer(
                "↩️ Твой прогноз на сегодня уже здесь 👆",
                reply_to_message_id=user_data["forecast_msg_id"]
            )
            return
        except Exception:
            pass  # если старое сообщение удалено — отправим заново

    # Отправляем новый прогноз (не добавляем в nav_msgs — он защищённый)
    forecast_msg = await message.answer(
        f"🔮 *{sign}*\n\n{forecast_data['ru'][sign]}",
        parse_mode="Markdown"
    )
    # Сохраняем дату и ID прогноза
    users = load_users()
    users.setdefault(user_id, {})["forecast_date"] = today
    users[user_id]["forecast_msg_id"] = forecast_msg.message_id
    save_users(users)
    # Восстанавливаем клавиатуру — она могла пропасть после удаления предыдущих сообщений
    nav_msg = await message.answer("☝️ Прогноз выше", reply_markup=get_main_keyboard())

@dp.message(F.text == "📖 Мой знак")
async def read_about_me(message: Message):
    users = load_users()
    user_id = str(message.from_user.id)
    track_activity(user_id, "about_me")
    user_data = users.get(user_id)

    if not user_data or "sign" not in user_data:
        msg = await message.answer("Сначала выбери знак зодиака", reply_markup=get_sign_keyboard())
        return

    sign = user_data["sign"]
    descriptions = load_descriptions()
    cache_key = f"{sign}_ru"

    if cache_key not in descriptions:
        msg = await message.answer("⏳ Составляю описание твоего знака...")
        description = await get_sign_description(sign)
        if not description:
            msg = await message.answer("Не удалось получить описание, попробуй позже.")
            return
        descriptions[cache_key] = description
        save_descriptions(descriptions)
    else:
        description = descriptions[cache_key]

    description = re.sub(r'#{1,6}\s*', '', description)

    full_text = f"📖 *{sign}*\n\n{description}"
    parts = [full_text[i:i+4000] for i in range(0, len(full_text), 4000)]
    for i, part in enumerate(parts):
        if i == len(parts) - 1:
            msg = await message.answer(part, parse_mode="Markdown", reply_markup=get_main_keyboard())
        else:
            msg = await message.answer(part, parse_mode="Markdown")

# ====== СОВМЕСТИМОСТЬ ЗНАКОВ ======
@dp.message(F.text == "💕 Совместимость")
async def compat_start(message: Message):
    user_id = str(message.from_user.id)
    track_activity(user_id, "compatibility")
    await message.answer(
        "💕 *Совместимость знаков*\n\nВыбери свой знак зодиака:",
        parse_mode="Markdown",
        reply_markup=get_compat_sign_keyboard("first")
    )

@dp.callback_query(F.data.startswith("compat1_"))
async def compat_pick_first(callback: CallbackQuery):
    try:
        sign1_idx = int(callback.data.replace("compat1_", ""))
    except ValueError:
        await callback.answer("Неизвестный знак")
        return
    if sign1_idx < 0 or sign1_idx >= len(SIGNS):
        await callback.answer("Неизвестный знак")
        return
    sign1 = SIGNS[sign1_idx]
    await callback.message.edit_text(
        f"💕 Твой знак: *{sign1}*\n\nТеперь выбери знак партнёра или друга:",
        parse_mode="Markdown",
        reply_markup=get_compat_sign_keyboard("second", sign1_idx)
    )
    await callback.answer()

@dp.callback_query(F.data == "compat_back")
async def compat_go_back(callback: CallbackQuery):
    await callback.message.edit_text(
        "💕 *Совместимость знаков*\n\nВыбери свой знак зодиака:",
        parse_mode="Markdown",
        reply_markup=get_compat_sign_keyboard("first")
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("compat2_"))
async def compat_pick_second(callback: CallbackQuery):
    # callback_data = "compat2_{sign1_idx}_{sign2_idx}"
    parts = callback.data.split("_")
    if len(parts) != 3:
        await callback.answer("Ошибка, попробуй заново")
        return
    try:
        sign1_idx = int(parts[1])
        sign2_idx = int(parts[2])
    except ValueError:
        await callback.answer("Ошибка, попробуй заново")
        return
    if not (0 <= sign1_idx < len(SIGNS) and 0 <= sign2_idx < len(SIGNS)):
        await callback.answer("Ошибка, попробуй заново")
        return

    sign1 = SIGNS[sign1_idx]
    sign2 = SIGNS[sign2_idx]

    # Ключ кеша — сортированная пара, чтобы Овен+Рыбы == Рыбы+Овен
    cache_key = "_".join(sorted([sign1, sign2]))
    compat_cache = load_compatibility()

    if cache_key in compat_cache:
        result = compat_cache[cache_key]
        await callback.answer()
    else:
        await callback.message.edit_text("⏳ Анализирую совместимость...")
        await callback.answer()

        result = await get_compatibility(sign1, sign2)
        if not result:
            await callback.message.edit_text(
                "Не удалось получить разбор совместимости, попробуй позже 🌙"
            )
            return

        result = re.sub(r'#{1,6}\s*', '', result)
        compat_cache[cache_key] = result
        save_compatibility(compat_cache)
    text = f"💕 *{sign1} + {sign2}*\n\n{result}"

    # Кнопка «Поделиться» для вирусности
    bot_info = await bot.get_me()
    share_text = f"💕 Узнай совместимость своего знака зодиака!\nПопробуй бота: https://t.me/{bot_info.username}"
    share_kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Другая пара", callback_data="compat_restart")],
        [InlineKeyboardButton(
            text="📤 Поделиться с другом",
            switch_inline_query=share_text
        )]
    ])

    parts = [text[i:i+4000] for i in range(0, len(text), 4000)]
    for i, part in enumerate(parts):
        if i == len(parts) - 1:
            await callback.message.answer(part, parse_mode="Markdown", reply_markup=share_kb)
        else:
            await callback.message.answer(part, parse_mode="Markdown")

@dp.callback_query(F.data == "compat_restart")
async def compat_restart(callback: CallbackQuery):
    await callback.message.answer(
        "💕 *Совместимость знаков*\n\nВыбери свой знак зодиака:",
        parse_mode="Markdown",
        reply_markup=get_compat_sign_keyboard("first")
    )
    await callback.answer()

@dp.message(F.text == "🌟 Консультация")
async def consultations_menu(message: Message):
    user_id = str(message.from_user.id)
    balance_line = _available_session_note(user_id)
    msg = await message.answer(
        f"🌟 *Консультация*\n\n{balance_line}\n\nВыбери специалиста:",
        parse_mode="Markdown",
        reply_markup=get_consultations_keyboard()
    )

async def _start_promo_input(message: Message, user_id: str) -> None:
    if user_id in ACTIVE_SESSIONS:
        await message.answer(
            "Сначала заверши текущий сеанс, потом можно будет активировать промокод.",
            reply_markup=get_session_keyboard(),
        )
        return
    WAITING_TAROT_STORY.pop(user_id, None)
    WAITING_ASTRO_STORY.pop(user_id, None)
    WAITING_REVIEW.pop(user_id, None)
    WAITING_PROMOCODE.add(user_id)
    await message.answer(
        "🎟 Введи промокод одним сообщением.\n\n"
        "Если передумал, нажми «❌ Отменить» или вернись в главное меню.",
        reply_markup=get_cancel_keyboard(),
    )

@dp.message(F.text == "🎟 Ввести промокод")
async def promo_code_prompt(message: Message):
    await _start_promo_input(message, str(message.from_user.id))

@dp.callback_query(F.data == "promo_start")
async def promo_code_prompt_callback(callback: CallbackQuery):
    if callback.message:
        await _start_promo_input(callback.message, str(callback.from_user.id))
    await callback.answer()

def _promo_activation_error_text(reason: str) -> str:
    return {
        "invalid_code": "Промокод выглядит странно. Проверь, что в нём только латинские буквы, цифры, дефис или подчёркивание.",
        "not_found": "Не нашёл такой промокод. Проверь символы и попробуй ещё раз.",
        "inactive": "Этот промокод уже отключён.",
        "expired": "Срок действия этого промокода истёк.",
        "already_used": "Ты уже активировал этот промокод раньше.",
        "exhausted": "Этот промокод уже использовали максимальное количество раз.",
    }.get(reason, "Не получилось активировать промокод. Попробуй ещё раз чуть позже.")

async def _handle_promo_code_input(message: Message) -> None:
    user_id = str(message.from_user.id)
    try:
        result = promo_store.activate_code(
            message.text or "",
            user_id=user_id,
            username=message.from_user.username or "",
            full_name=message.from_user.full_name or "",
        )
    except Exception as e:
        print(f"[promo] activation failed for user {user_id}: {e}")
        asyncio.create_task(notify_admin(f"[promo] Activation failed for user {user_id}: {e}"))
        await message.answer(
            "Не получилось проверить промокод из-за технической ошибки. Попробуй чуть позже.",
            reply_markup=get_consultations_keyboard(),
        )
        return
    if not result.ok:
        await message.answer(
            f"{_promo_activation_error_text(result.reason)}\n\n"
            "Можешь ввести другой код или нажать «❌ Отменить».",
            reply_markup=get_cancel_keyboard(),
        )
        return

    WAITING_PROMOCODE.discard(user_id)
    total_available = get_available_session_count(user_id)
    await message.answer(
        f"✅ Промокод {result.code} активирован.\n\n"
        f"Начислено: {result.sessions} {_session_word(result.sessions)}.\n"
        f"Промо-сеансов осталось: {result.balance}.\n"
        f"Всего доступно: {total_available} {_session_word(total_available)}.\n\n"
        "Теперь можно выбрать специалиста.",
        reply_markup=get_consultations_keyboard(),
    )
    user_label = (
        f"@{message.from_user.username}" if message.from_user.username
        else (message.from_user.full_name or f"ID {user_id}")
    )
    asyncio.create_task(notify_admin(
        f"🎟 Активирован промокод\n"
        f"Код: {result.code}\n"
        f"Пользователь: {user_label} [ID {user_id}]\n"
        f"Начислено сеансов: {result.sessions}\n"
        f"Промо-баланс пользователя: {result.balance}"
    ))

@dp.callback_query(F.data == "ckassa_check")
async def ckassa_check_payment(callback: CallbackQuery):
    user_id = str(callback.from_user.id)
    try:
        credited = await process_ckassa_payment_updates(notify_users=True)
    except CkassaPaymentConfigError:
        await callback.answer("Оплата пока не настроена.", show_alert=True)
        return
    except CkassaPaymentError:
        await callback.answer("Не удалось проверить оплату. Попробуй чуть позже.", show_alert=True)
        return
    except Exception as e:
        print(f"[Ckassa] manual check failed: {e}")
        await callback.answer("Не удалось проверить оплату. Попробуй чуть позже.", show_alert=True)
        return

    if any(str(order.get("user_id")) == user_id for order in credited):
        await callback.answer("Оплата подтверждена.")
        return

    if has_available_session(user_id):
        await callback.message.answer(
            "У тебя уже есть доступный сеанс. Выбери специалиста в разделе консультаций.",
            reply_markup=get_consultations_keyboard(),
        )
        await callback.answer("Сеанс доступен.")
        return

    await callback.answer("Пока не вижу подтверждение оплаты. Обычно это занимает 30-60 секунд.", show_alert=True)

@dp.callback_query(F.data.startswith("ckassa_refresh:"))
async def ckassa_refresh_invoice(callback: CallbackQuery):
    order_id = callback.data.removeprefix("ckassa_refresh:")
    user_id = str(callback.from_user.id)

    await callback.answer("Обновляю ссылку на оплату...")

    async with CKASSA_STATE_LOCK:
        order = ckassa_store.get_order(order_id)
        if not order or str(order.get("user_id")) != user_id:
            await callback.message.answer(
                "Не нашёл этот счёт. Выбери специалиста ещё раз, и я создам новую ссылку.",
                reply_markup=get_consultations_keyboard(),
            )
            return
        if order.get("status") != "created":
            await callback.message.answer(
                "Эта ссылка уже была обновлена или закрыта. Используй последнее сообщение со счётом."
            )
            return

        try:
            canceled = await ckassa_client.cancel_invoice(order.get("invoice_url", ""))
        except CkassaPaymentError as e:
            print(f"[Ckassa] cancel invoice before refresh failed for user {user_id}: {e}")
            await notify_admin(
                f"[Ckassa] Cancel invoice before refresh failed for user {user_id}: {e}"
            )
            canceled = False

        if not canceled:
            print(f"[Ckassa] invoice {order_id} could not be canceled; replacing locally")
        ckassa_store.mark_order_status(order_id, "replaced")

    await offer_ckassa_payment(
        callback.message,
        callback.from_user,
        order.get("specialist_type", ""),
        order.get("specialist_id", ""),
    )

@dp.message(F.text.in_({"🎴 Тарологи", "🎴 Задать вопрос тарологу"}))
async def tarot_list(message: Message):
    user_id = str(message.from_user.id)
    WAITING_PROMOCODE.discard(user_id)
    remaining = get_available_session_count(user_id)
    is_admin = message.from_user.id == ADMIN_ID

    if not is_admin and remaining == 0:
        bonus = get_bonus_sessions(user_id)
        hint = "\n\n🎁 Пригласи друга и получи бонусный сеанс!" if bonus == 0 else ""
        limit_note = (
            "\n\n🔒 *Лимит сеансов исчерпан.* "
            "Выбери специалиста, и я дам ссылку на оплату консультации. "
            "Если у тебя есть промокод, введи его в разделе консультаций. "
            f"Бонусных сеансов: {bonus}.{hint}"
        )
    elif not is_admin:
        available_parts = _available_session_parts(user_id)
        bonus_note = f" (из них {', '.join(available_parts)})" if available_parts else ""
        limit_note = (
            f"\n\n⚠️ *Внимание:* у тебя осталось *{remaining}* {_session_word(remaining)}{bonus_note}. "
            "Пригласи друга — получи ещё!"
        )
    else:
        limit_note = ""

    await message.answer(
        f"🔯 *Наши тарологи*\n\nНажми на имя — увидишь карточку специалиста и сможешь выбрать его 👇{limit_note}",
        parse_mode="Markdown",
        reply_markup=get_back_keyboard()
    )
    await message.answer(
        "Кто тебя интересует?",
        reply_markup=get_tarologists_list_keyboard()
    )

@dp.callback_query(F.data == "tarot_list")
async def back_to_tarot_list(callback: CallbackQuery):
    await callback.message.edit_text(
        "Кто тебя интересует?",
        reply_markup=get_tarologists_list_keyboard()
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("view_") & ~F.data.startswith("view_astro_"))
async def view_tarot_card(callback: CallbackQuery):
    tarot_id = callback.data.replace("view_", "")
    tarologist = TAROLOGISTS_BY_ID.get(tarot_id)
    if not tarologist:
        await callback.answer("Таролог не найден")
        return
    card_text, card_kb = _specialist_card_text("tarot", tarologist, callback.from_user.id)
    await callback.message.edit_text(card_text, parse_mode="Markdown", reply_markup=card_kb)
    await callback.answer()

@dp.callback_query(F.data.startswith("ask_") & ~F.data.startswith("ask_astro_"))
async def ask_tarot(callback: CallbackQuery):
    tarot_id = callback.data.replace("ask_", "")
    tarologist = TAROLOGISTS_BY_ID.get(tarot_id)
    if not tarologist:
        await callback.answer("Таролог не найден")
        return

    if not can_start_consultation_now(callback.from_user):
        await callback.message.answer(get_offline_message(tarologist["name"]), parse_mode="Markdown")
        await callback.answer()
        return

    user_id = str(callback.from_user.id)
    WAITING_PROMOCODE.discard(user_id)
    if callback.from_user.id != ADMIN_ID and not has_available_session(user_id):
        await offer_ckassa_payment(callback.message, callback.from_user, "tarot", tarot_id)
        await callback.answer()
        return

    WAITING_TAROT_STORY[user_id] = tarot_id

    brief = (
        f"📋 Отлично, ты выбрал *{tarologist['name']}*!\n\n"
        "Чтобы таролог мог дать точный ответ, опиши свою ситуацию по плану:\n\n"
        "1. *История* что уже произошло и что происходит сейчас\n"
        "2. *Участники* имя, пол, возраст, кем приходится тебе, дата рождения если знаешь, время рождения если есть\n"
        "3. *Запрос* что именно хочешь узнать\n"
        "4. *Твои чувства* как ты сейчас себя чувствуешь\n"
        "5. *Временной горизонт* на какой период хочешь прогноз\n\n"
        "Напиши всё одним сообщением в свободной форме 👇\n\n"
        "_Или запиши голосовое сообщение 🎤\n"
        "⚠️ Важно: говори чётко и разборчиво, без фонового шума, "
        "иначе сообщение может не дойти до таролога._"
    )

    user_id = str(callback.from_user.id)
    msg = await callback.message.answer(brief, parse_mode="Markdown", reply_markup=get_cancel_keyboard())
    await callback.answer()

@dp.message(F.text == "⭐ Астрологи")
async def astro_list(message: Message):
    user_id = str(message.from_user.id)
    WAITING_PROMOCODE.discard(user_id)
    remaining = get_available_session_count(user_id)
    is_admin = message.from_user.id == ADMIN_ID

    if not is_admin and remaining == 0:
        bonus = get_bonus_sessions(user_id)
        hint = "\n\n🎁 Пригласи друга и получи бонусный сеанс!" if bonus == 0 else ""
        limit_note = (
            "\n\n🔒 *Лимит сеансов исчерпан.* "
            "Выбери специалиста, и я дам ссылку на оплату консультации. "
            "Если у тебя есть промокод, введи его в разделе консультаций. "
            f"Бонусных сеансов: {bonus}.{hint}"
        )
    elif not is_admin:
        available_parts = _available_session_parts(user_id)
        bonus_note = f" (из них {', '.join(available_parts)})" if available_parts else ""
        limit_note = (
            f"\n\n⚠️ *Внимание:* у тебя осталось *{remaining}* {_session_word(remaining)}{bonus_note}. "
            "Пригласи друга — получи ещё!"
        )
    else:
        limit_note = ""

    await message.answer(
        f"⭐ *Наши астрологи*\n\nНажми на имя — увидишь карточку специалиста и сможешь выбрать его 👇{limit_note}",
        parse_mode="Markdown",
        reply_markup=get_back_keyboard()
    )
    await message.answer(
        "Кто тебя интересует?",
        reply_markup=get_astrologers_list_keyboard()
    )

@dp.callback_query(F.data == "astro_list")
async def back_to_astro_list(callback: CallbackQuery):
    await callback.message.edit_text(
        "Кто тебя интересует?",
        reply_markup=get_astrologers_list_keyboard()
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("view_astro_"))
async def view_astro_card(callback: CallbackQuery):
    astro_id = callback.data.replace("view_astro_", "")
    astrologer = ASTROLOGERS_BY_ID.get(astro_id)
    if not astrologer:
        await callback.answer("Астролог не найден")
        return
    card_text, card_kb = _specialist_card_text("astro", astrologer, callback.from_user.id)
    await callback.message.edit_text(card_text, parse_mode="Markdown", reply_markup=card_kb)
    await callback.answer()

@dp.callback_query(F.data.startswith("ask_astro_"))
async def ask_astro(callback: CallbackQuery):
    astro_id = callback.data.replace("ask_astro_", "")
    astrologer = ASTROLOGERS_BY_ID.get(astro_id)
    if not astrologer:
        await callback.answer("Астролог не найден")
        return

    if not can_start_consultation_now(callback.from_user):
        await callback.message.answer(get_offline_message(astrologer["name"]), parse_mode="Markdown")
        await callback.answer()
        return

    user_id = str(callback.from_user.id)
    WAITING_PROMOCODE.discard(user_id)
    if callback.from_user.id != ADMIN_ID and not has_available_session(user_id):
        await offer_ckassa_payment(callback.message, callback.from_user, "astro", astro_id)
        await callback.answer()
        return

    WAITING_ASTRO_STORY[user_id] = astro_id

    brief = (
        f"📋 Отлично, ты выбрал *{astrologer['name']}*!\n\n"
        "Для точного астрологического прогноза укажи данные участников:\n\n"
        "1. *Дата рождения* — день, месяц, год (обязательно)\n"
        "2. *Время рождения* — час и минуты (очень важно для точности)\n"
        "3. *Место рождения* — город или страна\n"
        "4. *Ситуация* — что происходит сейчас\n"
        "5. *Вопрос* — что именно хочешь узнать\n"
        "6. *Участники* — если вопрос про другого человека: те же данные о нём\n\n"
        "Напиши всё одним сообщением в свободной форме 👇\n\n"
        "_Если времени рождения нет — укажи это, прогноз будет чуть менее точным._\n\n"
        "_Или запиши голосовое сообщение 🎤_"
    )

    msg = await callback.message.answer(brief, parse_mode="Markdown", reply_markup=get_cancel_keyboard())
    await callback.answer()

@dp.message(F.text == "❌ Отменить")
async def cancel_tarot(message: Message):
    user_id = str(message.from_user.id)
    if user_id in WAITING_TAROT_STORY:
        del WAITING_TAROT_STORY[user_id]
    if user_id in WAITING_ASTRO_STORY:
        del WAITING_ASTRO_STORY[user_id]
    WAITING_PROMOCODE.discard(user_id)
    msg = await message.answer("Отменено.", reply_markup=get_main_keyboard())

@dp.message(F.text == "⭐ Отзывы")
async def show_reviews(message: Message):
    WAITING_PROMOCODE.discard(str(message.from_user.id))
    write_btn = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="✍️ Оставить отзыв", callback_data="write_review")
    ]])
    await message.answer(
        "⭐ *Отзывы наших пользователей*\n\nЧитайте что говорят люди которые уже пользуются Голосом Звёзд 👇",
        parse_mode="Markdown",
        reply_markup=get_main_keyboard()
    )
    text = get_reviews_page_text(0)
    keyboard = get_reviews_more_keyboard(REVIEWS_PER_PAGE)
    await message.answer(text, parse_mode="Markdown", reply_markup=keyboard)
    await message.answer(
        "Хочешь поделиться своим впечатлением?",
        reply_markup=write_btn
    )

@dp.callback_query(F.data.startswith("reviews_"))
async def show_more_reviews(callback: CallbackQuery):
    offset = int(callback.data.replace("reviews_", ""))
    text = get_reviews_page_text(offset)
    keyboard = get_reviews_more_keyboard(offset + REVIEWS_PER_PAGE)
    await callback.message.answer(text, parse_mode="Markdown", reply_markup=keyboard)
    await callback.answer()

# ====== ОСТАВИТЬ ОТЗЫВ: НАЧАЛО ======
@dp.callback_query(F.data == "write_review")
async def review_start(callback: CallbackQuery):
    user_id = str(callback.from_user.id)
    WAITING_PROMOCODE.discard(user_id)
    if user_id in ACTIVE_SESSIONS:
        await callback.answer("Сначала заверши текущий сеанс.", show_alert=True)
        return
    WAITING_REVIEW[user_id] = {"step": "topic"}
    await callback.message.answer(
        "✍️ *Оставить отзыв*\n\nПро что хочешь написать?",
        parse_mode="Markdown",
        reply_markup=get_review_topic_keyboard()
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("rev_topic_"))
async def review_topic(callback: CallbackQuery):
    user_id = str(callback.from_user.id)
    if user_id not in WAITING_REVIEW or WAITING_REVIEW[user_id].get("step") != "topic":
        await callback.answer()
        return
    tag = REVIEW_TOPIC_MAP.get(callback.data)
    if not tag:
        await callback.answer()
        return
    WAITING_REVIEW[user_id]["topic"] = tag
    WAITING_REVIEW[user_id]["step"] = "anon"
    await callback.message.answer(
        "Как хочешь подписать отзыв?",
        reply_markup=get_review_anon_keyboard()
    )
    await callback.answer()

@dp.callback_query(F.data.in_({"rev_anon_yes", "rev_anon_no"}))
async def review_anon(callback: CallbackQuery):
    user_id = str(callback.from_user.id)
    if user_id not in WAITING_REVIEW or WAITING_REVIEW[user_id].get("step") != "anon":
        await callback.answer()
        return
    if callback.data == "rev_anon_yes":
        WAITING_REVIEW[user_id]["anonymous"] = True
        WAITING_REVIEW[user_id]["name"] = "Анонимный пользователь"
        WAITING_REVIEW[user_id]["step"] = "text"
        await callback.message.answer(
            "Отлично! Теперь напиши свой отзыв — просто отправь сообщение:",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="❌ Отмена", callback_data="rev_cancel")
            ]])
        )
    else:
        WAITING_REVIEW[user_id]["anonymous"] = False
        WAITING_REVIEW[user_id]["tg_username"] = callback.from_user.username  # может быть None
        WAITING_REVIEW[user_id]["step"] = "name"
        await callback.message.answer(
            "Как тебя зовут? Напиши своё имя:",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="❌ Отмена", callback_data="rev_cancel")
            ]])
        )
    await callback.answer()

@dp.callback_query(F.data == "rev_cancel")
async def review_cancel(callback: CallbackQuery):
    user_id = str(callback.from_user.id)
    WAITING_REVIEW.pop(user_id, None)
    await callback.message.answer("Отменено.", reply_markup=get_main_keyboard())
    await callback.answer()


@dp.callback_query(F.data == "fb_done")
async def feedback_done(callback: CallbackQuery):
    user_id = str(callback.from_user.id)
    entry = _pop_waiting_feedback(user_id)
    if not entry:
        await callback.answer("Этот отзыв уже закрыт.", show_alert=False)
        return
    stype = entry.get("type", "")
    sname = entry.get("specialist_name", "—")
    count = int(entry.get("messages_count", 0))
    type_label = "🎴 Таролог" if stype == "tarot" else "⭐ Астролог"
    user_label = (
        f"@{callback.from_user.username}" if callback.from_user.username
        else (callback.from_user.full_name or f"ID {user_id}")
    )
    await callback.message.answer(
        "💛 Спасибо за отзыв! Мы всё передали администрации. "
        "Если появятся новые вопросы — возвращайтесь.",
        reply_markup=get_main_keyboard(),
    )
    asyncio.create_task(notify_admin(
        f"✅ Отзыв завершён пользователем\n\n"
        f"👤 {user_label} [ID {user_id}]\n"
        f"{type_label}: {sname}\n"
        f"💬 Сообщений в отзыве: {count}"
    ))
    await callback.answer()


@dp.callback_query(F.data == "fb_later")
async def feedback_later(callback: CallbackQuery):
    user_id = str(callback.from_user.id)
    entry = _update_waiting_feedback(user_id, state="deferred")
    if not entry:
        await callback.answer("Запрос на отзыв уже не активен.", show_alert=False)
        return
    await callback.message.answer(
        "⏰ Хорошо, не торопитесь. Когда будете готовы поделиться впечатлением — "
        "просто нажмите кнопку ниже:",
        reply_markup=_feedback_resume_keyboard(),
    )
    sname = entry.get("specialist_name", "—")
    user_label = (
        f"@{callback.from_user.username}" if callback.from_user.username
        else (callback.from_user.full_name or f"ID {user_id}")
    )
    asyncio.create_task(notify_admin(
        f"⏰ Отзыв отложен пользователем\n\n"
        f"👤 {user_label} [ID {user_id}]\n"
        f"⭐ {sname}\n"
        f"Ожидаем, пока пользователь вернётся к отзыву."
    ))
    await callback.answer()


@dp.callback_query(F.data == "fb_resume")
async def feedback_resume(callback: CallbackQuery):
    user_id = str(callback.from_user.id)
    entry = _load_waiting_feedback().get(user_id)
    if not entry:
        await callback.answer("Запрос на отзыв уже не активен.", show_alert=False)
        return
    if _feedback_is_expired(entry):
        _pop_waiting_feedback(user_id)
        await callback.message.answer(
            "Время для этого отзыва истекло. Если будут впечатления — "
            "напишите их через раздел «⭐ Отзывы»."
        )
        await callback.answer()
        return
    _update_waiting_feedback(user_id, state="active")
    await callback.message.answer(
        "✍️ Отлично! Делитесь впечатлениями — можно несколькими сообщениями. "
        "Когда закончите, нажмите «✅ Завершить отзыв».",
        reply_markup=_feedback_action_keyboard(),
    )
    await callback.answer()

@dp.message(F.text == "ℹ️ О нас")
async def about_us(message: Message):
    user_id = message.from_user.id
    msg = await message.answer(ABOUT_TEXT, parse_mode="Markdown", reply_markup=get_main_keyboard())

@dp.message(F.text == "🎁 Друзьям")
async def referral_menu(message: Message):
    user_id = str(message.from_user.id)
    bot_info = await bot.get_me()
    ref_link = f"https://t.me/{bot_info.username}?start=ref_{user_id}"
    users = load_users()
    user_data = users.get(user_id, {})
    total_refs = user_data.get("referrals_total", 0)
    bonus = user_data.get("bonus_sessions", 0)
    # Считаем друзей, которые пришли но ещё не прошли сеанс
    pending_refs = 0
    for udata in users.values():
        if udata.get("referred_by") == user_id and not udata.get("referral_bonus_granted"):
            pending_refs += 1
    pending_line = f"\n⏳ Ждут первого сеанса: *{pending_refs}*" if pending_refs > 0 else ""
    text = (
        "🎁 *Пригласи друга — получи бонусный сеанс\\!*\n\n"
        "Отправь эту ссылку другу:\n"
        f"`{ref_link}`\n\n"
        "Когда друг пройдёт свою первую консультацию, "
        f"тебе начислится *\\+{BONUS_SESSIONS_PER_REFERRAL} бонусный сеанс* "
        "с любым тарологом или астрологом\\. 🌟\n\n"
        f"👥 Приглашено друзей: *{total_refs}*{pending_line}\n"
        f"🎴 Бонусных сеансов доступно: *{bonus}*"
    )
    await message.answer(text, parse_mode="MarkdownV2", reply_markup=get_main_keyboard())

@dp.message(F.text == "⚙️ Настройки")
async def settings(message: Message):
    user_id = message.from_user.id
    msg = await message.answer("⚙️ Настройки:", reply_markup=get_settings_keyboard())

@dp.message(F.text == "📄 Оферта")
async def show_offer(message: Message):
    await message.answer(
        "📄 *Публичная оферта*\n\nУсловия оказания услуг, реквизиты и контактные данные:",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="Открыть оферту", url="https://docs.google.com/document/d/1ZTZsjsR7GGW6F6p8Cdfh7cWMjSF8fHy1s5mHsh4xgF8/edit?usp=sharing")
        ]])
    )

@dp.message(F.text == "🔐 Политика обработки ПДн")
async def show_privacy_policy(message: Message):
    await message.answer(
        "🔐 *Политика обработки персональных данных*\n\nПорядок сбора, хранения и передачи персональных данных:",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="Открыть политику", url="https://docs.google.com/document/d/1XAVyPmqUQunoD--PJwP24Sxlwt6NxJW069Dxrollzak/edit?usp=sharing")
        ]])
    )

@dp.message(F.text == "📞 Контакты")
async def show_contacts(message: Message):
    await message.answer(
        "📞 *Контактные данные*\n\n"
        "Исполнитель: Яковенко Андрей Алексеевич\n"
        "ИНН: 503114316367\n"
        "Адрес: г. Москва, ул. Марксистская, д. 5\n"
        "Телефон: +7 966 141-63-65\n"
        "E-mail: andreyyakovenko05@mail.ru",
        parse_mode="Markdown"
    )

@dp.message(F.text == "♈ Изменить знак зодиака")
async def change_sign(message: Message):
    user_id = message.from_user.id
    WAITING_SIGN_CHANGE[str(user_id)] = True
    msg = await message.answer("Выбери новый знак зодиака:", reply_markup=get_sign_keyboard())

@dp.message(F.contact)
async def handle_ckassa_contact(message: Message):
    await message.answer(
        "Номер телефона в боте больше не нужен. Для оплаты нажми кнопку с суммой — данные вводятся на странице Ckassa.",
        reply_markup=get_main_keyboard(),
    )

# ====== ГОЛОСОВЫЕ СООБЩЕНИЯ ======
@dp.message(F.voice)
async def handle_voice(message: Message):
    user_id = str(message.from_user.id)

    if user_id not in WAITING_TAROT_STORY and user_id not in WAITING_ASTRO_STORY and user_id not in ACTIVE_SESSIONS:
        await message.answer("Голосовые сообщения принимаются только при обращении к специалисту. Выбери таролога или астролога в разделе 🌟 Консультация")
        return

    # Антиспам для сеанса
    if user_id in ACTIVE_SESSIONS and SESSION_BUSY.get(user_id, False):
        await message.answer("⏳ Подожди, специалист ещё печатает ответ...")
        return

    await message.answer("🎤 Слушаю твоё сообщение, подожди немного...")

    voice: Voice = message.voice
    file = await bot.get_file(voice.file_id)
    file_path = f"voice_{user_id}.ogg"

    await bot.download_file(file.file_path, file_path)

    text = await transcribe_voice(file_path)

    if not text:
        try:
            os.remove(file_path)
        except:
            pass
        await message.answer(
            "😔 К сожалению, не удалось доставить голосовое сообщение до специалиста. "
            "Пожалуйста, напишите ваш вопрос текстом. Извините за неполадки!",
            reply_markup=get_cancel_keyboard()
        )
        return

    await message.answer(f"📝 Распознал твоё сообщение:\n\n_{text}_", parse_mode="Markdown")

    if user_id in ACTIVE_SESSIONS:
        try:
            os.remove(file_path)
        except:
            pass
        asyncio.create_task(send_session_reply(message.from_user.id, text))
        return

    # Сохраняем голосовое навсегда — админ сможет прослушать его в админ-боте
    saved_voice_path = os.path.join(VOICE_REQUESTS_DIR, f"req_{uuid.uuid4().hex[:8]}.ogg")
    try:
        os.replace(file_path, saved_voice_path)
    except Exception as e:
        print(f"[handle_voice] save voice: {e}")
        saved_voice_path = None
        try:
            os.remove(file_path)
        except:
            pass

    if user_id in WAITING_ASTRO_STORY:
        astro_id = WAITING_ASTRO_STORY.get(user_id)
        astrologer = ASTROLOGERS_BY_ID.get(astro_id)
        if not astrologer:
            WAITING_ASTRO_STORY.pop(user_id, None)
            await message.answer("Что-то пошло не так, попробуй снова.", reply_markup=get_main_keyboard())
            return
        if await check_incomprehensible(text):
            await message.answer(
                "⚠️ Мы не смогли понять твой запрос — сообщение похоже на бессвязный набор слов "
                "или написано на другом языке.\n\n"
                "Запиши голосовое ещё раз или напиши текстом, понятно и по-русски 👇",
                reply_markup=get_cancel_keyboard()
            )
            return
        WAITING_ASTRO_STORY.pop(user_id, None)
        is_flagged = await check_profanity(text)
        await message.answer(
            f"✅ Запрос принят! {astrologer['name']} изучит вашу натальную карту и ответит в течение 20-25 минут.",
            reply_markup=get_main_keyboard()
        )
        if is_flagged:
            await message.answer(
                "💛 Мы ценим вас и ваше время. Наша система первичной модерации обнаружила "
                "в вашем сообщении неприемлемые слова или выражения. Впредь просим воздерживаться "
                "от их использования. Тем не менее мы вас ценим как клиента, "
                f"и {astrologer['name']} всё равно ответит на ваш вопрос."
            )
        asyncio.create_task(record_consultation_request(
            message.from_user, "astro", astrologer, text,
            voice_path=saved_voice_path, is_flagged=is_flagged,
        ))
        asyncio.create_task(send_astro_answer_delayed(message.from_user.id, astrologer, text, is_flagged=is_flagged))
        return

    tarot_id = WAITING_TAROT_STORY.get(user_id)
    tarologist = TAROLOGISTS_BY_ID.get(tarot_id)

    if not tarologist:
        WAITING_TAROT_STORY.pop(user_id, None)
        await message.answer("Что-то пошло не так, попробуй снова.", reply_markup=get_main_keyboard())
        return

    if await check_incomprehensible(text):
        await message.answer(
            "⚠️ Мы не смогли понять твой запрос — сообщение похоже на бессвязный набор слов "
            "или написано на другом языке.\n\n"
            "Запиши голосовое ещё раз или напиши текстом, понятно и по-русски 👇",
            reply_markup=get_cancel_keyboard()
        )
        return

    WAITING_TAROT_STORY.pop(user_id, None)
    is_flagged = await check_profanity(text)

    await message.answer(
        f"✅ Запрос принят! {tarologist['name']} вытянет карту и ответит тебе в течение 15-20 минут.",
        reply_markup=get_main_keyboard()
    )

    if is_flagged:
        await message.answer(
            "💛 Мы ценим вас и ваше время. Наша система первичной модерации обнаружила "
            "в вашем сообщении неприемлемые слова или выражения. Впредь просим воздерживаться "
            "от их использования. Тем не менее мы вас ценим как клиента, "
            f"и {tarologist['name']} всё равно ответит на ваш вопрос."
        )

    asyncio.create_task(record_consultation_request(
        message.from_user, "tarot", tarologist, text,
        voice_path=saved_voice_path, is_flagged=is_flagged,
    ))
    asyncio.create_task(send_tarot_answer_delayed(message.from_user.id, tarologist, text, is_flagged=is_flagged))

# ====== ЗАВЕРШЕНИЕ СЕАНСА ПОЛЬЗОВАТЕЛЕМ ======
@dp.message(F.text == "🚪 Завершить сеанс")
async def end_session_manually(message: Message):
    user_id = message.from_user.id
    user_id_str = str(user_id)
    if user_id_str in ACTIVE_SESSIONS:
        tarologist_name = ACTIVE_SESSIONS[user_id_str]["tarologist"]["name"]
        del ACTIVE_SESSIONS[user_id_str]
        _save_active_sessions()
        SESSION_BUSY.pop(user_id_str, None)
        SESSION_MSG_QUEUE.pop(user_id_str, None)
        await message.answer(
            f"✨ Ты завершил сеанс с {tarologist_name}. Если появятся вопросы — возвращайся.",
            reply_markup=get_main_keyboard()
        )
    else:
        msg = await message.answer("Главное меню:", reply_markup=get_main_keyboard())

# ====== ТЕКСТОВЫЕ СООБЩЕНИЯ ======
@dp.message(F.text & ~F.text.startswith("/"))
async def handle_story(message: Message):
    user_id = str(message.from_user.id)

    # Обновляем username и имя при каждом сообщении (для пользователей без этих данных)
    if message.from_user.username or message.from_user.full_name:
        users = load_users()
        u = users.get(user_id, {})
        if not u.get("username") and not u.get("full_name"):
            u["username"] = message.from_user.username or ""
            u["full_name"] = message.from_user.full_name or ""
            users[user_id] = u
            save_users(users)

    if user_id in WAITING_PROMOCODE:
        await _handle_promo_code_input(message)
        return

    # ====== ОТВЕТ НА ЗАПРОС ОБ ОПЫТЕ КОНСУЛЬТАЦИИ (инициирован админом) ======
    feedback_entry = _load_waiting_feedback().get(user_id)
    if feedback_entry and feedback_entry.get("state", "active") == "active":
        if _feedback_is_expired(feedback_entry):
            _pop_waiting_feedback(user_id)
        else:
            feedback_text = (message.text or "").strip()
            if len(feedback_text) < 2:
                await message.answer("Напиши, пожалуйста, чуть подробнее — пара слов или больше. 💛")
                return
            count = int(feedback_entry.get("messages_count", 0)) + 1
            _update_waiting_feedback(user_id, messages_count=count)
            stype = feedback_entry.get("type", "")
            sname = feedback_entry.get("specialist_name", "—")
            type_label = "🎴 Таролог" if stype == "tarot" else "⭐ Астролог"
            user_label = (
                f"@{message.from_user.username}" if message.from_user.username
                else (message.from_user.full_name or f"ID {user_id}")
            )
            if count == 1:
                ack = (
                    "💛 Спасибо, передал админу. Если хотите дополнить — пишите ещё, "
                    "сколько нужно. Когда закончите, нажмите «✅ Завершить отзыв»."
                )
            else:
                ack = "💛 Принял. Можно дописать или закрыть отзыв кнопкой ниже."
            await message.answer(ack, reply_markup=_feedback_action_keyboard())
            asyncio.create_task(notify_admin(
                f"💌 Отзыв о консультации (сообщение #{count})\n\n"
                f"👤 {user_label} [ID {user_id}]\n"
                f"{type_label}: {sname}\n"
                f"🕐 {datetime.now().strftime('%d.%m.%Y %H:%M')}\n\n"
                f"💬 Ответ пользователя:\n{feedback_text}"
            ))
            return

    # ====== ПОТОК ОТЗЫВА ======
    if user_id in WAITING_REVIEW:
        state = WAITING_REVIEW[user_id]
        step = state.get("step")

        if step == "name":
            name = message.text.strip()
            if len(name) < 2 or len(name) > 50:
                await message.answer("Пожалуйста, введи нормальное имя (от 2 до 50 символов):")
                return
            state["name"] = name
            state["step"] = "text"
            await message.answer(
                f"Приятно познакомиться, {name}! Теперь напиши свой отзыв:",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(text="❌ Отмена", callback_data="rev_cancel")
                ]])
            )
            return

        if step == "text":
            text = message.text.strip()
            if len(text) < 10:
                await message.answer("Отзыв слишком короткий. Напиши хотя бы пару предложений:")
                return
            if len(text) > 1000:
                await message.answer("Отзыв слишком длинный (максимум 1000 символов). Сократи немного:")
                return
            review_data = WAITING_REVIEW.pop(user_id)
            review_id = uuid.uuid4().hex[:10]
            tg_username = review_data.get("tg_username")
            if review_data.get("anonymous"):
                author = "Анонимный пользователь"
            elif tg_username:
                author = f"@{tg_username} ({review_data['name']})"
            else:
                author = review_data["name"]
            new_review = {
                "author": author,
                "tag": review_data["topic"],
                "text": text,
            }
            pending = load_pending_reviews()
            pending[review_id] = new_review
            save_pending_reviews(pending)
            await message.answer(
                "✨ *Спасибо за отзыв!*\n\n"
                "Он отправлен на модерацию — обычно это занимает не больше суток. "
                "После одобрения он появится в разделе «Отзывы» для всех пользователей. 🌟",
                parse_mode="Markdown",
                reply_markup=get_main_keyboard()
            )
            track_activity(user_id, "review")
            asyncio.create_task(send_review_notification(review_id, new_review))
            return
        return

    # Если активен сеанс — обрабатываем как допвопрос
    if user_id in ACTIVE_SESSIONS:
        # Антиспам: если бот ещё печатает ответ
        if SESSION_BUSY.get(user_id, False):
            # Не отвечаем ничего, просто складываем в очередь
            SESSION_MSG_QUEUE[user_id] = message.text
            return
        asyncio.create_task(send_session_reply(message.from_user.id, message.text))
        return

    if user_id in WAITING_ASTRO_STORY:
        astro_id = WAITING_ASTRO_STORY.get(user_id)
        astrologer = ASTROLOGERS_BY_ID.get(astro_id)
        if not astrologer:
            WAITING_ASTRO_STORY.pop(user_id, None)
            await message.answer("Что-то пошло не так, попробуй снова.", reply_markup=get_main_keyboard())
            return
        if await check_incomprehensible(message.text):
            await message.answer(
                "⚠️ Мы не смогли понять твой запрос — сообщение похоже на бессвязный набор слов "
                "или написано на другом языке.\n\n"
                "Опиши свою ситуацию по-русски, одним понятным сообщением 👇",
                reply_markup=get_cancel_keyboard()
            )
            return
        WAITING_ASTRO_STORY.pop(user_id, None)
        is_flagged = await check_profanity(message.text)
        await message.answer(
            f"✅ Запрос принят! {astrologer['name']} изучит вашу натальную карту и ответит в течение 20-25 минут.",
            reply_markup=get_main_keyboard()
        )
        if is_flagged:
            await message.answer(
                "💛 Мы ценим вас и ваше время. Наша система первичной модерации обнаружила "
                "в вашем сообщении неприемлемые слова или выражения. Впредь просим воздерживаться "
                "от их использования. Тем не менее мы вас ценим как клиента, "
                f"и {astrologer['name']} всё равно ответит на ваш вопрос."
            )
        asyncio.create_task(record_consultation_request(
            message.from_user, "astro", astrologer, message.text, is_flagged=is_flagged,
        ))
        asyncio.create_task(send_astro_answer_delayed(message.from_user.id, astrologer, message.text, is_flagged=is_flagged))
        return

    if user_id not in WAITING_TAROT_STORY:
        # Любое нераспознанное сообщение — просто показываем главное меню,
        # чтобы панель с кнопками всегда была на виду и не требовала /start.
        users = load_users()
        if users.get(user_id, {}).get("sign"):
            await message.answer("Главное меню:", reply_markup=get_main_keyboard())
        else:
            await message.answer(
                "Привет! Я — Голос Звёзд 🌟\n\n"
                "Выбери специалиста — таролог или астролог. "
                "Или нажми «🏠 Главное меню», чтобы посмотреть прогноз по знаку зодиака.",
                reply_markup=get_consultations_keyboard()
            )
        return

    tarot_id = WAITING_TAROT_STORY.get(user_id)
    tarologist = TAROLOGISTS_BY_ID.get(tarot_id)

    if not tarologist:
        WAITING_TAROT_STORY.pop(user_id, None)
        await message.answer("Что-то пошло не так, попробуй снова.", reply_markup=get_main_keyboard())
        return

    if await check_incomprehensible(message.text):
        await message.answer(
            "⚠️ Мы не смогли понять твой запрос — сообщение похоже на бессвязный набор слов "
            "или написано на другом языке.\n\n"
            "Опиши свою ситуацию по-русски, одним понятным сообщением 👇",
            reply_markup=get_cancel_keyboard()
        )
        return

    WAITING_TAROT_STORY.pop(user_id, None)
    is_flagged = await check_profanity(message.text)

    await message.answer(
        f"✅ Запрос принят! {tarologist['name']} вытянет карту и ответит тебе в течение 15-20 минут.",
        reply_markup=get_main_keyboard()
    )

    if is_flagged:
        await message.answer(
            "💛 Мы ценим вас и ваше время. Наша система первичной модерации обнаружила "
            "в вашем сообщении неприемлемые слова или выражения. Впредь просим воздерживаться "
            "от их использования. Тем не менее мы вас ценим как клиента, "
            f"и {tarologist['name']} всё равно ответит на ваш вопрос."
        )

    asyncio.create_task(record_consultation_request(
        message.from_user, "tarot", tarologist, message.text, is_flagged=is_flagged,
    ))
    asyncio.create_task(send_tarot_answer_delayed(message.from_user.id, tarologist, message.text, is_flagged=is_flagged))

# ====== HEARTBEAT ======
_bot_started_at = _msk_now()

async def heartbeat():
    """Ежедневный отчёт о здоровье бота в 12:00 МСК и проверка каждые 30 минут."""
    last_daily_report = None
    while True:
        try:
            msk = _msk_now()
            uptime = msk - _bot_started_at
            hours = int(uptime.total_seconds() // 3600)
            minutes = int((uptime.total_seconds() % 3600) // 60)

            # Ежедневный отчёт в 12:00 МСК
            if msk.hour == 12 and last_daily_report != msk.date():
                last_daily_report = msk.date()
                users = load_users()
                text = (
                    f"📊 Ежедневный отчёт:\n"
                    f"• Аптайм: {hours}ч {minutes}м\n"
                    f"• Пользователей: {len(users)}\n"
                    f"• Статус: работает ✅"
                )
                await notify_admin(text)
        except Exception as e:
            print(f"[heartbeat] Ошибка: {e}")

        await asyncio.sleep(1800)  # проверка каждые 30 минут

# ====== ЗАПУСК ======
async def on_startup(bot):
    global MAIN_BOT_USERNAME, MAIN_BOT_URL
    try:
        me = await bot.get_me()
        if me.username:
            MAIN_BOT_USERNAME = me.username.lstrip("@")
            MAIN_BOT_URL = f"https://t.me/{MAIN_BOT_USERNAME}"
    except Exception as e:
        print(f"[on_startup] failed to resolve bot username: {e}")
    await notify_admin("🔄 Бот перезапущен и работает.")
    await check_channel_publish_access(notify=True)


async def _restore_pending_answers() -> None:
    """При старте — пересоздаём таски send_*_answer_delayed для не отправленных запросов.
    Если дедлайн уже прошёл — таск отправит ответ сразу."""
    items = _load_pending_answers()
    restored = 0
    for entry in items:
        try:
            stype = entry.get("type")
            specialist_id = entry.get("specialist_id")
            if stype == "astro":
                specialist = ASTROLOGERS_BY_ID.get(specialist_id)
                fn = send_astro_answer_delayed
            elif stype == "tarot":
                specialist = TAROLOGISTS_BY_ID.get(specialist_id)
                fn = send_tarot_answer_delayed
            else:
                continue
            if not specialist:
                continue
            kwargs = {
                "is_flagged": entry.get("is_flagged", False),
                "pending_id": entry["id"],
                "deadline_ts": entry.get("deadline_ts", 0),
            }
            if stype == "tarot":
                kwargs["selected_card"] = entry.get("selected_tarot_card")
            asyncio.create_task(fn(entry["user_id"], specialist, entry["user_story"], **kwargs))
            restored += 1
        except Exception as e:
            print(f"[_restore_pending_answers] skip entry: {e}")
    if restored:
        print(f"[_restore_pending_answers] возобновлено отложенных ответов: {restored}")


async def _restore_active_sessions() -> None:
    """При старте — восстанавливаем ACTIVE_SESSIONS из файла.
    Просроченные сеансы — уведомляем пользователя один раз и не восстанавливаем."""
    saved = _load_active_sessions_from_disk()
    if not saved:
        return
    now = time.time()
    expired = []
    restored = 0
    for uid, s in saved.items():
        try:
            stype = s.get("type", "tarot")
            sid = s.get("specialist_id")
            if stype == "astro":
                specialist = ASTROLOGERS_BY_ID.get(sid)
            else:
                specialist = TAROLOGISTS_BY_ID.get(sid)
            if not specialist:
                continue
            expires_at = s.get("expires_at", 0)
            if expires_at <= now:
                expired.append((uid, specialist, stype))
                continue
            ACTIVE_SESSIONS[uid] = {
                "type": stype,
                "tarologist": specialist,
                "history": s.get("history", []),
                "msg_count": s.get("msg_count", 0),
                "profanity_count": s.get("profanity_count", 0),
                "selected_tarot_card": s.get("selected_tarot_card"),
                "anecdote_allowed": s.get("anecdote_allowed", False),
                "anecdote_used": s.get("anecdote_used", False),
                "expires_at": expires_at,
            }
            SESSION_BUSY[uid] = False
            asyncio.create_task(session_timeout(int(uid), delay=expires_at - now))
            restored += 1
        except Exception as e:
            print(f"[_restore_active_sessions] skip {uid}: {e}")
    # Перезаписываем файл, удаляя просроченные и несовместимые записи
    _save_active_sessions()
    # Уведомляем пользователей, чей сеанс истёк во время простоя бота
    for uid, specialist, stype in expired:
        try:
            if stype == "astro":
                text = (f"⏰ Пока бот был недоступен, время сеанса с {specialist['name']} истекло.\n"
                        "Если хочешь продолжить — выбери астролога в меню 🌟")
            else:
                text = (f"⏰ Пока бот был недоступен, время сеанса с {specialist['name']} истекло.\n"
                        "Если хочешь продолжить — выбери таролога в меню 🎴")
            await bot.send_message(int(uid), text, reply_markup=get_main_keyboard())
        except Exception as e:
            print(f"[_restore_active_sessions] notify {uid}: {e}")
    if restored:
        print(f"[_restore_active_sessions] восстановлено сеансов: {restored}")


async def run_background_task(name: str, task_factory):
    while True:
        try:
            await task_factory()
            print(f"[background] {name} returned; restarting in 60 seconds")
            await notify_admin(f"Background task returned: {name}. Restarting in 60 seconds.")
        except asyncio.CancelledError:
            raise
        except Exception as e:
            clean_error = _redact_channel_publish_error(str(e))
            print(f"[background] {name} crashed: {clean_error}")
            await notify_admin(f"Background task crashed: {name}\n{clean_error}\nRestarting in 60 seconds.")
        await asyncio.sleep(60)


async def main():
    asyncio.create_task(run_background_task("scheduler", scheduler))
    asyncio.create_task(heartbeat())
    asyncio.create_task(ckassa_payment_watcher())
    await on_startup(bot)
    await credit_uncredited_paid_orders(notify_user=True)
    await _restore_active_sessions()
    await _restore_pending_answers()
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
