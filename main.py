import asyncio
import json
import re
from datetime import datetime, timedelta

import aiohttp
from aiogram import Bot, Dispatcher, F
from aiogram.types import Message, ReplyKeyboardMarkup, KeyboardButton
from dotenv import load_dotenv
from os import getenv

load_dotenv()  # загружает переменные из .env файла

# ====== Токены ======
TOKEN = getenv("BOT_TOKEN")
OPENROUTER_KEY = getenv("OPENROUTER_KEY")

bot = Bot(token=TOKEN)
dp = Dispatcher()

# ====== ФАЙЛЫ ======
USERS_FILE = "users.json"
FORECAST_FILE = "forecast.json"
DESCRIPTIONS_FILE = "descriptions.json"

# ====== ЯЗЫКИ ======
LANGUAGES = {"Русский": "ru", "English": "en"}

# ====== ЗНАКИ ЗОДИАКА ======
SIGNS = {
    "ru": ["Овен", "Телец", "Близнецы", "Рак",
           "Лев", "Дева", "Весы", "Скорпион",
           "Стрелец", "Козерог", "Водолей", "Рыбы"],
    "en": ["Aries", "Taurus", "Gemini", "Cancer",
           "Leo", "Virgo", "Libra", "Scorpio",
           "Sagittarius", "Capricorn", "Aquarius", "Pisces"]
}

# ====== СОСТОЯНИЯ ======
WAITING_LANGUAGE_CHANGE = {}
WAITING_SIGN_CHANGE = {}

# ====== КНОПКИ ======
def get_language_keyboard():
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text=lang)] for lang in LANGUAGES.keys()],
        resize_keyboard=True
    )

def get_sign_keyboard(lang_code="ru"):
    rows = []
    signs = SIGNS[lang_code]
    for i in range(0, len(signs), 2):
        row = [KeyboardButton(text=signs[i])]
        if i + 1 < len(signs):
            row.append(KeyboardButton(text=signs[i + 1]))
        rows.append(row)
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True)

def get_main_keyboard(lang_code="ru"):
    if lang_code == "ru":
        buttons = [
            [KeyboardButton(text="🔮 Посмотреть прогноз")],
            [KeyboardButton(text="📖 Читать о себе")],
            [KeyboardButton(text="⚙️ Настройки")]
        ]
    else:
        buttons = [
            [KeyboardButton(text="🔮 View Forecast")],
            [KeyboardButton(text="📖 Read About Me")],
            [KeyboardButton(text="⚙️ Settings")]
        ]
    return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)

def get_settings_keyboard(lang_code="ru"):
    if lang_code == "ru":
        buttons = [
            [KeyboardButton(text="🌍 Изменить язык")],
            [KeyboardButton(text="♈ Изменить знак зодиака")],
            [KeyboardButton(text="◀️ Назад")]
        ]
    else:
        buttons = [
            [KeyboardButton(text="🌍 Change language")],
            [KeyboardButton(text="♈ Change zodiac sign")],
            [KeyboardButton(text="◀️ Back")]
        ]
    return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)

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

# ====== БЕЗОПАСНЫЙ РАЗБОР JSON ======
def extract_json_from_text(text: str):
    try:
        match = re.search(r"\{.*\}", text, re.DOTALL)
        if match:
            return json.loads(match.group())
    except json.JSONDecodeError:
        pass
    return {}

# ====== ПОЛУЧЕНИЕ ГОРОСКОПА ======
async def get_horoscope(lang="ru"):
    url = "https://openrouter.ai/api/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {OPENROUTER_KEY}",
        "Content-Type": "application/json"
    }

    if lang == "ru":
        prompt = """
Сделай гороскоп на следующие 24 часа для всех 12 знаков зодиака.
Каждый знак должен получить 2-3 предложения, которые дают понимание, что его ждет сегодня и как лучше действовать (например, чего избегать или на что обратить внимание), но без жестких инструкций.
Ответ строго в JSON формате без лишнего текста, вот структура:
{
"Овен": "...",
"Телец": "...",
"Близнецы": "...",
"Рак": "...",
"Лев": "...",
"Дева": "...",
"Весы": "...",
"Скорпион": "...",
"Стрелец": "...",
"Козерог": "...",
"Водолей": "...",
"Рыбы": "..."
}
"""
    else:
        prompt = """
Make a horoscope for the next 24 hours for all 12 zodiac signs.
Each sign should have 2-3 sentences giving insight into what to expect today and general advice (like what to avoid or notice), but without strict instructions.
Answer strictly in JSON format without extra text, using this structure:
{
"Aries": "...",
"Taurus": "...",
"Gemini": "...",
"Cancer": "...",
"Leo": "...",
"Virgo": "...",
"Libra": "...",
"Scorpio": "...",
"Sagittarius": "...",
"Capricorn": "...",
"Aquarius": "...",
"Pisces": "..."
}
"""

    data = {
        "model": "deepseek/deepseek-chat",
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": 1500
    }

    async with aiohttp.ClientSession() as session:
        async with session.post(url, headers=headers, json=data) as resp:
            try:
                response_json = await resp.json(content_type=None)
            except Exception as e:
                print("Ошибка парсинга ответа:", e)
                return {}

            try:
                content = response_json["choices"][0]["message"]["content"]
            except (KeyError, IndexError) as e:
                if "error" in response_json:
                    print("Ошибка API:", response_json["error"]["message"])
                else:
                    print("Ошибка извлечения content:", e)
                    print("Полный ответ:", response_json)
                return {}

            print(f"Контент от модели ({lang}):", content)
            content = content.replace("```json", "").replace("```", "").strip()

            result = extract_json_from_text(content)
            if not result:
                print(f"Не удалось извлечь JSON из контента ({lang})")
            return result

# ====== ПОЛУЧЕНИЕ ОПИСАНИЯ ЗНАКА ======
async def get_sign_description(sign: str, lang: str) -> str:
    url = "https://openrouter.ai/api/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {OPENROUTER_KEY}",
        "Content-Type": "application/json"
    }

    if lang == "ru":
        prompt = f"""
Напиши подробное и глубокое описание знака зодиака {sign}.
Структура описания:
- Общая характеристика и суть знака
- Характер и личность: сильные стороны и слабости
- Отношения и любовь
- Карьера и призвание
- Здоровье и энергетика
- Интересные факты и особенности

Пиши живым, тёплым языком, без шаблонов. Объём — не менее 400 слов.
Только текст, без JSON, без заголовков со звёздочками, можно использовать эмодзи.
"""
    else:
        prompt = f"""
Write a detailed and deep description of the zodiac sign {sign}.
Structure:
- General character and essence of the sign
- Personality: strengths and weaknesses
- Relationships and love
- Career and calling
- Health and energy
- Interesting facts and traits

Write in a warm, vivid style, no clichés. At least 400 words.
Plain text only, no JSON, no markdown headers, emojis are welcome.
"""

    data = {
        "model": "deepseek/deepseek-chat",
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": 1500
    }

    async with aiohttp.ClientSession() as session:
        async with session.post(url, headers=headers, json=data) as resp:
            try:
                response_json = await resp.json(content_type=None)
                content = response_json["choices"][0]["message"]["content"]
                return content.strip()
            except Exception as e:
                if "error" in response_json:
                    print("Ошибка API:", response_json["error"]["message"])
                else:
                    print("Ошибка описания знака:", e)
                return ""

# ====== УТРЕННЕЕ УВЕДОМЛЕНИЕ ======
async def get_morning_message(sign: str, lang: str) -> str:
    url = "https://openrouter.ai/api/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {OPENROUTER_KEY}",
        "Content-Type": "application/json"
    }

    if lang == "ru":
        prompt = f"""
Напиши короткое утреннее астрологическое напоминание для знака зодиака {sign}.
1-2 предложения, тёплым и вдохновляющим тоном, без клише.
Только текст, без JSON, без лишних символов.
"""
    else:
        prompt = f"""
Write a short morning astrological reminder for the zodiac sign {sign}.
1-2 sentences, warm and inspiring tone, no clichés.
Just plain text, no JSON, no extra symbols.
"""

    data = {
        "model": "deepseek/deepseek-chat",
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": 200
    }

    async with aiohttp.ClientSession() as session:
        async with session.post(url, headers=headers, json=data) as resp:
            try:
                response_json = await resp.json(content_type=None)
                content = response_json["choices"][0]["message"]["content"]
                return content.strip()
            except Exception as e:
                print("Ошибка утреннего сообщения:", e)
                return ""

# ====== ОБНОВЛЕНИЕ ПРОГНОЗА ======
async def update_forecast():
    print(f"[{datetime.now()}] Обновляю прогноз...")
    try:
        forecast_ru = await get_horoscope("ru")
        forecast_en = await get_horoscope("en")
        if forecast_ru and forecast_en:
            save_forecast({"ru": forecast_ru, "en": forecast_en})
            print("Прогноз обновлён!")
        else:
            print("Прогноз не обновлён, пустой ответ API")
    except Exception as e:
        print("Ошибка при обновлении прогноза:", e)

# ====== УТРЕННИЕ УВЕДОМЛЕНИЯ ======
async def send_morning_notifications():
    print(f"[{datetime.now()}] Отправляю утренние уведомления...")
    users = load_users()
    for user_id, user_data in users.items():
        if "language" not in user_data or "sign" not in user_data:
            continue
        lang = user_data["language"]
        sign = user_data["sign"]
        try:
            msg = await get_morning_message(sign, lang)
            if msg:
                if lang == "ru":
                    text = f"🌅 Доброе утро!\n\n🔮 {sign}:\n{msg}"
                else:
                    text = f"🌅 Good morning!\n\n🔮 {sign}:\n{msg}"
                await bot.send_message(int(user_id), text)
        except Exception as e:
            print(f"Ошибка отправки уведомления пользователю {user_id}:", e)

# ====== ПЛАНИРОВЩИК ======
async def scheduler():
    # При старте — сразу обновляем прогноз если файла нет
    forecast = load_forecast()
    if not forecast:
        await update_forecast()

    forecast_updated_date = None
    morning_sent_date = None

    while True:
        now = datetime.now()
        today = now.date()

        # Обновление прогноза ровно в полночь (00:00)
        if now.hour == 0 and now.minute == 0 and forecast_updated_date != today:
            forecast_updated_date = today
            await update_forecast()

        # Утренние уведомления ровно в 8:00
        if now.hour == 8 and now.minute == 0 and morning_sent_date != today:
            morning_sent_date = today
            await send_morning_notifications()

        # Спим до следующей минуты
        next_minute = (now + timedelta(minutes=1)).replace(second=0, microsecond=0)
        sleep_seconds = (next_minute - now).total_seconds()
        await asyncio.sleep(sleep_seconds)

# ====== ОБРАБОТЧИКИ ======
@dp.message(F.text == "/start")
async def start(message: Message):
    users = load_users()
    user_id = str(message.from_user.id)
    if user_id in users and "language" in users[user_id] and "sign" in users[user_id]:
        lang = users[user_id]["language"]
        sign = users[user_id]["sign"]
        if lang == "ru":
            await message.answer(
                f"С возвращением! Твой знак: {sign}.",
                reply_markup=get_main_keyboard(lang)
            )
        else:
            await message.answer(
                f"Welcome back! Your sign: {sign}.",
                reply_markup=get_main_keyboard(lang)
            )
    else:
        await message.answer(
            "Выбери язык / Choose language:",
            reply_markup=get_language_keyboard()
        )

# Выбор языка
@dp.message(F.text.in_(LANGUAGES.keys()))
async def set_language(message: Message):
    user_id = str(message.from_user.id)

    if user_id in WAITING_LANGUAGE_CHANGE:
        del WAITING_LANGUAGE_CHANGE[user_id]
        users = load_users()
        users.setdefault(user_id, {})["language"] = LANGUAGES[message.text]
        save_users(users)
        lang_code = LANGUAGES[message.text]
        if lang_code == "ru":
            await message.answer("Язык изменён!", reply_markup=get_main_keyboard(lang_code))
        else:
            await message.answer("Language changed!", reply_markup=get_main_keyboard(lang_code))
        return

    users = load_users()
    users.setdefault(user_id, {})["language"] = LANGUAGES[message.text]
    save_users(users)
    lang_code = LANGUAGES[message.text]
    await message.answer(
        "Выбери свой знак зодиака:" if lang_code == "ru" else "Choose your zodiac sign:",
        reply_markup=get_sign_keyboard(lang_code)
    )

# Выбор знака зодиака
@dp.message(F.text.in_(SIGNS["ru"] + SIGNS["en"]))
async def set_sign(message: Message):
    users = load_users()
    user_id = str(message.from_user.id)
    user_data = users.get(user_id)

    if not user_data or "language" not in user_data:
        await message.answer("Сначала выбери язык / First choose language",
                             reply_markup=get_language_keyboard())
        return

    lang_code = user_data["language"]
    users[user_id]["sign"] = message.text
    save_users(users)

    if user_id in WAITING_SIGN_CHANGE:
        del WAITING_SIGN_CHANGE[user_id]
        if lang_code == "ru":
            await message.answer(f"Знак зодиака изменён на {message.text}!", reply_markup=get_main_keyboard(lang_code))
        else:
            await message.answer(f"Zodiac sign changed to {message.text}!", reply_markup=get_main_keyboard(lang_code))
        return

    await message.answer(
        "Твой знак сохранён! Можешь смотреть прогноз." if lang_code == "ru" else "Your sign is saved! You can view your forecast.",
        reply_markup=get_main_keyboard(lang_code)
    )

# Просмотр прогноза
@dp.message(F.text.in_(["🔮 Посмотреть прогноз", "🔮 View Forecast"]))
async def send_forecast(message: Message):
    users = load_users()
    user_id = str(message.from_user.id)
    user_data = users.get(user_id)

    if not user_data or "language" not in user_data or "sign" not in user_data:
        await message.answer("Сначала выбери язык и знак зодиака / First choose language and zodiac sign",
                             reply_markup=get_language_keyboard())
        return

    lang = user_data["language"]
    sign = user_data["sign"]

    forecast_data = load_forecast()
    if lang not in forecast_data or sign not in forecast_data[lang]:
        await message.answer("Прогноз ещё не готов, попробуй позже" if lang == "ru" else "Forecast is not ready yet, try later")
        return

    await message.answer(f"🔮 {sign}:\n{forecast_data[lang][sign]}")

# Читать о себе
@dp.message(F.text.in_(["📖 Читать о себе", "📖 Read About Me"]))
async def read_about_me(message: Message):
    users = load_users()
    user_id = str(message.from_user.id)
    user_data = users.get(user_id)

    if not user_data or "language" not in user_data or "sign" not in user_data:
        await message.answer("Сначала выбери язык и знак зодиака / First choose language and zodiac sign",
                             reply_markup=get_language_keyboard())
        return

    lang = user_data["language"]
    sign = user_data["sign"]

    descriptions = load_descriptions()
    cache_key = f"{sign}_{lang}"

    if cache_key in descriptions:
        await message.answer(f"📖 {sign}\n\n{descriptions[cache_key]}")
        return

    await message.answer("⏳ Составляю описание твоего знака..." if lang == "ru" else "⏳ Generating your sign description...")

    description = await get_sign_description(sign, lang)

    if not description:
        await message.answer(
            "Не удалось получить описание, попробуй позже." if lang == "ru" else "Could not get description, try later."
        )
        return

    descriptions[cache_key] = description
    save_descriptions(descriptions)

    await message.answer(f"📖 {sign}\n\n{description}")

# Настройки
@dp.message(F.text.in_(["⚙️ Настройки", "⚙️ Settings"]))
async def settings(message: Message):
    users = load_users()
    user_id = str(message.from_user.id)
    user_data = users.get(user_id, {})
    lang = user_data.get("language", "ru")
    await message.answer(
        "⚙️ Настройки:" if lang == "ru" else "⚙️ Settings:",
        reply_markup=get_settings_keyboard(lang)
    )

# Изменить язык
@dp.message(F.text.in_(["🌍 Изменить язык", "🌍 Change language"]))
async def change_language(message: Message):
    user_id = str(message.from_user.id)
    WAITING_LANGUAGE_CHANGE[user_id] = True
    await message.answer(
        "Выбери новый язык / Choose new language:",
        reply_markup=get_language_keyboard()
    )

# Изменить знак
@dp.message(F.text.in_(["♈ Изменить знак зодиака", "♈ Change zodiac sign"]))
async def change_sign(message: Message):
    users = load_users()
    user_id = str(message.from_user.id)
    lang = users.get(user_id, {}).get("language", "ru")
    WAITING_SIGN_CHANGE[user_id] = True
    await message.answer(
        "Выбери новый знак зодиака:" if lang == "ru" else "Choose your new zodiac sign:",
        reply_markup=get_sign_keyboard(lang)
    )

# Назад
@dp.message(F.text.in_(["◀️ Назад", "◀️ Back"]))
async def go_back(message: Message):
    users = load_users()
    user_id = str(message.from_user.id)
    lang = users.get(user_id, {}).get("language", "ru")
    await message.answer(
        "Главное меню:" if lang == "ru" else "Main menu:",
        reply_markup=get_main_keyboard(lang)
    )

# ====== ЗАПУСК ======
async def main():
    asyncio.create_task(scheduler())
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())