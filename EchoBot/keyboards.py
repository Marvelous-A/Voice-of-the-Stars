from __future__ import annotations

from aiogram.types import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardMarkup,
)
from aiogram.utils.keyboard import InlineKeyboardBuilder

from texts import INTERESTS, REPORT_REASONS
from subscriptions import PLANS


SEARCH_BUTTON = "📡 Подать сигнал"
INTERESTS_BUTTON = "〰️ Мои интересы"
SETTINGS_BUTTON = "⚙️ Настройки"
PREMIUM_BUTTON = "💎 Premium и VIP"
NEXT_BUTTON = "🔄 Другое эхо"
STOP_BUTTON = "🔇 Завершить связь"
SHARE_BUTTON = "👤 Открыть профиль"
REPORT_BUTTON = "🛡 Пожаловаться"
CALL_BUTTON = "📞 Анонимный звонок"
CANCEL_SEARCH_BUTTON = "🔇 Отозвать сигнал"


def main_menu() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=SEARCH_BUTTON)],
            [
                KeyboardButton(text=INTERESTS_BUTTON),
                KeyboardButton(text=SETTINGS_BUTTON),
            ],
            [KeyboardButton(text=PREMIUM_BUTTON)],
        ],
        resize_keyboard=True,
        is_persistent=True,
        input_field_placeholder="Отправь мысль. Получи отклик.",
    )


def search_menu() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text=CANCEL_SEARCH_BUTTON)]],
        resize_keyboard=True,
        is_persistent=True,
        input_field_placeholder="Слушаем эфир…",
    )


def chat_menu() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=NEXT_BUTTON), KeyboardButton(text=STOP_BUTTON)],
            [KeyboardButton(text=CALL_BUTTON)],
            [KeyboardButton(text=SHARE_BUTTON), KeyboardButton(text=REPORT_BUTTON)],
            [KeyboardButton(text=PREMIUM_BUTTON)],
        ],
        resize_keyboard=True,
        is_persistent=True,
        input_field_placeholder="Напиши собеседнику…",
    )


def call_keyboard(url: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📞 Войти в звонок", url=url)],
        ]
    )


def age_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Мне уже есть 18", callback_data="age:accept")],
            [InlineKeyboardButton(text="Мне нет 18", callback_data="age:decline")],
        ]
    )


def registration_gender_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="Мужской", callback_data="reg_gender:male"),
                InlineKeyboardButton(text="Женский", callback_data="reg_gender:female"),
            ],
            [InlineKeyboardButton(text="Не указывать", callback_data="reg_gender:unknown")],
        ]
    )


def interests_keyboard(selected: set[str]) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for code, label in INTERESTS.items():
        mark = "✓ " if code in selected else ""
        builder.button(text=f"{mark}{label}", callback_data=f"interest:{code}")
    builder.adjust(2)
    builder.row(
        InlineKeyboardButton(text="Сбросить всё", callback_data="interests:clear"),
        InlineKeyboardButton(text="Готово", callback_data="interests:done"),
    )
    return builder.as_markup()


def settings_keyboard(user: dict, has_premium: bool = False) -> InlineKeyboardMarkup:
    gender = {"male": "мужской", "female": "женский", None: "не указан"}.get(
        user.get("gender"), "не указан"
    )
    preferred = {
        "any": "любой",
        "male": "мужской",
        "female": "женский",
    }.get(user.get("preferred_gender"), "любой") if has_premium else "любой 🔒"
    blur = "включено" if user.get("blur_media") else "выключено"
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=f"Мой пол: {gender}", callback_data="settings:gender")],
            [
                InlineKeyboardButton(
                    text=f"Ищу: {preferred}", callback_data="settings:preferred_gender"
                )
            ],
            [
                InlineKeyboardButton(
                    text=f"Скрытие медиа: {blur}", callback_data="settings:blur"
                )
            ],
            [InlineKeyboardButton(text="Удалить мои данные", callback_data="settings:delete")],
            [InlineKeyboardButton(text="Закрыть", callback_data="settings:close")],
        ]
    )


def subscription_keyboard() -> InlineKeyboardMarkup:
    rows = []
    for plan in PLANS.values():
        marker = "👑" if plan.tier == "vip" else "💎"
        rows.append(
            [
                InlineKeyboardButton(
                    text=f"{marker} {plan.title} — {plan.price_rubles} ₽",
                    callback_data=f"subscription:buy:{plan.code}",
                )
            ]
        )
    rows.append([InlineKeyboardButton(text="Закрыть", callback_data="subscription:close")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def payment_keyboard(
    invoice_url: str, order_id: str, amount_rubles: int
) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text=f"💳 Оплатить {amount_rubles} ₽", url=invoice_url
                )
            ],
            [
                InlineKeyboardButton(
                    text="✅ Проверить оплату",
                    callback_data=f"subscription:check:{order_id}",
                )
            ],
            [
                InlineKeyboardButton(
                    text="← Другой тариф", callback_data="subscription:show"
                )
            ],
        ]
    )


def ad_keyboard(url: str, button_text: str) -> InlineKeyboardMarkup | None:
    if not url.startswith(("http://", "https://")):
        return None
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text=button_text, url=url)]]
    )


def gender_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="Мужской", callback_data="set_gender:male"),
                InlineKeyboardButton(text="Женский", callback_data="set_gender:female"),
            ],
            [InlineKeyboardButton(text="Не указывать", callback_data="set_gender:unknown")],
            [InlineKeyboardButton(text="Назад", callback_data="settings:back")],
        ]
    )


def preferred_gender_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Любой", callback_data="set_preferred:any")],
            [
                InlineKeyboardButton(text="Мужской", callback_data="set_preferred:male"),
                InlineKeyboardButton(text="Женский", callback_data="set_preferred:female"),
            ],
            [InlineKeyboardButton(text="Назад", callback_data="settings:back")],
        ]
    )


def share_confirmation_keyboard(dialog_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="Да, открыть профиль", callback_data=f"share:{dialog_id}"
                )
            ],
            [InlineKeyboardButton(text="Отмена", callback_data="share:cancel")],
        ]
    )


def after_chat_keyboard(dialog_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="👍", callback_data=f"rate:{dialog_id}:up"),
                InlineKeyboardButton(text="👎", callback_data=f"rate:{dialog_id}:down"),
            ],
            [
                InlineKeyboardButton(
                    text="🛡 Пожаловаться", callback_data=f"report:{dialog_id}"
                )
            ],
        ]
    )


def report_reasons_keyboard(dialog_id: int) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for code, label in REPORT_REASONS.items():
        builder.button(text=label, callback_data=f"report_reason:{dialog_id}:{code}")
    builder.adjust(1)
    builder.row(InlineKeyboardButton(text="Отмена", callback_data="report:cancel"))
    return builder.as_markup()


def delete_confirmation_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Удалить безвозвратно", callback_data="delete:confirm")],
            [InlineKeyboardButton(text="Отмена", callback_data="delete:cancel")],
        ]
    )
