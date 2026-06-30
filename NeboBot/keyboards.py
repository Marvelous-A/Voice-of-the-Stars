from __future__ import annotations

from math import ceil

from aiogram.types import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardMarkup,
)

from models import Region


BTN_STATS = "📊 Что сейчас"
BTN_OVERVIEW = "🗺 По России"
BTN_REGION = "📍 Сменить регион"
BTN_NOTIFICATIONS = "🔔 Уведомления"
BTN_SAFETY = "🛡 Безопасность"
BTN_SOURCES = "📡 Источники"
REGIONS_PER_PAGE = 8


def main_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=BTN_STATS), KeyboardButton(text=BTN_OVERVIEW)],
            [KeyboardButton(text=BTN_NOTIFICATIONS), KeyboardButton(text=BTN_REGION)],
            [KeyboardButton(text=BTN_SAFETY), KeyboardButton(text=BTN_SOURCES)],
        ],
        resize_keyboard=True,
        input_field_placeholder="Выберите действие",
    )


def regions_keyboard(
    regions: tuple[Region, ...] | list[Region], page: int = 0
) -> InlineKeyboardMarkup:
    total_pages = max(1, ceil(len(regions) / REGIONS_PER_PAGE))
    page = max(0, min(page, total_pages - 1))
    start = page * REGIONS_PER_PAGE
    visible = regions[start : start + REGIONS_PER_PAGE]

    rows = [
        [
            InlineKeyboardButton(
                text=region.name,
                callback_data=f"region:pick:{region.id}",
            )
        ]
        for region in visible
    ]

    navigation = []
    if page > 0:
        navigation.append(
            InlineKeyboardButton(text="←", callback_data=f"region:page:{page - 1}")
        )
    navigation.append(
        InlineKeyboardButton(
            text=f"{page + 1}/{total_pages}", callback_data="region:noop"
        )
    )
    if page + 1 < total_pages:
        navigation.append(
            InlineKeyboardButton(text="→", callback_data=f"region:page:{page + 1}")
        )
    rows.append(navigation)
    return InlineKeyboardMarkup(inline_keyboard=rows)


def region_search_keyboard(regions: list[Region]) -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton(
                text=region.name,
                callback_data=f"region:pick:{region.id}",
            )
        ]
        for region in regions[:12]
    ]
    rows.append(
        [InlineKeyboardButton(text="Показать весь список", callback_data="region:page:0")]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def notifications_keyboard(mode: str, scope: str = "region") -> InlineKeyboardMarkup:
    labels = {
        "all": "Все события",
        "important": "Тревоги и отбои",
        "off": "Выключить",
    }
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text=("✅ " if scope == "region" else "") + "Мой регион",
                    callback_data="notification_scope:region",
                ),
                InlineKeyboardButton(
                    text=("✅ " if scope == "all" else "") + "Вся Россия",
                    callback_data="notification_scope:all",
                ),
            ],
            *[
            [
                InlineKeyboardButton(
                    text=("✅ " if mode == value else "") + label,
                    callback_data=f"notifications:{value}",
                )
            ]
            for value, label in labels.items()
            ],
        ]
    )


def stats_keyboard(region: Region) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="🔄 Обновить", callback_data="stats:refresh"),
                InlineKeyboardButton(text="Источник", url=region.url),
            ]
        ]
    )


def overview_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="🔄 Обновить", callback_data="overview:refresh"
                ),
                InlineKeyboardButton(
                    text="Карта", url="https://bplarussia.ru/map/"
                ),
            ]
        ]
    )


def safety_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="Официальное приложение МЧС",
                    url=(
                        "https://mchs.gov.ru/deyatelnost/informacionnye-sistemy/"
                        "mobilnoe-prilozhenie-mchs-rossii"
                    ),
                )
            ],
            [
                InlineKeyboardButton(
                    text="Памятка bplarussia.ru",
                    url="https://bplarussia.ru/safety/",
                )
            ],
        ]
    )


def sources_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="БПЛА Россия", url="https://bplarussia.ru/regions/"
                )
            ],
            [
                InlineKeyboardButton(
                    text="RadarMap — живая карта", url="https://radar-map.ru/"
                )
            ],
            [
                InlineKeyboardButton(
                    text="Воздушная обстановка", url="https://bplaalert.ru/"
                )
            ],
            [
                InlineKeyboardButton(
                    text="Радар ВРВ", url="https://radarvrv.ru/"
                )
            ],
        ]
    )


def delete_confirmation_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="Удалить", callback_data="delete:yes"),
                InlineKeyboardButton(text="Отмена", callback_data="delete:no"),
            ]
        ]
    )


def event_keyboard(url: str) -> InlineKeyboardMarkup | None:
    if not url:
        return None
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="Открыть источник", url=url)]]
    )
