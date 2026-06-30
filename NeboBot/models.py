from __future__ import annotations

import re
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any


@dataclass(frozen=True, slots=True)
class Region:
    id: int
    name: str
    slug: str
    incidents_total: int
    url: str


@dataclass(frozen=True, slots=True)
class Event:
    id: int
    published_at: datetime
    url: str
    title: str
    description: str
    region_ids: tuple[int, ...]
    region_names: tuple[str, ...]
    incident_type: str = ""
    threat_level: str = ""
    response_measures: str = ""

    def to_payload(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["published_at"] = self.published_at.isoformat()
        payload["region_ids"] = list(self.region_ids)
        payload["region_names"] = list(self.region_names)
        return payload

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> "Event":
        return cls(
            id=int(payload["id"]),
            published_at=datetime.fromisoformat(str(payload["published_at"])),
            url=str(payload.get("url", "")),
            title=str(payload.get("title", "")),
            description=str(payload.get("description", "")),
            region_ids=tuple(int(value) for value in payload.get("region_ids", [])),
            region_names=tuple(str(value) for value in payload.get("region_names", [])),
            incident_type=str(payload.get("incident_type", "")),
            threat_level=str(payload.get("threat_level", "")),
            response_measures=str(payload.get("response_measures", "")),
        )


@dataclass(frozen=True, slots=True)
class RegionStats:
    region: Region
    risk_level: str
    air_defence_total: int
    incidents_24h: int
    active_alerts_24h: int
    detections_24h: int
    air_defence_mentions_24h: int
    breakdown: tuple[tuple[str, int], ...]
    recent_events: tuple[Event, ...]
    updated_at: datetime
    history_truncated: bool = False
    is_stale: bool = False


@dataclass(frozen=True, slots=True)
class NationalOverview:
    incidents_total: int
    incidents_24h: int
    active_regions: tuple[str, ...]
    air_defence_mentions_24h: int
    top_regions_24h: tuple[tuple[str, int], ...]
    recent_events: tuple[Event, ...]
    updated_at: datetime
    history_truncated: bool = False
    is_stale: bool = False


def normalize_region_name(value: str) -> str:
    value = value.casefold().replace("ё", "е")
    value = re.sub(r"[^a-zа-я0-9]+", " ", value)
    return " ".join(value.split())


def is_primary_region(region: Region) -> bool:
    """Убирает составные категории сайта из основного списка выбора."""
    name = normalize_region_name(region.name)
    raw = region.name.casefold()
    if name in {
        "без рубрики",
        "московский регион",
        "северный кавказ",
        "пермская область",
    }:
        return False
    return not (
        "," in raw
        or "/" in raw
        or " и " in f" {name} "
        or name.count(" область") > 1
        or name.count(" край") > 1
        or name.count(" республика") > 1
    )


def risk_level_for_total(incidents_total: int) -> str:
    """Повторяет шкалу риска на региональных страницах bplarussia.ru."""
    if incidents_total <= 4:
        return "НИЗКИЙ"
    if incidents_total <= 10:
        return "СРЕДНИЙ"
    return "ВЫСОКИЙ"


def is_clear_event(event: Event) -> bool:
    searchable = f"{event.incident_type} {event.title} {event.description}"
    searchable = searchable.casefold().replace("ё", "е")
    return "отбой" in searchable or "угроза миновала" in searchable


def is_important_event(event: Event) -> bool:
    if is_clear_event(event):
        return False
    searchable = f"{event.incident_type} {event.title} {event.description}"
    searchable = searchable.casefold().replace("ё", "е")
    return any(
        marker in searchable
        for marker in (
            "опасност",
            "тревог",
            "угроз",
            "обнаруж",
            "пво",
            "сбит",
            "уничтож",
            "перехва",
        )
    )


def event_matches_region(event: Event, region: Region) -> bool:
    """Сопоставляет и точные категории, и составные названия регионов сайта."""
    if region.id in event.region_ids:
        return True

    needle = normalize_region_name(region.name)
    if not needle:
        return False
    needle_tokens = set(needle.split())
    for event_region in event.region_names:
        candidate = normalize_region_name(event_region)
        if re.search(rf"(?<!\w){re.escape(needle)}(?!\w)", candidate):
            return True
        if needle_tokens == set(candidate.split()):
            return True
    return False


def classify_events(events: list[Event]) -> tuple[int, int, int, tuple[tuple[str, int], ...]]:
    active_alerts = 0
    detections = 0
    air_defence_mentions = 0
    by_type: dict[str, int] = {}

    for event in events:
        label = (event.incident_type or event.title or "Прочее").strip()
        by_type[label] = by_type.get(label, 0) + 1

        searchable = f"{event.incident_type} {event.title}".casefold().replace("ё", "е")
        if not is_clear_event(event) and any(
            marker in searchable
            for marker in ("опасност", "тревог", "угроз", "обнаруж")
        ):
            active_alerts += 1
        if "обнаруж" in searchable:
            detections += 1
        if any(
            marker in searchable
            for marker in ("пво", "сбит", "уничтож", "перехва")
        ):
            air_defence_mentions += 1

    breakdown = tuple(sorted(by_type.items(), key=lambda item: (-item[1], item[0]))[:6])
    return active_alerts, detections, air_defence_mentions, breakdown
