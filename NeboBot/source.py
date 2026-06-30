from __future__ import annotations

import asyncio
import html
import logging
import re
from collections import Counter
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Any

import aiohttp

from models import (
    Event,
    NationalOverview,
    Region,
    RegionStats,
    classify_events,
    is_important_event,
    is_primary_region,
    normalize_region_name,
    risk_level_for_total,
)


logger = logging.getLogger(__name__)


class SourceError(RuntimeError):
    pass


def _strip_html(value: str) -> str:
    value = re.sub(r"<\s*br\s*/?\s*>", "\n", value, flags=re.IGNORECASE)
    value = re.sub(r"<[^>]+>", " ", value)
    value = " ".join(html.unescape(value).split())
    return re.sub(r"\s+([,.;:!?])", r"\1", value)


def _parse_datetime(value: str, *, is_gmt: bool) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC if is_gmt else datetime.now().astimezone().tzinfo)
    return parsed.astimezone(UTC)


class BplaRussiaClient:
    def __init__(
        self,
        base_url: str,
        *,
        timeout_seconds: int = 20,
        regions_cache_ttl_seconds: int = 600,
        stats_cache_ttl_seconds: int = 30,
        max_history_pages: int = 50,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.api_url = f"{self.base_url}/wp-json/wp/v2"
        self.timeout = aiohttp.ClientTimeout(total=timeout_seconds)
        self.regions_cache_ttl_seconds = regions_cache_ttl_seconds
        self.stats_cache_ttl_seconds = stats_cache_ttl_seconds
        self.max_history_pages = max_history_pages
        self._session: aiohttp.ClientSession | None = None
        self._regions: tuple[Region, ...] = ()
        self._regions_cached_at = 0.0
        self._stats_cache: dict[int, tuple[float, RegionStats]] = {}
        self._stats_locks: dict[int, asyncio.Lock] = {}
        self._overview_cache: tuple[float, NationalOverview] | None = None
        self._overview_lock = asyncio.Lock()

    async def close(self) -> None:
        if self._session is not None:
            await self._session.close()
            self._session = None

    async def _get(
        self, path: str, params: dict[str, Any] | None = None
    ) -> tuple[Any, aiohttp.typedefs.LooseHeaders]:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=self.timeout,
                trust_env=True,
                headers={
                    "Accept": "application/json",
                    "User-Agent": "BPLA-Region-Telegram-Bot/1.0",
                },
            )

        url = f"{self.api_url}/{path.lstrip('/')}"
        last_error: Exception | None = None
        for attempt in range(3):
            try:
                async with self._session.get(url, params=params) as response:
                    if response.status >= 400:
                        body = (await response.text())[:500]
                        raise SourceError(
                            f"Сайт вернул HTTP {response.status}: {body}"
                        )
                    return await response.json(content_type=None), response.headers
            except (
                aiohttp.ClientError,
                asyncio.TimeoutError,
                SourceError,
                ValueError,
            ) as error:
                last_error = error
                if attempt < 2:
                    await asyncio.sleep(1.5 * (attempt + 1))
        raise SourceError(f"Не удалось получить данные с {url}: {last_error}") from last_error

    async def get_regions(self, *, force: bool = False) -> tuple[Region, ...]:
        now = asyncio.get_running_loop().time()
        if (
            not force
            and self._regions
            and now - self._regions_cached_at < self.regions_cache_ttl_seconds
        ):
            return self._regions

        regions: list[Region] = []
        page = 1
        total_pages = 1
        while page <= total_pages:
            data, headers = await self._get(
                "categories",
                {
                    "per_page": 100,
                    "page": page,
                    "hide_empty": "false",
                    "orderby": "name",
                    "order": "asc",
                },
            )
            total_pages = int(headers.get("X-WP-TotalPages", "1"))
            for item in data:
                url = str(item.get("link", ""))
                if "/region/" not in url:
                    continue
                regions.append(
                    Region(
                        id=int(item["id"]),
                        name=_strip_html(str(item.get("name", ""))),
                        slug=str(item.get("slug", "")),
                        incidents_total=int(item.get("count", 0)),
                        url=url,
                    )
                )
            page += 1

        self._regions = tuple(sorted(regions, key=lambda region: region.name.casefold()))
        self._regions_cached_at = now
        return self._regions

    async def get_region(self, region_id: int) -> Region | None:
        regions = await self.get_regions()
        return next((region for region in regions if region.id == region_id), None)

    async def get_primary_regions(self) -> tuple[Region, ...]:
        grouped: dict[tuple[str, ...], Region] = {}
        for region in await self.get_regions():
            if not is_primary_region(region):
                continue
            key = tuple(sorted(normalize_region_name(region.name).split()))
            current = grouped.get(key)
            if current is None or region.incidents_total > current.incidents_total:
                grouped[key] = region
        return tuple(sorted(grouped.values(), key=lambda item: item.name.casefold()))

    def parse_event(self, item: dict[str, Any]) -> Event:
        terms = item.get("_embedded", {}).get("wp:term", [])
        category_terms = terms[0] if terms and isinstance(terms[0], list) else []
        region_ids = tuple(int(value) for value in item.get("categories", []))
        region_names = tuple(
            _strip_html(str(term.get("name", "")))
            for term in category_terms
            if term.get("taxonomy") == "category" and term.get("name")
        )

        meta = item.get("meta") or {}
        meta_region = _strip_html(str(meta.get("region", "")))
        if meta_region and meta_region not in region_names:
            region_names += (meta_region,)

        gmt_value = str(item.get("date_gmt", ""))
        local_value = str(item.get("date", ""))
        published_at = _parse_datetime(
            gmt_value or local_value,
            is_gmt=bool(gmt_value),
        )
        return Event(
            id=int(item["id"]),
            published_at=published_at,
            url=str(item.get("link", "")),
            title=_strip_html(str((item.get("title") or {}).get("rendered", ""))),
            description=_strip_html(
                str((item.get("excerpt") or {}).get("rendered", ""))
            ),
            region_ids=region_ids,
            region_names=region_names,
            incident_type=_strip_html(str(meta.get("incident_type", ""))),
            threat_level=_strip_html(str(meta.get("threat_level", ""))),
            response_measures=_strip_html(str(meta.get("response_measures", ""))),
        )

    async def _get_events_page(
        self,
        *,
        page: int = 1,
        category_id: int | None = None,
        after: datetime | None = None,
        per_page: int = 100,
    ) -> tuple[list[Event], int, int]:
        params: dict[str, Any] = {
            "per_page": min(100, max(1, per_page)),
            "page": page,
            "orderby": "date",
            "order": "desc",
            "_embed": "wp:term",
        }
        if category_id is not None:
            params["categories"] = category_id
        if after is not None:
            params["after"] = after.astimezone(UTC).isoformat().replace("+00:00", "Z")

        data, headers = await self._get("posts", params)
        return (
            [self.parse_event(item) for item in data],
            int(headers.get("X-WP-Total", len(data))),
            int(headers.get("X-WP-TotalPages", "1")),
        )

    async def get_latest_events(self, limit: int = 100) -> list[Event]:
        events, _, _ = await self._get_events_page(per_page=limit)
        return events

    async def get_events_since(self, after: datetime) -> list[Event]:
        events: list[Event] = []
        page = 1
        total_pages = 1
        while page <= total_pages and page <= self.max_history_pages:
            batch, _, total_pages = await self._get_events_page(
                page=page, after=after, per_page=100
            )
            events.extend(batch)
            page += 1
        if page <= total_pages:
            logger.warning(
                "Лента содержит больше %s страниц новых событий; хвост пропущен",
                self.max_history_pages,
            )
        return events

    async def _fetch_region_stats(self, region: Region) -> RegionStats:
        after = datetime.now(UTC) - timedelta(hours=24)
        category_result, pvo_result, first_page = await asyncio.gather(
            self._get(f"categories/{region.id}"),
            self._get(
                "posts",
                {
                    "categories": region.id,
                    "search": "ПВО",
                    "per_page": 1,
                    "page": 1,
                },
            ),
            self._get_events_page(
                page=1,
                category_id=region.id,
                after=after,
                per_page=100,
            ),
        )
        category, _ = category_result
        _, pvo_headers = pvo_result
        first_events, total, total_pages = first_page
        refreshed_region = Region(
            id=region.id,
            name=_strip_html(str(category.get("name", region.name))),
            slug=str(category.get("slug", region.slug)),
            incidents_total=int(category.get("count", region.incidents_total)),
            url=str(category.get("link", region.url)),
        )

        events = list(first_events)
        page = 2
        while page <= total_pages and page <= self.max_history_pages:
            batch, _, _ = await self._get_events_page(
                page=page,
                category_id=region.id,
                after=after,
                per_page=100,
            )
            events.extend(batch)
            page += 1

        active, detections, air_defence, breakdown = classify_events(events)
        return RegionStats(
            region=refreshed_region,
            risk_level=risk_level_for_total(refreshed_region.incidents_total),
            air_defence_total=int(pvo_headers.get("X-WP-Total", "0")),
            incidents_24h=total,
            active_alerts_24h=active,
            detections_24h=detections,
            air_defence_mentions_24h=air_defence,
            breakdown=breakdown,
            recent_events=tuple(events[:5]),
            updated_at=datetime.now(UTC),
            history_truncated=page <= total_pages,
        )

    async def get_region_stats(
        self, region: Region, *, force: bool = False
    ) -> RegionStats:
        request_started = asyncio.get_running_loop().time()
        now = request_started
        cached = self._stats_cache.get(region.id)
        if (
            not force
            and cached
            and now - cached[0] < self.stats_cache_ttl_seconds
        ):
            return cached[1]

        lock = self._stats_locks.setdefault(region.id, asyncio.Lock())
        async with lock:
            now = asyncio.get_running_loop().time()
            cached = self._stats_cache.get(region.id)
            refreshed_while_waiting = bool(cached and cached[0] >= request_started)
            if (
                cached
                and (
                    refreshed_while_waiting
                    or (
                        not force
                        and now - cached[0] < self.stats_cache_ttl_seconds
                    )
                )
            ):
                return cached[1]
            try:
                stats = await self._fetch_region_stats(region)
            except SourceError:
                if cached:
                    return replace(cached[1], is_stale=True)
                raise
            self._stats_cache[region.id] = (
                asyncio.get_running_loop().time(),
                stats,
            )
            return stats

    async def _fetch_national_overview(self) -> NationalOverview:
        after = datetime.now(UTC) - timedelta(hours=24)
        total_result, first_page = await asyncio.gather(
            self._get("posts", {"per_page": 1, "page": 1}),
            self._get_events_page(page=1, after=after, per_page=100),
        )
        _, total_headers = total_result
        first_events, incidents_24h, total_pages = first_page
        events = list(first_events)
        page = 2
        while page <= total_pages and page <= self.max_history_pages:
            batch, _, _ = await self._get_events_page(
                page=page, after=after, per_page=100
            )
            events.extend(batch)
            page += 1

        latest_by_region: dict[str, Event] = {}
        display_names: dict[str, str] = {}
        counts: Counter[str] = Counter()
        for event in sorted(events, key=lambda item: item.published_at, reverse=True):
            for name in dict.fromkeys(event.region_names):
                key = normalize_region_name(name)
                if not key:
                    continue
                display_names.setdefault(key, name)
                latest_by_region.setdefault(key, event)
                counts[key] += 1

        active_regions = tuple(
            sorted(
                (
                    display_names[key]
                    for key, event in latest_by_region.items()
                    if is_important_event(event)
                ),
                key=str.casefold,
            )
        )
        top_regions = tuple(
            (display_names[key], count) for key, count in counts.most_common(6)
        )
        _, _, air_defence, _ = classify_events(events)
        return NationalOverview(
            incidents_total=int(total_headers.get("X-WP-Total", "0")),
            incidents_24h=incidents_24h,
            active_regions=active_regions,
            air_defence_mentions_24h=air_defence,
            top_regions_24h=top_regions,
            recent_events=tuple(events[:5]),
            updated_at=datetime.now(UTC),
            history_truncated=page <= total_pages,
        )

    async def get_national_overview(
        self, *, force: bool = False
    ) -> NationalOverview:
        request_started = asyncio.get_running_loop().time()
        now = request_started
        cached = self._overview_cache
        if (
            not force
            and cached
            and now - cached[0] < self.stats_cache_ttl_seconds
        ):
            return cached[1]
        async with self._overview_lock:
            now = asyncio.get_running_loop().time()
            cached = self._overview_cache
            refreshed_while_waiting = bool(cached and cached[0] >= request_started)
            if (
                cached
                and (
                    refreshed_while_waiting
                    or (
                        not force
                        and now - cached[0] < self.stats_cache_ttl_seconds
                    )
                )
            ):
                return cached[1]
            try:
                overview = await self._fetch_national_overview()
            except SourceError:
                if cached:
                    return replace(cached[1], is_stale=True)
                raise
            self._overview_cache = (
                asyncio.get_running_loop().time(),
                overview,
            )
            return overview
