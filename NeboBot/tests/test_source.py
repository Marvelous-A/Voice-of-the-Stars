from __future__ import annotations

import unittest
import asyncio
from datetime import UTC, datetime

from models import Region, RegionStats
from source import BplaRussiaClient


class SourceParsingTests(unittest.TestCase):
    def test_parses_wordpress_event(self) -> None:
        client = BplaRussiaClient("https://bplarussia.ru")
        event = client.parse_event(
            {
                "id": 27953,
                "date": "2026-06-23T20:10:07",
                "date_gmt": "2026-06-23T17:10:07",
                "link": "https://bplarussia.ru/event/",
                "title": {"rendered": "Отбой &amp; проверка"},
                "excerpt": {"rendered": "<p>Угроза <b>миновала</b>.</p>"},
                "categories": [82],
                "meta": {
                    "incident_type": "Отбой тревоги",
                    "threat_level": "Нет угрозы",
                    "region": "Рязанская область",
                },
                "_embedded": {
                    "wp:term": [
                        [
                            {
                                "id": 82,
                                "name": "Рязанская область",
                                "taxonomy": "category",
                            }
                        ],
                        [],
                    ]
                },
            }
        )

        self.assertEqual(event.id, 27953)
        self.assertEqual(event.title, "Отбой & проверка")
        self.assertEqual(event.description, "Угроза миновала.")
        self.assertEqual(event.region_ids, (82,))
        self.assertEqual(event.region_names, ("Рязанская область",))
        self.assertEqual(
            event.published_at, datetime(2026, 6, 23, 17, 10, 7, tzinfo=UTC)
        )


class CountingClient(BplaRussiaClient):
    def __init__(self) -> None:
        super().__init__("https://example.test", stats_cache_ttl_seconds=30)
        self.fetch_count = 0

    async def _fetch_region_stats(self, region: Region) -> RegionStats:
        self.fetch_count += 1
        await asyncio.sleep(0.02)
        return RegionStats(
            region=region,
            risk_level="НИЗКИЙ",
            air_defence_total=0,
            incidents_24h=0,
            active_alerts_24h=0,
            detections_24h=0,
            air_defence_mentions_24h=0,
            breakdown=(),
            recent_events=(),
            updated_at=datetime.now(UTC),
        )


class SourceCacheTests(unittest.IsolatedAsyncioTestCase):
    async def test_concurrent_forced_refresh_is_coalesced(self) -> None:
        client = CountingClient()
        region = Region(82, "Рязанская область", "ryazan", 100, "")
        first, second = await asyncio.gather(
            client.get_region_stats(region, force=True),
            client.get_region_stats(region, force=True),
        )
        self.assertEqual(first, second)
        self.assertEqual(client.fetch_count, 1)


if __name__ == "__main__":
    unittest.main()
