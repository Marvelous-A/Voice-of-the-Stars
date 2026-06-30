from __future__ import annotations

import unittest
from datetime import UTC, datetime

from models import (
    Event,
    Region,
    classify_events,
    event_matches_region,
    is_important_event,
    is_primary_region,
    risk_level_for_total,
)


def make_event(
    *,
    event_id: int = 1,
    title: str = "Опасность БПЛА",
    incident_type: str = "Опасность по БПЛА",
    region_ids: tuple[int, ...] = (10,),
    region_names: tuple[str, ...] = ("Рязанская область",),
) -> Event:
    return Event(
        id=event_id,
        published_at=datetime(2026, 6, 23, 17, 10, tzinfo=UTC),
        url="https://example.test/event",
        title=title,
        description="Описание",
        region_ids=region_ids,
        region_names=region_names,
        incident_type=incident_type,
        threat_level="Высокий",
    )


class RegionMatchingTests(unittest.TestCase):
    def test_matches_exact_category(self) -> None:
        event = make_event(region_ids=(10,), region_names=("Другое название",))
        region = Region(10, "Рязанская область", "ryazan", 0, "")
        self.assertTrue(event_matches_region(event, region))

    def test_matches_region_inside_composite_name(self) -> None:
        event = make_event(
            region_ids=(99,),
            region_names=("Ростовская область и Краснодарский край",),
        )
        region = Region(17, "Краснодарский край", "krasnodar", 0, "")
        self.assertTrue(event_matches_region(event, region))

    def test_short_name_does_not_match_part_of_word(self) -> None:
        event = make_event(region_ids=(99,), region_names=("Крымский район",))
        region = Region(18, "Крым", "crimea", 0, "")
        self.assertFalse(event_matches_region(event, region))


class ClassificationTests(unittest.TestCase):
    def test_counts_alerts_detections_and_clear_messages(self) -> None:
        events = [
            make_event(event_id=1),
            make_event(
                event_id=2,
                title="Обнаружение БПЛА",
                incident_type="Обнаружение БПЛА",
            ),
            make_event(
                event_id=3,
                title="Отбой опасности по БПЛА",
                incident_type="Отбой тревоги",
            ),
            make_event(
                event_id=4,
                title="Работа ПВО: цель уничтожена",
                incident_type="Работа ПВО",
            ),
        ]
        active, detections, air_defence, breakdown = classify_events(events)
        self.assertEqual(active, 2)
        self.assertEqual(detections, 1)
        self.assertEqual(air_defence, 1)
        self.assertEqual(sum(count for _, count in breakdown), 4)

    def test_risk_scale_matches_site_thresholds(self) -> None:
        self.assertEqual(risk_level_for_total(0), "НИЗКИЙ")
        self.assertEqual(risk_level_for_total(4), "НИЗКИЙ")
        self.assertEqual(risk_level_for_total(5), "СРЕДНИЙ")
        self.assertEqual(risk_level_for_total(10), "СРЕДНИЙ")
        self.assertEqual(risk_level_for_total(11), "ВЫСОКИЙ")

    def test_clear_is_not_important_but_threat_is(self) -> None:
        self.assertTrue(is_important_event(make_event()))
        self.assertFalse(
            is_important_event(
                make_event(
                    title="Отбой опасности",
                    incident_type="Отбой тревоги",
                )
            )
        )

    def test_composite_category_is_hidden_from_primary_picker(self) -> None:
        self.assertFalse(
            is_primary_region(
                Region(1, "Ростовская область и Краснодарский край", "combo", 1, "")
            )
        )
        self.assertTrue(
            is_primary_region(
                Region(2, "Ростовская область", "rostov", 1, "")
            )
        )


if __name__ == "__main__":
    unittest.main()
