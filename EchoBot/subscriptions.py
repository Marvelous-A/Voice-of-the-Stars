from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class SubscriptionPlan:
    code: str
    tier: str
    title: str
    duration_days: int
    price_kopeks: int
    old_price_kopeks: int

    @property
    def price_rubles(self) -> int:
        return self.price_kopeks // 100

    @property
    def old_price_rubles(self) -> int:
        return self.old_price_kopeks // 100


PLANS: dict[str, SubscriptionPlan] = {
    "premium_7": SubscriptionPlan(
        code="premium_7",
        tier="premium",
        title="Premium на 7 дней",
        duration_days=7,
        price_kopeks=14_900,
        old_price_kopeks=30_000,
    ),
    "premium_month": SubscriptionPlan(
        code="premium_month",
        tier="premium",
        title="Premium на месяц",
        duration_days=30,
        price_kopeks=24_900,
        old_price_kopeks=50_000,
    ),
    "premium_year": SubscriptionPlan(
        code="premium_year",
        tier="premium",
        title="Premium на год",
        duration_days=365,
        price_kopeks=99_900,
        old_price_kopeks=200_000,
    ),
    "vip_year": SubscriptionPlan(
        code="vip_year",
        tier="vip",
        title="VIP на год",
        duration_days=365,
        price_kopeks=349_900,
        old_price_kopeks=700_000,
    ),
}


def get_plan(code: str) -> SubscriptionPlan | None:
    return PLANS.get(code)
