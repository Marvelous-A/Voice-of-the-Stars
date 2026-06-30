from __future__ import annotations

import unittest

from ckassa_payments import (
    ORDER_ID_PREFIX,
    extract_payment_order_id,
    make_order_id,
    payment_identity,
    payment_validation_error,
)
from subscriptions import PLANS


class CkassaPaymentTests(unittest.TestCase):
    def test_tariff_prices_match_offer(self) -> None:
        self.assertEqual(PLANS["premium_7"].price_kopeks, 14_900)
        self.assertEqual(PLANS["premium_month"].price_kopeks, 24_900)
        self.assertEqual(PLANS["premium_year"].price_kopeks, 99_900)
        self.assertEqual(PLANS["vip_year"].price_kopeks, 349_900)

    def test_extracts_numeric_order_from_ckassa_properties(self) -> None:
        payment = {
            "properties": ["20260620123456123456", "", "123456"],
            "state": "PAYED",
        }
        self.assertEqual(
            extract_payment_order_id(payment), "20260620123456123456"
        )

    def test_payment_state_is_part_of_event_identity(self) -> None:
        created = payment_identity({"regPayNum": "42", "state": "CREATED"})
        paid = payment_identity({"regPayNum": "42", "state": "PAYED"})
        self.assertNotEqual(created, paid)

    def test_order_id_is_ckassa_compatible(self) -> None:
        order_id = make_order_id(123456789)
        self.assertTrue(order_id.isdigit())
        self.assertTrue(order_id.startswith(ORDER_ID_PREFIX))
        self.assertGreaterEqual(len(order_id), 3)
        self.assertLessEqual(len(order_id), 40)

    def test_paid_event_must_match_amount_payer_and_service(self) -> None:
        payment = {
            "amount": 14_900,
            "tgInvPayer": "123456789",
            "servCode": "echo-service",
        }
        self.assertIsNone(
            payment_validation_error(
                payment,
                expected_amount_kopeks=14_900,
                expected_telegram_id=123456789,
                expected_serv_code="echo-service",
            )
        )
        self.assertIn(
            "amount",
            payment_validation_error(
                payment,
                expected_amount_kopeks=24_900,
                expected_telegram_id=123456789,
            ),
        )
        self.assertIn(
            "payer",
            payment_validation_error(
                payment,
                expected_amount_kopeks=14_900,
                expected_telegram_id=987654321,
            ),
        )

    def test_payer_can_be_read_from_ckassa_properties(self) -> None:
        payment = {
            "amount": "14900",
            "properties": [
                {"name": "id", "value": "20260620123456123456"},
                {"name": "phone", "value": ""},
                {"name": "telegram_ID", "value": "123456789"},
            ],
        }
        self.assertIsNone(
            payment_validation_error(
                payment,
                expected_amount_kopeks=14_900,
                expected_telegram_id=123456789,
            )
        )


if __name__ == "__main__":
    unittest.main()
