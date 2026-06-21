from __future__ import annotations

import unittest

from ckassa_payments import (
    ORDER_ID_PREFIX,
    make_order_id,
    payment_identity,
    payment_validation_error,
)


class CkassaPaymentTests(unittest.TestCase):
    def test_voice_order_has_its_own_ckassa_namespace(self) -> None:
        order_id = make_order_id(123456789)
        self.assertTrue(order_id.isdigit())
        self.assertTrue(order_id.startswith(ORDER_ID_PREFIX))
        self.assertLessEqual(len(order_id), 40)
        self.assertNotEqual(ORDER_ID_PREFIX, "72")

    def test_payment_state_is_part_of_event_identity(self) -> None:
        created = payment_identity({"regPayNum": "42", "state": "CREATED"})
        paid = payment_identity({"regPayNum": "42", "state": "PAYED"})
        self.assertNotEqual(created, paid)

    def test_paid_event_must_match_order(self) -> None:
        payment = {
            "amount": 9_900,
            "properties": ["7120260621123456123456", "", "123456789"],
            "servCode": "voice-service",
        }
        self.assertIsNone(
            payment_validation_error(
                payment,
                expected_amount_kopeks=9_900,
                expected_telegram_id=123456789,
                expected_serv_code="voice-service",
            )
        )
        self.assertIn(
            "service",
            payment_validation_error(
                payment,
                expected_amount_kopeks=9_900,
                expected_telegram_id=123456789,
                expected_serv_code="another-service",
            ),
        )


if __name__ == "__main__":
    unittest.main()
