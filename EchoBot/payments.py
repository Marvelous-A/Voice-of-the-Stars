from __future__ import annotations

import asyncio
import html
import logging
from typing import Any

from aiogram import Bot
from aiogram.exceptions import TelegramAPIError, TelegramForbiddenError

from ckassa_payments import (
    CkassaClient,
    CkassaPaymentConfigError,
    extract_payment_order_id,
    make_order_id,
    payment_identity,
    payment_validation_error,
)
from config import Settings
from database import Database
from subscriptions import SubscriptionPlan


logger = logging.getLogger(__name__)


class SubscriptionPayments:
    def __init__(self, database: Database, settings: Settings) -> None:
        self.database = database
        self.settings = settings
        self.client = CkassaClient()
        self._create_lock = asyncio.Lock()
        self._process_lock = asyncio.Lock()

    async def create_or_reuse_order(
        self, user_id: int, plan: SubscriptionPlan
    ) -> tuple[dict[str, Any], bool]:
        async with self._create_lock:
            self.client.config.validate()
            active = await self.database.find_active_payment_order(user_id, plan.code)
            if active:
                return active, True

            order_id = make_order_id(user_id)
            invoice = await self.client.create_invoice(
                order_id=order_id,
                telegram_id=str(user_id),
                amount_kopeks=plan.price_kopeks,
            )
            order = await self.database.create_payment_order(
                order_id=order_id,
                user_id=user_id,
                plan_code=plan.code,
                amount_kopeks=plan.price_kopeks,
                invoice_url=invoice.pay_url,
                expires_at=invoice.expires_at,
            )
            return order, False

    async def process_updates(
        self, bot: Bot | None = None, *, notify: bool = True
    ) -> list[dict[str, Any]]:
        async with self._process_lock:
            credited: list[dict[str, Any]] = []
            for payment in await self.client.get_new_payments():
                payment_key = payment_identity(payment)
                order_id = extract_payment_order_id(payment)
                if not order_id:
                    continue
                order = await self.database.get_payment_order(order_id)
                if not order:
                    # Payments from other bots may be visible when the Ckassa
                    # API account is shared. An exact local order is mandatory.
                    continue
                state = str(payment.get("state") or "unknown")
                if state.upper() == "PAYED":
                    validation_error = payment_validation_error(
                        payment,
                        expected_amount_kopeks=int(order["amount_kopeks"]),
                        expected_telegram_id=int(order["user_id"]),
                        expected_serv_code=self.client.config.serv_code,
                    )
                    if validation_error:
                        is_new = await self.database.mark_ckassa_payment_seen(
                            payment_key=payment_key,
                            order_id=order_id,
                        )
                        if is_new:
                            await self._notify_rejected_payment(
                                bot, order_id, payment, validation_error
                            )
                        continue
                result = await self.database.apply_ckassa_payment(
                    payment_key=payment_key,
                    order_id=order_id,
                    state=state,
                    payment=payment,
                )
                if result:
                    credited.append(result)
                    if bot and notify:
                        await self._notify_credit(bot, result)
            return credited

    async def watch(self, bot: Bot) -> None:
        try:
            self.client.config.validate()
        except CkassaPaymentConfigError as error:
            logger.warning("Автопроверка CKassa отключена: %s", error)
            return
        while True:
            try:
                await self.process_updates(bot)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Не удалось проверить новые платежи CKassa")
            await asyncio.sleep(self.settings.ckassa_poll_interval_sec)

    async def _notify_rejected_payment(
        self,
        bot: Bot | None,
        order_id: str,
        payment: dict[str, Any],
        reason: str,
    ) -> None:
        logger.error(
            "CKassa payment quarantined: order_id=%s regPayNum=%s reason=%s",
            order_id,
            payment.get("regPayNum", ""),
            reason,
        )
        if not bot:
            return
        text = (
            "⚠️ <b>Платёж Ckassa отклонён защитой «ЭХО»</b>\n"
            f"Заказ: <code>{html.escape(order_id)}</code>\n"
            f"Платёж: <code>{html.escape(str(payment.get('regPayNum', '')))}</code>\n"
            f"Причина: {html.escape(reason)}"
        )
        admin_ids = set(self.settings.admin_ids)
        admin_ids.update(
            await self.database.get_user_ids_by_usernames(
                self.settings.admin_usernames
            )
        )
        for admin_id in admin_ids:
            try:
                await bot.send_message(admin_id, text)
            except TelegramForbiddenError:
                await self.database.mark_unreachable(admin_id)
            except TelegramAPIError:
                logger.exception(
                    "Не удалось уведомить администратора %s об отклонённом платеже",
                    admin_id,
                )

    async def _notify_credit(self, bot: Bot, result: dict[str, Any]) -> None:
        user_id = int(result["user_id"])
        title = html.escape(str(result["plan_title"]))
        expires = html.escape(str(result["expires_display"]))
        receipt = str(result.get("receipt") or "").strip()
        text = (
            "✅ <b>Оплата получена</b>\n\n"
            f"Подключён {title}.\n"
            f"Доступ действует до <b>{expires}</b>."
        )
        if receipt.startswith(("http://", "https://")):
            text += f'\n\n<a href="{html.escape(receipt, quote=True)}">Открыть чек</a>'
        try:
            await bot.send_message(user_id, text, disable_web_page_preview=True)
        except TelegramForbiddenError:
            await self.database.mark_unreachable(user_id)
        except TelegramAPIError:
            logger.exception("Не удалось уведомить пользователя %s об оплате", user_id)

        admin_text = (
            "💳 <b>Оплата подписки в «ЭХО»</b>\n"
            f"Пользователь: <code>{user_id}</code>\n"
            f"Тариф: {title}\n"
            f"Сумма: {int(result['amount_kopeks']) // 100} ₽\n"
            f"Заказ: <code>{html.escape(str(result['order_id']))}</code>"
        )
        admin_ids = set(self.settings.admin_ids)
        admin_ids.update(
            await self.database.get_user_ids_by_usernames(
                self.settings.admin_usernames
            )
        )
        for admin_id in admin_ids:
            try:
                await bot.send_message(admin_id, admin_text)
            except TelegramForbiddenError:
                await self.database.mark_unreachable(admin_id)
            except TelegramAPIError:
                logger.exception("Не удалось уведомить администратора %s", admin_id)
