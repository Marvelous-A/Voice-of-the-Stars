from __future__ import annotations

import json
import os
import re
import secrets
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import quote

import aiohttp


DEFAULT_BASE_URL = "https://api2.ckassa.ru/api-shop/rs/open"
DEMO_BASE_URL = "https://demo-api2.ckassa.ru/api-shop/rs/open"
MSK = timezone(timedelta(hours=3))
ORDER_ID_PREFIX = "72"


class CkassaPaymentError(Exception):
    pass


class CkassaPaymentConfigError(CkassaPaymentError):
    pass


class CkassaPaymentAccessDenied(CkassaPaymentError):
    pass


class CkassaProviderNotFound(CkassaPaymentConfigError):
    pass


@dataclass(frozen=True, slots=True)
class CkassaConfig:
    api_login: str
    api_authorization: str
    serv_code: str
    base_url: str = DEFAULT_BASE_URL
    invoice_ttl_minutes: int = 60
    timeout_sec: int = 60
    invoice_type: str = "READ_ONLY"

    @classmethod
    def from_env(cls) -> "CkassaConfig":
        base_url = os.getenv("CKASSA_BASE_URL", DEFAULT_BASE_URL).strip()
        if os.getenv("CKASSA_USE_DEMO", "").strip().lower() in {"1", "true", "yes"}:
            base_url = DEMO_BASE_URL
        return cls(
            api_login=os.getenv("CKASSA_API_LOGIN", "").strip(),
            api_authorization=os.getenv("CKASSA_API_AUTHORIZATION", "").strip(),
            serv_code=os.getenv("CKASSA_SERV_CODE", "").strip(),
            base_url=base_url,
            invoice_ttl_minutes=_read_positive_int("CKASSA_INVOICE_TTL_MINUTES", 60),
            timeout_sec=_read_positive_int("CKASSA_TIMEOUT_SEC", 60),
            invoice_type=(
                os.getenv("CKASSA_INVOICE_TYPE_WITHOUT_PHONE", "READ_ONLY").strip()
                or "READ_ONLY"
            ),
        )

    def validate(self) -> None:
        missing = []
        if not self.api_login:
            missing.append("CKASSA_API_LOGIN")
        if not self.api_authorization:
            missing.append("CKASSA_API_AUTHORIZATION")
        if not self.serv_code:
            missing.append("CKASSA_SERV_CODE")
        if missing:
            raise CkassaPaymentConfigError(
                "Ckassa payment is not configured: " + ", ".join(missing)
            )


@dataclass(frozen=True, slots=True)
class CkassaInvoice:
    order_id: str
    pay_url: str
    amount_kopeks: int
    best_before: str
    expires_at: datetime


class CkassaClient:
    def __init__(self, config: CkassaConfig | None = None) -> None:
        self.config = config or CkassaConfig.from_env()

    async def create_invoice(
        self,
        *,
        order_id: str,
        telegram_id: str,
        amount_kopeks: int,
    ) -> CkassaInvoice:
        self.config.validate()
        _validate_order_id(order_id)
        _validate_telegram_id(telegram_id)
        if amount_kopeks <= 0:
            raise CkassaPaymentError("Ckassa amount must be positive")

        expires_at = datetime.now(MSK) + timedelta(
            minutes=self.config.invoice_ttl_minutes
        )
        best_before = format_ckassa_datetime(expires_at)
        payload = {
            "servCode": self.config.serv_code,
            "startPaySelect": True,
            "invType": self.config.invoice_type,
            "amount": int(amount_kopeks),
            "bestBefore": best_before,
            "tgInvPayer": str(telegram_id),
            "properties": [str(order_id), "", str(telegram_id)],
        }
        text = await self._request_text("POST", "invoice/create2/", json=payload)
        pay_url = text.strip().strip('"')
        error = _ckassa_result_exception(pay_url)
        if error:
            raise error
        if not pay_url.startswith(("http://", "https://")):
            raise CkassaPaymentError(
                f"Unexpected Ckassa invoice response: {pay_url[:200]}"
            )
        return CkassaInvoice(
            order_id=str(order_id),
            pay_url=pay_url,
            amount_kopeks=int(amount_kopeks),
            best_before=best_before,
            expires_at=expires_at.astimezone(timezone.utc),
        )

    async def get_new_payments(self) -> list[dict[str, Any]]:
        self.config.validate()
        data = await self._request_json("GET", "payments/new")
        if isinstance(data, dict):
            payments = data.get("payments", [])
        elif isinstance(data, list):
            payments = data
        else:
            payments = []
        return [payment for payment in payments if isinstance(payment, dict)]

    async def cancel_invoice(self, invoice_url: str) -> bool:
        self.config.validate()
        text = await self._request_text(
            "POST",
            f"invoice/cancel?invoiceUrl={quote(invoice_url, safe='')}",
        )
        return text.strip().upper() == "SUCCESS"

    async def _request_json(self, method: str, path: str, **kwargs: Any) -> Any:
        text = await self._request_text(method, path, **kwargs)
        try:
            return json.loads(text)
        except json.JSONDecodeError as error:
            raise CkassaPaymentError(
                f"Ckassa returned invalid JSON: {text[:200]}"
            ) from error

    async def _request_text(self, method: str, path: str, **kwargs: Any) -> str:
        timeout = aiohttp.ClientTimeout(total=self.config.timeout_sec)
        headers = {
            "ApiLoginAuthorization": self.config.api_login,
            "ApiAuthorization": self.config.api_authorization,
        }
        if "json" in kwargs:
            headers["Content-Type"] = "application/json"
        url = f"{self.config.base_url.rstrip('/')}/{path.lstrip('/')}"
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.request(method, url, headers=headers, **kwargs) as response:
                text = await response.text()
                if not 200 <= response.status < 300:
                    raise CkassaPaymentError(
                        f"Ckassa {method} {path} failed with HTTP "
                        f"{response.status}: {text[:300]}"
                    )
                return text


def make_order_id(user_id: str | int) -> str:
    user_part = re.sub(r"\D+", "", str(user_id))[-10:] or "00000"
    stamp = datetime.now(MSK).strftime("%Y%m%d%H%M%S")
    nonce = secrets.randbelow(900_000) + 100_000
    return f"{ORDER_ID_PREFIX}{stamp}{user_part}{nonce}"


def extract_payment_order_id(payment: dict[str, Any]) -> str | None:
    for field in ("properties", "property", "map"):
        order_id = _extract_order_id_from_properties(payment.get(field))
        if order_id:
            return order_id
    return None


def payment_identity(payment: dict[str, Any]) -> str:
    reg_pay_num = payment.get("regPayNum")
    if reg_pay_num:
        state = str(payment.get("state") or "unknown").upper()
        return f"regPayNum:{reg_pay_num}:{state}"
    order_id = extract_payment_order_id(payment) or "unknown"
    state = payment.get("state") or "unknown"
    amount = payment.get("amount") or "0"
    created = payment.get("createDate") or payment.get("created") or ""
    return f"payment:{order_id}:{state}:{amount}:{created}"


def payment_validation_error(
    payment: dict[str, Any],
    *,
    expected_amount_kopeks: int,
    expected_telegram_id: str | int,
    expected_serv_code: str = "",
) -> str | None:
    amount = extract_payment_amount_kopeks(payment)
    if amount is None:
        return "Ckassa did not return the payment amount"
    if amount != int(expected_amount_kopeks):
        return f"payment amount {amount} does not match order amount {expected_amount_kopeks}"

    payer_id = extract_payment_telegram_id(payment)
    if payer_id is None:
        return "Ckassa did not return the Telegram payer ID"
    if payer_id != str(expected_telegram_id):
        return f"Telegram payer {payer_id} does not match order user {expected_telegram_id}"

    actual_serv_code = _extract_named_scalar(payment, "servcode", "servicecode")
    if actual_serv_code and expected_serv_code and actual_serv_code != str(expected_serv_code):
        return (
            f"payment service {actual_serv_code} does not match "
            f"order service {expected_serv_code}"
        )
    return None


def extract_payment_amount_kopeks(payment: dict[str, Any]) -> int | None:
    raw = payment.get("amount")
    if isinstance(raw, bool) or raw is None:
        return None
    try:
        text = str(raw).strip().replace(",", ".")
        value = float(text)
    except (TypeError, ValueError):
        return None
    if value < 0 or not value.is_integer():
        return None
    return int(value)


def extract_payment_telegram_id(payment: dict[str, Any]) -> str | None:
    direct = _extract_named_scalar(
        payment,
        "tginvpayer",
        "telegramid",
        "telegram_id",
    )
    direct_digits = re.sub(r"\D+", "", direct)
    if direct_digits:
        return direct_digits

    for field in ("properties", "property", "map"):
        properties = payment.get(field)
        named = _extract_named_scalar(
            properties,
            "tginvpayer",
            "telegramid",
            "telegram_id",
        )
        named_digits = re.sub(r"\D+", "", named)
        if named_digits:
            return named_digits
        if isinstance(properties, list) and len(properties) >= 3:
            value = properties[2]
            if isinstance(value, dict):
                value = value.get("value", "")
            positional_digits = re.sub(r"\D+", "", str(value))
            if positional_digits:
                return positional_digits
    return None


def _extract_named_scalar(container: Any, *names: str) -> str:
    wanted = {name.replace("_", "").lower() for name in names}
    if isinstance(container, dict):
        for key, value in container.items():
            normalized = str(key).replace("_", "").lower()
            if normalized in wanted:
                if isinstance(value, dict):
                    value = value.get("value", "")
                return str(value or "").strip()
    elif isinstance(container, list):
        for item in container:
            if not isinstance(item, dict):
                continue
            normalized = str(item.get("name", "")).replace("_", "").lower()
            if normalized in wanted:
                return str(item.get("value", "") or "").strip()
    return ""


def format_kopeks_amount(amount_kopeks: int) -> str:
    amount = max(0, int(amount_kopeks))
    rubles, kopeks = divmod(amount, 100)
    return f"{rubles} ₽" if not kopeks else f"{rubles},{kopeks:02d} ₽"


def format_ckassa_datetime(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=MSK)
    return value.strftime("%d-%m-%Y %H:%M:%S %z")


def _read_positive_int(name: str, default: int) -> int:
    try:
        value = int(os.getenv(name, "").strip() or default)
    except ValueError:
        return default
    return value if value > 0 else default


def _ckassa_result_exception(text: str) -> CkassaPaymentError | None:
    code: Any = None
    message = ""
    details = ""
    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        data = None
    result = data.get("result") if isinstance(data, dict) else None
    if isinstance(result, dict):
        code = result.get("code")
        message = str(result.get("message") or "")
        details = str(result.get("details") or "")
    else:
        code_match = re.search(r"\bcode\s*=\s*([^,\s)]+)", text)
        message_match = re.search(
            r"\bmessage\s*=\s*(.*?)(?=,\s*details\s*=|\)\s*\)?\s*$)", text
        )
        details_match = re.search(r"\bdetails\s*=\s*(.*?)(?=\)\s*\)?\s*$)", text)
        if code_match:
            code = code_match.group(1).strip()
            message = message_match.group(1).strip() if message_match else ""
            details = details_match.group(1).strip() if details_match else ""
    if code in (None, 0, "0"):
        return None
    error_text = f"Ckassa error {code}: {message or 'unknown error'}"
    if details:
        error_text += f" ({details})"
    if str(code) == "1354":
        return CkassaProviderNotFound(error_text)
    if str(code) == "2715":
        return CkassaPaymentAccessDenied(error_text)
    return CkassaPaymentError(error_text)


def _extract_order_id_from_properties(properties: Any) -> str | None:
    if isinstance(properties, dict):
        for key, value in properties.items():
            if str(key).strip().lower() == "id":
                digits = re.sub(r"\D+", "", str(value))
                return digits or None
        values = properties.values()
    elif isinstance(properties, list):
        for item in properties:
            if isinstance(item, dict) and str(item.get("name", "")).lower() == "id":
                digits = re.sub(r"\D+", "", str(item.get("value", "")))
                return digits or None
        values = properties
    else:
        return None
    for value in values:
        if isinstance(value, dict):
            value = value.get("value", "")
        digits = re.sub(r"\D+", "", str(value))
        if 3 <= len(digits) <= 40:
            return digits
    return None


def _validate_order_id(order_id: str) -> None:
    if not re.fullmatch(r"\d{3,40}", str(order_id)):
        raise CkassaPaymentError("Ckassa ID must contain 3 to 40 digits")


def _validate_telegram_id(telegram_id: str) -> None:
    if not re.fullmatch(r"\d{5,12}", str(telegram_id)):
        raise CkassaPaymentError("Ckassa telegram_ID must contain 5 to 12 digits")
