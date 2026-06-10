import json
import os
import random
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import quote

import aiohttp


DEFAULT_BASE_URL = "https://api2.ckassa.ru/api-shop/rs/open"
DEMO_BASE_URL = "https://demo-api2.ckassa.ru/api-shop/rs/open"
MSK = timezone(timedelta(hours=3))


class CkassaPaymentError(Exception):
    pass


class CkassaPaymentConfigError(CkassaPaymentError):
    pass


class CkassaPaymentAccessDenied(CkassaPaymentError):
    pass


class CkassaProviderNotFound(CkassaPaymentConfigError):
    pass


@dataclass(frozen=True)
class CkassaConfig:
    api_login: str
    api_authorization: str
    serv_code: str
    amount_kopeks: int
    base_url: str = DEFAULT_BASE_URL
    invoice_ttl_minutes: int = 60
    timeout_sec: int = 60
    invoice_type_with_phone: str = "READ_ONLY"
    invoice_type_without_phone: str = "READ_ONLY"

    @classmethod
    def from_env(cls) -> "CkassaConfig":
        base_url = os.getenv("CKASSA_BASE_URL", DEFAULT_BASE_URL).strip()
        if os.getenv("CKASSA_USE_DEMO", "").strip().lower() in {"1", "true", "yes"}:
            base_url = DEMO_BASE_URL

        return cls(
            api_login=os.getenv("CKASSA_API_LOGIN", "").strip(),
            api_authorization=os.getenv("CKASSA_API_AUTHORIZATION", "").strip(),
            serv_code=os.getenv("CKASSA_SERV_CODE", "").strip(),
            amount_kopeks=_read_int_env("CKASSA_CONSULTATION_AMOUNT_KOPEKS", 0),
            base_url=base_url,
            invoice_ttl_minutes=_read_int_env("CKASSA_INVOICE_TTL_MINUTES", 60),
            timeout_sec=_read_int_env("CKASSA_TIMEOUT_SEC", 60),
            invoice_type_with_phone=os.getenv("CKASSA_INVOICE_TYPE_WITH_PHONE", "READ_ONLY").strip() or "READ_ONLY",
            invoice_type_without_phone=os.getenv("CKASSA_INVOICE_TYPE_WITHOUT_PHONE", "READ_ONLY").strip() or "READ_ONLY",
        )

    def validate(self) -> None:
        missing = []
        if not self.api_login:
            missing.append("CKASSA_API_LOGIN")
        if not self.api_authorization:
            missing.append("CKASSA_API_AUTHORIZATION")
        if not self.serv_code:
            missing.append("CKASSA_SERV_CODE")
        if self.amount_kopeks <= 0:
            missing.append("CKASSA_CONSULTATION_AMOUNT_KOPEKS")
        if missing:
            raise CkassaPaymentConfigError(
                "Ckassa payment is not configured: " + ", ".join(missing)
            )

    @property
    def amount_rub_text(self) -> str:
        return format_kopeks_amount(self.amount_kopeks)


@dataclass(frozen=True)
class CkassaInvoice:
    order_id: str
    pay_url: str
    amount_kopeks: int
    best_before: str


class CkassaClient:
    def __init__(self, config: CkassaConfig | None = None):
        self.config = config or CkassaConfig.from_env()

    async def create_invoice(
        self,
        *,
        order_id: str,
        telegram_id: str,
        phone: str = "",
    ) -> CkassaInvoice:
        self.config.validate()
        _validate_order_id(order_id)
        phone = normalize_phone(phone)
        if phone:
            _validate_phone(phone)
        _validate_telegram_id(telegram_id)

        best_before = format_ckassa_datetime(
            datetime.now(MSK) + timedelta(minutes=self.config.invoice_ttl_minutes)
        )
        properties = [order_id, phone, telegram_id]
        payload = {
            "servCode": self.config.serv_code,
            "startPaySelect": bool(phone),
            "invType": (
                self.config.invoice_type_with_phone
                if phone
                else self.config.invoice_type_without_phone
            ),
            "amount": self.config.amount_kopeks,
            "bestBefore": best_before,
            "tgInvPayer": telegram_id,
            "properties": properties,
        }
        text = await self._request_text("POST", "invoice/create2/", json=payload)
        pay_url = text.strip().strip('"')
        error = _ckassa_result_exception(pay_url)
        if error:
            raise error
        if not pay_url.startswith(("http://", "https://")):
            raise CkassaPaymentError(f"Unexpected Ckassa invoice response: {pay_url[:200]}")
        return CkassaInvoice(
            order_id=order_id,
            pay_url=pay_url,
            amount_kopeks=self.config.amount_kopeks,
            best_before=best_before,
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
        return [p for p in payments if isinstance(p, dict)]

    async def get_receipt(self, reg_pay_num: str) -> dict[str, Any]:
        self.config.validate()
        return await self._request_json(
            "POST",
            f"payment/receipt2?regPayNum={quote(str(reg_pay_num))}",
        )

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
        except json.JSONDecodeError as exc:
            raise CkassaPaymentError(f"Ckassa returned invalid JSON: {text[:200]}") from exc

    async def _request_text(self, method: str, path: str, **kwargs: Any) -> str:
        url = self._url(path)
        timeout = aiohttp.ClientTimeout(total=self.config.timeout_sec)
        headers = {
            "ApiLoginAuthorization": self.config.api_login,
            "ApiAuthorization": self.config.api_authorization,
        }
        if "json" in kwargs:
            headers["Content-Type"] = "application/json"

        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.request(method, url, headers=headers, **kwargs) as resp:
                text = await resp.text()
                if resp.status < 200 or resp.status >= 300:
                    raise CkassaPaymentError(
                        f"Ckassa {method} {path} failed with HTTP {resp.status}: {text[:300]}"
                    )
                return text

    def _url(self, path: str) -> str:
        return f"{self.config.base_url.rstrip('/')}/{path.lstrip('/')}"


class CkassaPaymentStore:
    def __init__(self, path: str):
        self.path = path

    def load(self) -> dict[str, Any]:
        try:
            with open(self.path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            data = {}
        data.setdefault("orders", {})
        data.setdefault("processed_payments", [])
        earnings = data.setdefault("earnings", {})
        earnings["total_kopeks"] = _coerce_int(earnings.get("total_kopeks"), 0)
        earnings["orders_count"] = _coerce_int(earnings.get("orders_count"), 0)
        return data

    def save(self, data: dict[str, Any]) -> None:
        tmp_path = f"{self.path}.tmp"
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        os.replace(tmp_path, self.path)

    def create_order(
        self,
        *,
        order_id: str,
        user_id: str,
        amount_kopeks: int,
        invoice_url: str,
        best_before: str,
        phone: str = "",
        specialist_type: str | None = None,
        specialist_id: str | None = None,
    ) -> dict[str, Any]:
        data = self.load()
        order = {
            "order_id": order_id,
            "user_id": str(user_id),
            "phone": normalize_phone(phone),
            "amount_kopeks": int(amount_kopeks),
            "invoice_url": invoice_url,
            "best_before": best_before,
            "specialist_type": specialist_type or "",
            "specialist_id": specialist_id or "",
            "status": "created",
            "credited": False,
            "created_at": datetime.now(MSK).isoformat(),
            "updated_at": datetime.now(MSK).isoformat(),
        }
        data["orders"][order_id] = order
        self.save(data)
        return order

    def find_active_order(self, user_id: str, amount_kopeks: int) -> dict[str, Any] | None:
        now = datetime.now(MSK)
        data = self.load()
        orders = sorted(
            data["orders"].values(),
            key=lambda item: item.get("created_at", ""),
            reverse=True,
        )
        for order in orders:
            if str(order.get("user_id")) != str(user_id):
                continue
            if order.get("status") != "created":
                continue
            if int(order.get("amount_kopeks", 0)) != int(amount_kopeks):
                continue
            if _is_before_expired(order.get("best_before", ""), now):
                continue
            return order
        return None

    def mark_payment_seen(self, payment_key: str) -> bool:
        data = self.load()
        processed = data.setdefault("processed_payments", [])
        if payment_key in processed:
            return False
        processed.append(payment_key)
        del processed[:-500]
        self.save(data)
        return True

    def mark_order_paid(self, order_id: str, payment: dict[str, Any]) -> dict[str, Any] | None:
        data = self.load()
        order = data["orders"].get(order_id)
        if not order:
            return None
        order["status"] = "payed"
        order["payment"] = payment
        order["reg_pay_num"] = str(payment.get("regPayNum") or "")
        order["receipt"] = payment.get("receipt") or ""
        order["updated_at"] = datetime.now(MSK).isoformat()
        data["orders"][order_id] = order
        self.save(data)
        return order

    def mark_order_state(self, order_id: str, state: str, payment: dict[str, Any]) -> None:
        data = self.load()
        order = data["orders"].get(order_id)
        if not order:
            return
        state_normalized = (state or "unknown").upper()
        order["last_payment_state"] = state_normalized
        if state_normalized in {"CANCELED", "CANCELLED", "DECLINED", "ERROR", "FAILED", "REFUNDED", "REJECTED"}:
            order["status"] = state_normalized.lower()
        order["payment"] = payment
        order["updated_at"] = datetime.now(MSK).isoformat()
        data["orders"][order_id] = order
        self.save(data)

    def mark_order_credited(self, order_id: str) -> None:
        data = self.load()
        order = data["orders"].get(order_id)
        if not order:
            return
        order["credited"] = True
        order["credited_at"] = datetime.now(MSK).isoformat()
        order["updated_at"] = datetime.now(MSK).isoformat()
        data["orders"][order_id] = order
        self.save(data)

    def add_earned_amount(self, order_id: str, amount_kopeks: int | str | None = None) -> tuple[bool, dict[str, Any]]:
        data = self.load()
        order = data["orders"].get(order_id)
        earnings = data.setdefault("earnings", {})
        now = datetime.now(MSK).isoformat()

        if not order:
            return False, earnings
        if order.get("earned_counted"):
            return False, earnings

        amount = _coerce_int(amount_kopeks, 0)
        if amount <= 0:
            amount = _coerce_int(order.get("amount_kopeks"), 0)
        if amount <= 0:
            return False, earnings

        earnings["total_kopeks"] = _coerce_int(earnings.get("total_kopeks"), 0) + amount
        earnings["orders_count"] = _coerce_int(earnings.get("orders_count"), 0) + 1
        earnings["updated_at"] = now

        order["earned_counted"] = True
        order["earned_amount_kopeks"] = amount
        order["earned_counted_at"] = now
        order["updated_at"] = now
        data["orders"][order_id] = order

        self.save(data)
        return True, earnings

    def get_earnings(self) -> dict[str, Any]:
        data = self.load()
        return data.setdefault("earnings", {})

    def uncredited_paid_orders(self) -> list[dict[str, Any]]:
        data = self.load()
        return [
            order
            for order in data["orders"].values()
            if order.get("status") == "payed" and not order.get("credited")
        ]

    def get_user_orders(self, user_id: str) -> list[dict[str, Any]]:
        data = self.load()
        return [
            order
            for order in data["orders"].values()
            if str(order.get("user_id")) == str(user_id)
        ]


def _read_int_env(name: str, default: int) -> int:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _coerce_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _ckassa_result_exception(text: str) -> CkassaPaymentError | None:
    code = None
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
        # Some Ckassa failures are returned as a Java-style object string
        # instead of JSON, for example ErrorResponse(result=ResultResponse(...)).
        code_match = re.search(r"\bcode\s*=\s*([^,\s)]+)", text)
        message_match = re.search(
            r"\bmessage\s*=\s*(.*?)(?=,\s*details\s*=|\)\s*\)?\s*$)",
            text,
        )
        details_match = re.search(r"\bdetails\s*=\s*(.*?)(?=\)\s*\)?\s*$)", text)
        if code_match:
            code = code_match.group(1).strip()
            message = message_match.group(1).strip() if message_match else ""
            details = details_match.group(1).strip() if details_match else ""

    if code in (None, 0, "0"):
        return None
    message = message or "unknown error"
    if details:
        error_text = f"Ckassa error {code}: {message} ({details})"
    else:
        error_text = f"Ckassa error {code}: {message}"
    if str(code) == "1354":
        return CkassaProviderNotFound(error_text)
    if str(code) == "2715":
        return CkassaPaymentAccessDenied(error_text)
    return CkassaPaymentError(error_text)


def _rub_word(value: int) -> str:
    value = abs(value)
    if value % 100 in {11, 12, 13, 14}:
        return "рублей"
    if value % 10 == 1:
        return "рубль"
    if value % 10 in {2, 3, 4}:
        return "рубля"
    return "рублей"


def format_kopeks_amount(amount_kopeks: int | str | None) -> str:
    amount = max(0, _coerce_int(amount_kopeks, 0))
    rub = amount // 100
    kop = amount % 100
    if kop == 0:
        return f"{rub} {_rub_word(rub)}"
    return f"{rub},{kop:02d} руб."


def normalize_phone(value: str) -> str:
    digits = re.sub(r"\D+", "", value or "")
    if digits.startswith("8") and len(digits) == 11:
        digits = "7" + digits[1:]
    return digits


def make_order_id(user_id: str | int) -> str:
    user_part = re.sub(r"\D+", "", str(user_id))[-10:] or "00000"
    stamp = datetime.now(MSK).strftime("%Y%m%d%H%M%S")
    return f"{stamp}{user_part}{random.randint(1000, 9999)}"


def format_ckassa_datetime(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=MSK)
    return value.strftime("%d-%m-%Y %H:%M:%S %z")


def extract_payment_order_id(payment: dict[str, Any]) -> str | None:
    for field in ("properties", "property", "map"):
        props = payment.get(field)
        order_id = _extract_order_id_from_properties(props)
        if order_id:
            return order_id
    return None


def payment_identity(payment: dict[str, Any]) -> str:
    reg_pay_num = payment.get("regPayNum")
    if reg_pay_num:
        return f"regPayNum:{reg_pay_num}"
    order_id = extract_payment_order_id(payment) or "unknown"
    state = payment.get("state") or "unknown"
    amount = payment.get("amount") or "0"
    created = payment.get("createDate") or payment.get("created") or ""
    return f"payment:{order_id}:{state}:{amount}:{created}"


def _extract_order_id_from_properties(props: Any) -> str | None:
    if isinstance(props, dict):
        for key, value in props.items():
            if str(key).strip().lower() == "id":
                digits = re.sub(r"\D+", "", str(value))
                return digits or None
        for value in props.values():
            digits = re.sub(r"\D+", "", str(value))
            if 3 <= len(digits) <= 40:
                return digits
        return None

    if isinstance(props, list):
        for item in props:
            if isinstance(item, dict) and str(item.get("name", "")).strip().lower() == "id":
                digits = re.sub(r"\D+", "", str(item.get("value", "")))
                return digits or None
        if props:
            first = props[0]
            if isinstance(first, dict):
                first = first.get("value", "")
            digits = re.sub(r"\D+", "", str(first))
            if digits:
                return digits
    return None


def _is_before_expired(best_before: str, now: datetime) -> bool:
    if not best_before:
        return False
    for fmt in ("%d-%m-%Y %H:%M:%S %z", "%Y-%m-%dT%H:%M:%S%z"):
        try:
            return datetime.strptime(best_before, fmt) <= now
        except ValueError:
            pass
    return False


def _validate_order_id(order_id: str) -> None:
    if not re.fullmatch(r"\d{3,40}", str(order_id)):
        raise CkassaPaymentError("Ckassa ID must contain 3 to 40 digits")


def _validate_phone(phone: str) -> None:
    if not re.fullmatch(r"\d{10,12}", phone):
        raise CkassaPaymentError("Ckassa PHONE must contain 10 to 12 digits")


def _validate_telegram_id(telegram_id: str) -> None:
    if not re.fullmatch(r"\d{5,10}", str(telegram_id)):
        raise CkassaPaymentError("Ckassa telegram_ID must contain 5 to 10 digits")
