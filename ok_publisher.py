from __future__ import annotations

import asyncio
import hashlib
import html
import json
import mimetypes
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

import aiohttp


OK_API_URL = "https://api.ok.ru/fb.do"
DEFAULT_OK_FORMAT = "json"

_HTML_BREAK_RE = re.compile(r"<br\s*/?>", re.IGNORECASE)
_HTML_TAG_RE = re.compile(r"<[^>]+>")


class OKPublisherError(RuntimeError):
    """Base exception for OK publishing failures."""


class OKPublisherConfigError(OKPublisherError):
    """Raised when OK publishing env config is incomplete or invalid."""


class OKAPIError(OKPublisherError):
    """Raised when OK API returns an error object."""

    def __init__(self, method: str, error: Mapping[str, Any]):
        self.method = method
        self.code = error.get("error_code") or error.get("code")
        self.api_message = str(error.get("error_msg") or error.get("message") or "OK API error")
        super().__init__(f"{method}: OK API error {self.code}: {self.api_message}")


class OKHTTPError(OKPublisherError):
    def __init__(self, label: str, status: int, body: str):
        self.label = label
        self.status = status
        self.body = body
        super().__init__(f"{label}: HTTP {status}: {body[:500]}")


@dataclass(frozen=True)
class OKPublisherConfig:
    application_key: str
    secret_key: str
    group_id: str
    session_key: str = ""
    access_token: str = ""
    proxy_url: str = ""
    timeout_sec: float = 30.0
    text_only: bool = False
    allow_text_fallback: bool = True
    on_behalf_of_group: bool = True

    @classmethod
    def from_env(cls) -> "OKPublisherConfig":
        application_key = (os.getenv("OK_APPLICATION_KEY") or "").strip()
        group_id = _parse_group_id(os.getenv("OK_GROUP_ID") or "")
        session_key = (os.getenv("OK_SESSION_KEY") or "").strip()
        access_token = (os.getenv("OK_ACCESS_TOKEN") or "").strip()
        session_secret_key = (os.getenv("OK_SESSION_SECRET_KEY") or "").strip()
        application_secret_key = (os.getenv("OK_APPLICATION_SECRET_KEY") or "").strip()
        proxy_url = (os.getenv("OK_PROXY_URL") or "").strip()
        timeout_sec = _parse_timeout(os.getenv("OK_TIMEOUT_SEC") or "30")
        text_only = _env_bool("OK_TEXT_ONLY", default=False)
        allow_text_fallback = _env_bool("OK_ALLOW_TEXT_FALLBACK", default=True)
        on_behalf_of_group = _env_bool("OK_ON_BEHALF_OF_GROUP", default=True)

        if not application_key:
            raise OKPublisherConfigError("OK_APPLICATION_KEY is empty")
        if not group_id:
            raise OKPublisherConfigError("OK_GROUP_ID is empty or invalid")
        if not session_key and not access_token:
            raise OKPublisherConfigError("OK_SESSION_KEY or OK_ACCESS_TOKEN is empty")

        secret_key = session_secret_key
        if not secret_key and access_token and application_secret_key:
            secret_key = hashlib.md5(f"{access_token}{application_secret_key}".encode("utf-8")).hexdigest()
        if not secret_key:
            raise OKPublisherConfigError("OK_SESSION_SECRET_KEY is empty")

        return cls(
            application_key=application_key,
            secret_key=secret_key,
            group_id=group_id,
            session_key=session_key,
            access_token=access_token,
            proxy_url=proxy_url,
            timeout_sec=timeout_sec,
            text_only=text_only,
            allow_text_fallback=allow_text_fallback,
            on_behalf_of_group=on_behalf_of_group,
        )


@dataclass(frozen=True)
class OKPostResult:
    topic_id: str
    raw_response: Any


@dataclass(frozen=True)
class OKPublishAttempt:
    ok: bool
    topic_id: str = ""
    error: str = ""


def is_ok_configured() -> bool:
    return bool(
        (os.getenv("OK_APPLICATION_KEY") or "").strip()
        and _parse_group_id(os.getenv("OK_GROUP_ID") or "")
        and ((os.getenv("OK_SESSION_KEY") or "").strip() or (os.getenv("OK_ACCESS_TOKEN") or "").strip())
        and (
            (os.getenv("OK_SESSION_SECRET_KEY") or "").strip()
            or ((os.getenv("OK_ACCESS_TOKEN") or "").strip() and (os.getenv("OK_APPLICATION_SECRET_KEY") or "").strip())
        )
    )


def telegram_html_to_ok_text(text: str) -> str:
    text = str(text or "")
    text = _HTML_BREAK_RE.sub("\n", text)
    text = _HTML_TAG_RE.sub("", text)
    return html.unescape(text).strip()


async def post_channel_payload_to_ok(post: Mapping[str, Any]) -> OKPostResult:
    """Publish the same payload shape that main.build_channel_post() returns."""
    async with OKPublisher.from_env() as publisher:
        return await publisher.post(
            message=str(post.get("text") or ""),
            image_path=str(post.get("image_path") or ""),
        )


async def try_post_channel_payload_to_ok(post: Mapping[str, Any]) -> bool:
    return (await post_channel_payload_to_ok_attempt(post)).ok


async def post_channel_payload_to_ok_attempt(post: Mapping[str, Any]) -> OKPublishAttempt:
    try:
        result = await post_channel_payload_to_ok(post)
        print(f"[ok_autoposting] published {result.topic_id}")
        return OKPublishAttempt(ok=True, topic_id=result.topic_id)
    except Exception as exc:
        error = _safe_error_text(exc)
        print(f"[ok_autoposting] publish error: {error}")
        return OKPublishAttempt(ok=False, error=error)


class OKPublisher:
    def __init__(self, config: OKPublisherConfig, session: aiohttp.ClientSession | None = None):
        self.config = config
        self._session = session
        self._owns_session = session is None

    @classmethod
    def from_env(cls) -> "OKPublisher":
        return cls(OKPublisherConfig.from_env())

    async def __aenter__(self) -> "OKPublisher":
        await self._get_session()
        return self

    async def __aexit__(self, exc_type, exc, traceback) -> None:
        await self.close()

    async def close(self) -> None:
        if self._owns_session and self._session and not self._session.closed:
            await self._session.close()

    async def post(self, message: str, image_path: str = "") -> OKPostResult:
        text = telegram_html_to_ok_text(message)
        media: list[dict[str, str]] = []

        if text:
            media.append({"type": "text", "text": text})

        if image_path and not self.config.text_only:
            try:
                photo_token = await self.upload_photo(image_path)
                media.append({"type": "photo", "list": [{"id": photo_token}]})
            except Exception as exc:
                if not text or not self.config.allow_text_fallback:
                    raise
                print(f"[ok_autoposting] photo upload failed, publishing text only: {_safe_error_text(exc)}")
        elif image_path and self.config.text_only:
            print("[ok_autoposting] OK_TEXT_ONLY=1, publishing without photo")

        if not media:
            raise OKPublisherError("OK post must contain text or photo")

        attachment = {
            "media": media,
            "onBehalfOfGroup": "true" if self.config.on_behalf_of_group else "false",
        }
        response = await self._api(
            "mediatopic.post",
            {
                "type": "GROUP_THEME",
                "gid": self.config.group_id,
                "attachment": json.dumps(attachment, ensure_ascii=False, separators=(",", ":")),
            },
        )
        topic_id = _extract_topic_id(response)
        return OKPostResult(topic_id=topic_id, raw_response=response)

    async def upload_photo(self, image_path: str) -> str:
        path = Path(image_path)
        if not path.is_file():
            raise OKPublisherError(f"OK photo does not exist: {image_path}")

        upload_info = await self._api("photosV2.getUploadUrl", {"gid": self.config.group_id, "count": 1})
        upload_url = _extract_upload_url(upload_info)
        uploaded = await self._upload_photo(upload_url, path)
        return _extract_photo_token(uploaded)

    async def _api(self, method: str, params: Mapping[str, Any] | None = None) -> Any:
        session = await self._get_session()
        data: dict[str, str] = {
            "method": method,
            "application_key": self.config.application_key,
            "format": DEFAULT_OK_FORMAT,
        }
        if self.config.session_key:
            data["session_key"] = self.config.session_key
        else:
            data["access_token"] = self.config.access_token

        for key, value in (params or {}).items():
            if value is not None and value != "":
                data[key] = _stringify_api_value(value)

        data["sig"] = _calc_ok_signature(data, self.config.secret_key)

        async with session.post(OK_API_URL, data=data, **self._request_kwargs()) as response:
            payload = await _read_json_response(response, method)

        if isinstance(payload, Mapping) and "error_code" in payload:
            raise OKAPIError(method, payload)
        if isinstance(payload, Mapping) and isinstance(payload.get("error"), Mapping):
            raise OKAPIError(method, payload["error"])
        return payload

    async def _upload_photo(self, upload_url: str, image_path: Path) -> Mapping[str, Any]:
        session = await self._get_session()
        content_type = mimetypes.guess_type(image_path.name)[0] or "image/jpeg"

        payload = None
        last_error = None
        for attempt in range(1, 4):
            try:
                with image_path.open("rb") as file_obj:
                    form = aiohttp.FormData()
                    form.add_field(
                        "pic1",
                        file_obj,
                        filename=image_path.name,
                        content_type=content_type,
                    )
                    async with session.post(upload_url, data=form, **self._request_kwargs()) as response:
                        payload = await _read_json_response(response, "OK photo upload")
                break
            except OKHTTPError as exc:
                last_error = exc
                if exc.status < 500 and exc.status != 429:
                    raise
                if attempt >= 3:
                    raise
                await asyncio.sleep(1.5 * attempt)
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                last_error = exc
                if attempt >= 3:
                    raise OKPublisherError(f"OK photo upload failed after retries: {exc}") from exc
                await asyncio.sleep(1.5 * attempt)

        if payload is None:
            raise OKPublisherError(f"OK photo upload returned no response: {last_error}")
        if not isinstance(payload, Mapping):
            raise OKPublisherError(f"OK photo upload returned invalid response: {_safe_json(payload)}")
        if "error_code" in payload:
            raise OKAPIError("OK photo upload", payload)
        return payload

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=self.config.timeout_sec)
            self._session = aiohttp.ClientSession(timeout=timeout)
        return self._session

    def _request_kwargs(self) -> dict[str, Any]:
        if self.config.proxy_url:
            return {"proxy": self.config.proxy_url}
        return {}


async def _read_json_response(response: aiohttp.ClientResponse, label: str) -> Any:
    text = await response.text()
    if response.status >= 400:
        raise OKHTTPError(label, response.status, text)
    try:
        return json.loads(text)
    except json.JSONDecodeError as exc:
        raise OKPublisherError(f"{label}: invalid JSON response: {text[:500]}") from exc


def _calc_ok_signature(data: Mapping[str, str], secret_key: str) -> str:
    items = sorted(
        (key, value)
        for key, value in data.items()
        if key not in {"sig", "session_key", "access_token"}
    )
    raw = "".join(f"{key}={value}" for key, value in items) + secret_key
    return hashlib.md5(raw.encode("utf-8")).hexdigest()


def _stringify_api_value(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (Mapping, list, tuple)):
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    return str(value)


def _extract_upload_url(response: Any) -> str:
    if not isinstance(response, Mapping):
        raise OKPublisherError(f"photosV2.getUploadUrl returned invalid response: {_safe_json(response)}")
    upload_url = str(response.get("upload_url") or response.get("url") or "")
    if not upload_url:
        raise OKPublisherError(f"photosV2.getUploadUrl returned no upload_url: {_safe_json(response)}")
    return upload_url


def _extract_photo_token(response: Mapping[str, Any]) -> str:
    photos = response.get("photos")
    if isinstance(photos, Mapping):
        for photo in photos.values():
            if isinstance(photo, Mapping):
                token = str(photo.get("token") or "")
                if token:
                    return token

    token = str(response.get("token") or response.get("photo_token") or "")
    if token:
        return token

    raise OKPublisherError(f"OK photo upload returned no photo token: {_safe_json(response)}")


def _extract_topic_id(response: Any) -> str:
    if isinstance(response, Mapping):
        for key in ("id", "topic_id", "media_topic_id", "tid"):
            value = str(response.get(key) or "").strip()
            if value:
                return value
    value = str(response or "").strip()
    if value:
        return value
    raise OKPublisherError(f"mediatopic.post returned empty response: {_safe_json(response)}")


def _parse_group_id(raw_value: str) -> str:
    value = str(raw_value or "").strip()
    value = value.removeprefix("group").strip()
    if not value:
        return ""
    return value if value.isdigit() else ""


def _parse_timeout(raw_value: str) -> float:
    try:
        timeout = float(raw_value)
    except ValueError:
        return 30.0
    return max(1.0, timeout)


def _env_bool(name: str, default: bool = False) -> bool:
    raw = (os.getenv(name) or "").strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "y", "on"}


def _safe_json(value: Any) -> str:
    try:
        return json.dumps(value, ensure_ascii=False)[:1000]
    except TypeError:
        return str(value)[:1000]


def _safe_error_text(error: Exception) -> str:
    text = str(error)
    for secret in (
        os.getenv("OK_ACCESS_TOKEN") or "",
        os.getenv("OK_SESSION_KEY") or "",
        os.getenv("OK_SESSION_SECRET_KEY") or "",
        os.getenv("OK_APPLICATION_SECRET_KEY") or "",
    ):
        if secret:
            text = text.replace(secret, "<secret>")
    return text[:1000]
