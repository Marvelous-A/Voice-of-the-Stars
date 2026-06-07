from __future__ import annotations

import asyncio
import html
import json
import mimetypes
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence

import aiohttp


VK_API_URL = "https://api.vk.com/method/{method}"
DEFAULT_VK_API_VERSION = "5.199"

_HTML_BREAK_RE = re.compile(r"<br\s*/?>", re.IGNORECASE)
_HTML_TAG_RE = re.compile(r"<[^>]+>")


class VKPublisherError(RuntimeError):
    """Base exception for VK publishing failures."""


class VKPublisherConfigError(VKPublisherError):
    """Raised when VK publishing env config is incomplete or invalid."""


class VKAPIError(VKPublisherError):
    """Raised when VK API returns an error object."""

    def __init__(self, method: str, error: Mapping[str, Any]):
        self.method = method
        self.code = error.get("error_code")
        self.api_message = str(error.get("error_msg") or "VK API error")
        super().__init__(f"{method}: VK API error {self.code}: {self.api_message}")


class VKHTTPError(VKPublisherError):
    def __init__(self, label: str, status: int, body: str):
        self.label = label
        self.status = status
        self.body = body
        super().__init__(f"{label}: HTTP {status}: {body[:500]}")


@dataclass(frozen=True)
class VKPublisherConfig:
    access_token: str
    group_id: int
    api_version: str = DEFAULT_VK_API_VERSION
    proxy_url: str = ""
    timeout_sec: float = 30.0
    from_group: bool = True
    signed: bool = False
    allow_text_fallback: bool = True

    @classmethod
    def from_env(cls) -> "VKPublisherConfig":
        access_token = (os.getenv("VK_ACCESS_TOKEN") or "").strip()
        group_id = _parse_group_id(os.getenv("VK_GROUP_ID") or "")
        api_version = (os.getenv("VK_API_VERSION") or DEFAULT_VK_API_VERSION).strip()
        proxy_url = (os.getenv("VK_PROXY_URL") or "").strip()
        timeout_sec = _parse_timeout(os.getenv("VK_TIMEOUT_SEC") or "30")
        signed = _env_bool("VK_SIGNED", default=False)
        allow_text_fallback = _env_bool("VK_ALLOW_TEXT_FALLBACK", default=True)

        if not access_token:
            raise VKPublisherConfigError("VK_ACCESS_TOKEN is empty")
        if not group_id:
            raise VKPublisherConfigError("VK_GROUP_ID is empty or invalid")

        return cls(
            access_token=access_token,
            group_id=group_id,
            api_version=api_version,
            proxy_url=proxy_url,
            timeout_sec=timeout_sec,
            signed=signed,
            allow_text_fallback=allow_text_fallback,
        )


@dataclass(frozen=True)
class VKPostResult:
    owner_id: int
    post_id: int
    attachments: tuple[str, ...]
    raw_response: Mapping[str, Any]

    @property
    def wall_id(self) -> str:
        return f"wall{self.owner_id}_{self.post_id}"


@dataclass(frozen=True)
class VKPublishAttempt:
    ok: bool
    wall_id: str = ""
    error: str = ""


def is_vk_configured() -> bool:
    return bool((os.getenv("VK_ACCESS_TOKEN") or "").strip() and _parse_group_id(os.getenv("VK_GROUP_ID") or ""))


def telegram_html_to_vk_text(text: str) -> str:
    text = str(text or "")
    text = _HTML_BREAK_RE.sub("\n", text)
    text = _HTML_TAG_RE.sub("", text)
    return html.unescape(text).strip()


async def post_channel_payload_to_vk(post: Mapping[str, Any]) -> VKPostResult:
    """Publish the same payload shape that main.build_channel_post() returns."""
    async with VKPublisher.from_env() as publisher:
        return await publisher.post(
            message=str(post.get("text") or ""),
            image_path=str(post.get("image_path") or ""),
        )


async def try_post_channel_payload_to_vk(post: Mapping[str, Any]) -> bool:
    return (await post_channel_payload_to_vk_attempt(post)).ok


async def post_channel_payload_to_vk_attempt(post: Mapping[str, Any]) -> VKPublishAttempt:
    try:
        result = await post_channel_payload_to_vk(post)
        print(f"[vk_autoposting] published {result.wall_id}")
        return VKPublishAttempt(ok=True, wall_id=result.wall_id)
    except Exception as exc:
        error = _safe_error_text(exc)
        print(f"[vk_autoposting] publish error: {error}")
        return VKPublishAttempt(ok=False, error=error)


class VKPublisher:
    def __init__(self, config: VKPublisherConfig, session: aiohttp.ClientSession | None = None):
        self.config = config
        self._session = session
        self._owns_session = session is None

    @classmethod
    def from_env(cls) -> "VKPublisher":
        return cls(VKPublisherConfig.from_env())

    async def __aenter__(self) -> "VKPublisher":
        await self._get_session()
        return self

    async def __aexit__(self, exc_type, exc, traceback) -> None:
        await self.close()

    async def close(self) -> None:
        if self._owns_session and self._session and not self._session.closed:
            await self._session.close()

    async def post(
        self,
        message: str,
        image_path: str = "",
        attachments: Sequence[str] | None = None,
        publish_date: int | None = None,
        guid: str = "",
    ) -> VKPostResult:
        text = telegram_html_to_vk_text(message)
        attachment_items = [item.strip() for item in (attachments or ()) if str(item).strip()]

        if image_path:
            try:
                attachment_items.append(await self.upload_wall_photo(image_path))
            except Exception as exc:
                if not text or not self.config.allow_text_fallback:
                    raise
                print(f"[vk_autoposting] photo upload failed, publishing text only: {_safe_error_text(exc)}")

        if not text and not attachment_items:
            raise VKPublisherError("VK post must contain text or attachments")

        owner_id = -abs(self.config.group_id)
        params: dict[str, Any] = {
            "owner_id": owner_id,
            "from_group": 1 if self.config.from_group else 0,
            "message": text,
            "attachments": ",".join(attachment_items),
            "signed": 1 if self.config.signed else 0,
        }
        if publish_date:
            params["publish_date"] = int(publish_date)
        if guid:
            params["guid"] = guid

        response = await self._api("wall.post", _drop_empty(params))
        post_id = int(response.get("post_id") or 0)
        if not post_id:
            raise VKPublisherError(f"wall.post returned no post_id: {_safe_json(response)}")

        return VKPostResult(
            owner_id=owner_id,
            post_id=post_id,
            attachments=tuple(attachment_items),
            raw_response=response,
        )

    async def upload_wall_photo(self, image_path: str) -> str:
        path = Path(image_path)
        if not path.is_file():
            raise VKPublisherError(f"VK photo does not exist: {image_path}")

        upload_server = await self._api(
            "photos.getWallUploadServer",
            {"group_id": abs(self.config.group_id)},
        )
        upload_url = str(upload_server.get("upload_url") or "")
        if not upload_url:
            raise VKPublisherError("photos.getWallUploadServer returned no upload_url")

        uploaded = await self._upload_photo(upload_url, path)
        saved = await self._api(
            "photos.saveWallPhoto",
            {
                "group_id": abs(self.config.group_id),
                "photo": uploaded.get("photo"),
                "server": uploaded.get("server"),
                "hash": uploaded.get("hash"),
            },
        )
        photo = _first_photo(saved)
        return _photo_attachment(photo, fallback_owner_id=-abs(self.config.group_id))

    async def _api(self, method: str, params: Mapping[str, Any] | None = None) -> Any:
        session = await self._get_session()
        data = {
            "access_token": self.config.access_token,
            "v": self.config.api_version,
        }
        data.update({key: value for key, value in (params or {}).items() if value is not None})

        request_kwargs = self._request_kwargs()
        url = VK_API_URL.format(method=method)
        async with session.post(url, data=data, **request_kwargs) as response:
            payload = await _read_json_response(response, method)

        if isinstance(payload, Mapping) and "error" in payload:
            raise VKAPIError(method, payload["error"])
        if not isinstance(payload, Mapping) or "response" not in payload:
            raise VKPublisherError(f"{method}: unexpected VK response: {_safe_json(payload)}")
        return payload["response"]

    async def _upload_photo(self, upload_url: str, image_path: Path) -> Mapping[str, Any]:
        session = await self._get_session()
        content_type = mimetypes.guess_type(image_path.name)[0] or "image/jpeg"
        request_kwargs = self._request_kwargs()

        payload = None
        last_error = None
        for attempt in range(1, 4):
            try:
                with image_path.open("rb") as file_obj:
                    form = aiohttp.FormData()
                    form.add_field(
                        "photo",
                        file_obj,
                        filename=image_path.name,
                        content_type=content_type,
                    )
                    async with session.post(upload_url, data=form, **request_kwargs) as response:
                        payload = await _read_json_response(response, "VK photo upload")
                break
            except VKHTTPError as exc:
                last_error = exc
                if exc.status < 500 and exc.status != 429:
                    raise
                if attempt >= 3:
                    raise
                await asyncio.sleep(1.5 * attempt)
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                last_error = exc
                if attempt >= 3:
                    raise VKPublisherError(f"VK photo upload failed after retries: {exc}") from exc
                await asyncio.sleep(1.5 * attempt)

        if payload is None:
            raise VKPublisherError(f"VK photo upload returned no response: {last_error}")

        if isinstance(payload, Mapping) and "error" in payload:
            raise VKPublisherError(f"VK photo upload error: {_safe_json(payload['error'])}")
        if not isinstance(payload, Mapping):
            raise VKPublisherError(f"VK photo upload returned invalid response: {_safe_json(payload)}")
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
        raise VKHTTPError(label, response.status, text)
    try:
        return json.loads(text)
    except json.JSONDecodeError as exc:
        raise VKPublisherError(f"{label}: invalid JSON response: {text[:500]}") from exc


def _first_photo(response: Any) -> Mapping[str, Any]:
    if isinstance(response, Sequence) and not isinstance(response, (str, bytes)) and response:
        photo = response[0]
    elif isinstance(response, Mapping):
        photo = response
    else:
        photo = None

    if not isinstance(photo, Mapping):
        raise VKPublisherError(f"photos.saveWallPhoto returned no photo object: {_safe_json(response)}")
    return photo


def _photo_attachment(photo: Mapping[str, Any], fallback_owner_id: int) -> str:
    owner_id = int(photo.get("owner_id") or fallback_owner_id)
    photo_id = int(photo.get("id") or 0)
    if not photo_id:
        raise VKPublisherError(f"VK photo object has no id: {_safe_json(photo)}")
    access_key = str(photo.get("access_key") or "").strip()
    attachment = f"photo{owner_id}_{photo_id}"
    if access_key:
        attachment = f"{attachment}_{access_key}"
    return attachment


def _parse_group_id(raw_value: str) -> int:
    value = str(raw_value or "").strip()
    value = value.removeprefix("club").removeprefix("public")
    if not value:
        return 0
    try:
        return abs(int(value))
    except ValueError:
        return 0


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


def _drop_empty(params: Mapping[str, Any]) -> dict[str, Any]:
    return {
        key: value
        for key, value in params.items()
        if value is not None and value != "" and value != []
    }


def _safe_json(value: Any) -> str:
    try:
        return json.dumps(value, ensure_ascii=False)[:1000]
    except TypeError:
        return str(value)[:1000]


def _safe_error_text(error: Exception) -> str:
    text = str(error)
    token = os.getenv("VK_ACCESS_TOKEN") or ""
    if token:
        text = text.replace(token, "<secret>")
    return text[:1000]
