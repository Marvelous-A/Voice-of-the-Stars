import re
from os import getenv

import aiohttp


def strip_telegram_html(text: str) -> str:
    text = re.sub(r"</?[a-zA-Z][a-zA-Z0-9\-]*(?:\s[^>]*)?>", "", text or "")
    return text.strip()


def make_max_plain_text(text: str) -> str:
    text = strip_telegram_html(text)
    text = text.replace("@VoiceOfTheStarsBot", "Voice of the Stars")
    return text.strip()


def _split_message(text: str, limit: int = 3900) -> list[str]:
    if len(text) <= limit:
        return [text]

    chunks = []
    current = text
    while len(current) > limit:
        split_at = current.rfind("\n\n", 0, limit)
        if split_at < limit // 2:
            split_at = current.rfind("\n", 0, limit)
        if split_at < limit // 2:
            split_at = limit

        chunks.append(current[:split_at].strip())
        current = current[split_at:].strip()

    if current:
        chunks.append(current)
    return chunks


async def send_manual_preview(admin_bot, admin_id: int, post: dict) -> bool:
    if not admin_bot or not admin_id:
        return False

    text = make_max_plain_text(post.get("text", ""))
    image_url = post.get("image_url")
    body = (
        "Пост для MAX готов.\n\n"
        "Скопируй текст ниже и вставь его в MAX-канал:\n\n"
        f"{text}"
    )

    try:
        chunks = _split_message(body)
        if image_url:
            caption = chunks[0][:1024]
            await admin_bot.send_photo(admin_id, photo=image_url, caption=caption)
            remaining = chunks[0][1024:].strip()
            if remaining:
                await admin_bot.send_message(admin_id, remaining)
            for chunk in chunks[1:]:
                await admin_bot.send_message(admin_id, chunk)
        else:
            for chunk in chunks:
                await admin_bot.send_message(admin_id, chunk)
        return True
    except Exception as e:
        print(f"[max_manual] error: {e}")
        return False


async def post_to_connector(post: dict) -> bool:
    connector_url = getenv("MAX_CONNECTOR_URL", "")
    connector_token = getenv("MAX_CONNECTOR_TOKEN", "")
    if not connector_url:
        return False

    payload = {
        "text": make_max_plain_text(post.get("text", "")),
        "image_url": post.get("image_url"),
        "topic": post.get("topic_info", {}).get("topic"),
        "category": post.get("topic_info", {}).get("category"),
    }
    headers = {}
    if connector_token:
        headers["Authorization"] = f"Bearer {connector_token}"

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(connector_url, json=payload, headers=headers) as resp:
                if 200 <= resp.status < 300:
                    return True
                body = await resp.text()
                print(f"[max_connector] status={resp.status}, body={body[:500]}")
                return False
    except Exception as e:
        print(f"[max_connector] error: {e}")
        return False
