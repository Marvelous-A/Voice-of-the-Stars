import json
import html
from datetime import datetime, timezone
from pathlib import Path
from os import getenv

from aiohttp import web
from dotenv import load_dotenv


load_dotenv()

HOST = getenv("MAX_CONNECTOR_HOST", "0.0.0.0")
PORT = int(getenv("MAX_CONNECTOR_PORT", "8090"))
TOKEN = getenv("MAX_CONNECTOR_TOKEN", "")
POSTS_FILE = Path(getenv("MAX_CONNECTOR_POSTS_FILE", "max_connector_posts.json"))


def load_posts() -> list[dict]:
    try:
        return json.loads(POSTS_FILE.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError):
        return []


def save_posts(posts: list[dict]) -> None:
    POSTS_FILE.write_text(json.dumps(posts, ensure_ascii=False, indent=2), encoding="utf-8")


def check_auth(request: web.Request) -> bool:
    if not TOKEN:
        return True
    expected = f"Bearer {TOKEN}"
    return request.headers.get("Authorization") == expected


async def health(_: web.Request) -> web.Response:
    return web.json_response({"ok": True})


async def publish(request: web.Request) -> web.Response:
    if not check_auth(request):
        return web.json_response({"ok": False, "error": "unauthorized"}, status=401)

    try:
        data = await request.json()
    except json.JSONDecodeError:
        return web.json_response({"ok": False, "error": "invalid json"}, status=400)

    text = str(data.get("text") or "").strip()
    image_url = str(data.get("image_url") or "").strip()
    if not text:
        return web.json_response({"ok": False, "error": "text is required"}, status=400)

    post = {
        "id": datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S%f"),
        "created_at": datetime.now(timezone.utc).isoformat(),
        "text": text,
        "image_url": image_url,
        "topic": data.get("topic"),
        "category": data.get("category"),
    }

    posts = load_posts()
    posts.insert(0, post)
    save_posts(posts[:200])
    return web.json_response({"ok": True, "id": post["id"]})


async def list_posts(_: web.Request) -> web.Response:
    posts = load_posts()
    cards = []
    for post in posts:
        safe_text = html.escape(post.get("text", ""))
        safe_image_url = html.escape(post.get("image_url", ""))
        safe_created_at = html.escape(post.get("created_at", ""))
        image_block = ""
        if safe_image_url:
            image_block = (
                f'<a class="image-link" href="{safe_image_url}" target="_blank">Open image</a>'
                f'<img src="{safe_image_url}" alt="">'
            )

        cards.append(
            f"""
            <article class="post">
                <div class="meta">{safe_created_at}</div>
                {image_block}
                <textarea readonly>{safe_text}</textarea>
                <button onclick="copyText(this)">Copy text</button>
            </article>
            """
        )

    body = "\n".join(cards) or '<p class="empty">No posts yet.</p>'
    page = f"""
    <!doctype html>
    <html lang="ru">
    <head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <title>MAX Connector Queue</title>
        <style>
            body {{
                margin: 0;
                font-family: Arial, sans-serif;
                color: #1f2933;
                background: #f5f7fb;
            }}
            header {{
                padding: 20px;
                background: #ffffff;
                border-bottom: 1px solid #d9e2ec;
            }}
            main {{
                max-width: 880px;
                margin: 0 auto;
                padding: 20px;
            }}
            .post {{
                margin-bottom: 18px;
                padding: 16px;
                background: #ffffff;
                border: 1px solid #d9e2ec;
                border-radius: 8px;
            }}
            .meta {{
                margin-bottom: 10px;
                color: #627d98;
                font-size: 13px;
            }}
            img {{
                display: block;
                max-width: 100%;
                max-height: 360px;
                margin: 10px 0;
                border-radius: 6px;
            }}
            textarea {{
                box-sizing: border-box;
                width: 100%;
                min-height: 180px;
                padding: 12px;
                resize: vertical;
                border: 1px solid #bcccdc;
                border-radius: 6px;
                font: 15px/1.5 Arial, sans-serif;
                color: #102a43;
                background: #f8fafc;
            }}
            button, .image-link {{
                display: inline-block;
                margin-top: 10px;
                padding: 9px 12px;
                border: 0;
                border-radius: 6px;
                color: #ffffff;
                background: #0b69a3;
                font-size: 14px;
                text-decoration: none;
                cursor: pointer;
            }}
            .image-link {{
                margin-right: 8px;
                background: #486581;
            }}
            .empty {{
                color: #627d98;
            }}
        </style>
    </head>
    <body>
        <header>
            <h1>MAX Connector Queue</h1>
        </header>
        <main>{body}</main>
        <script>
            async function copyText(button) {{
                const text = button.parentElement.querySelector("textarea").value;
                await navigator.clipboard.writeText(text);
                const old = button.textContent;
                button.textContent = "Copied";
                setTimeout(() => button.textContent = old, 1400);
            }}
        </script>
    </body>
    </html>
    """
    return web.Response(text=page, content_type="text/html")


app = web.Application()
app.router.add_get("/health", health)
app.router.add_post("/publish", publish)
app.router.add_get("/", list_posts)


if __name__ == "__main__":
    print(f"MAX connector server: http://{HOST}:{PORT}")
    web.run_app(app, host=HOST, port=PORT)
