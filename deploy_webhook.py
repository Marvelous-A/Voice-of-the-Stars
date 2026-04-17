"""
GitHub Webhook receiver — auto-deploy on push.
Listens on port 9000, verifies the secret, runs git pull and restarts the bot.

Managed by systemd: deploy-webhook.service
Set WEBHOOK_SECRET in .env (same value as in GitHub webhook settings).

Архитектура деплоя:
  /root/Voice-of-the-Stars/  — git-репозиторий (сюда приходит git pull)
  /home/bot/                  — рабочая папка бота (сюда копируются файлы)
  tarot-bot.service           — systemd-сервис бота (единственный!)
  deploy-webhook.service      — systemd-сервис этого вебхука
"""

import hashlib
import hmac
import json
import os
import socket
import subprocess
import sys
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler

from dotenv import load_dotenv

load_dotenv()

PORT = 9000
SECRET = os.getenv("WEBHOOK_SECRET", "")
PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))

BOT_DIR = "/home/bot"
BOT_VENV_PYTHON = os.path.join(BOT_DIR, "venv", "bin", "python3")
CODE_FILES = ["main.py", "mainAdmin.py", "requirements.txt", "descriptions.json"]


def verify_signature(payload: bytes, signature: str) -> bool:
    if not SECRET:
        return True  # no secret configured — skip check
    expected = "sha256=" + hmac.new(
        SECRET.encode(), payload, hashlib.sha256
    ).hexdigest()
    return hmac.compare_digest(expected, signature)


def deploy():
    print("=== Deploying ===", flush=True)

    # git pull
    result = subprocess.run(
        ["git", "pull", "origin", "master"],
        cwd=PROJECT_DIR,
        capture_output=True,
        text=True,
    )
    print(result.stdout, result.stderr, flush=True)

    # copy code files to bot directory
    for fname in CODE_FILES:
        src = os.path.join(PROJECT_DIR, fname)
        dst = os.path.join(BOT_DIR, fname)
        if os.path.exists(src):
            subprocess.run(["cp", src, dst], check=False)
            print(f"Copied {fname} -> {BOT_DIR}", flush=True)

    # install new dependencies (if any)
    subprocess.run(
        [BOT_VENV_PYTHON, "-m", "pip", "install", "-r",
         os.path.join(BOT_DIR, "requirements.txt"), "-q"],
        cwd=BOT_DIR,
        capture_output=True,
        text=True,
    )

    # restart bots via systemd
    subprocess.run(["systemctl", "restart", "tarot-bot.service"], check=False)
    subprocess.run(["systemctl", "restart", "tarot-admin.service"], check=False)
    print("=== Bots restarted ===", flush=True)

    # if deploy_webhook.py itself was updated — restart webhook service
    src_wh = os.path.join(PROJECT_DIR, "deploy_webhook.py")
    dst_wh = os.path.join(PROJECT_DIR, "deploy_webhook.py")
    # always restart webhook so it picks up any changes
    subprocess.run(["systemctl", "restart", "deploy-webhook.service"], check=False)
    print("=== Webhook restarted ===", flush=True)


class WebhookHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        payload = self.rfile.read(length)
        signature = self.headers.get("X-Hub-Signature-256", "")

        if not verify_signature(payload, signature):
            self.send_response(403)
            self.end_headers()
            self.wfile.write(b"Invalid signature")
            return

        event = self.headers.get("X-GitHub-Event", "")
        if event == "push":
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"Deploying...")
            threading.Thread(target=deploy, daemon=True).start()
        else:
            self.send_response(200)
            self.end_headers()
            self.wfile.write(f"Ignored event: {event}".encode())

    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"Webhook listener is running")


class ReusableHTTPServer(HTTPServer):
    allow_reuse_address = True
    allow_reuse_port = True


if __name__ == "__main__":
    server = ReusableHTTPServer(("0.0.0.0", PORT), WebhookHandler)
    print(f"Webhook server listening on port {PORT}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("Shutting down")
        server.server_close()
