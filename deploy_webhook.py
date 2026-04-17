"""
GitHub Webhook receiver — auto-deploy on push.
Listens on port 9000, verifies the secret, runs git pull and restarts the bot.

Managed by systemd: bot.service and deploy-webhook.service
Set WEBHOOK_SECRET in .env (same value as in GitHub webhook settings).
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
VENV_PYTHON = os.path.join(PROJECT_DIR, "venv", "bin", "python3")


def verify_signature(payload: bytes, signature: str) -> bool:
    if not SECRET:
        return True  # no secret configured — skip check
    expected = "sha256=" + hmac.new(
        SECRET.encode(), payload, hashlib.sha256
    ).hexdigest()
    return hmac.compare_digest(expected, signature)


BOT_DIR = "/home/bot"
BOT_VENV_PYTHON = os.path.join(BOT_DIR, "venv", "bin", "python3")
CODE_FILES = ["main.py", "requirements.txt", "descriptions.json"]


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

    # restart bot via systemd
    subprocess.run(["systemctl", "restart", "tarot-bot.service"], check=False)
    print("=== Bot restarted ===", flush=True)


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
