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
import os
import shutil
import subprocess
import threading
from datetime import datetime
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler

from dotenv import load_dotenv

load_dotenv()

PORT = 9000
SECRET = os.getenv("WEBHOOK_SECRET", "")
PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
REMOTE = "origin"
BRANCH = "master"

BOT_DIR = "/home/bot"
BOT_VENV_PYTHON = os.path.join(BOT_DIR, "venv", "bin", "python3")
DEPLOY_BACKUP_DIR = os.getenv("DEPLOY_BACKUP_DIR", "/root/deploy-backup")
CODE_FILES = [
    "main.py",
    "mainAdmin.py",
    "max_publisher.py",
    "max_connector_server.py",
    "requirements.txt",
    "descriptions.json",
]

# Чтобы мусорные сканеры не могли подвесить однопоточный сервер:
SOCKET_TIMEOUT_SEC = 10               # молчуны и медленные клиенты отваливаются
MAX_BODY_BYTES = 5 * 1024 * 1024      # GitHub-пейлоады сильно меньше 5 МБ
LISTEN_BACKLOG = 64                   # default 5 переполнялся при атаках сканерами


def verify_signature(payload: bytes, signature: str) -> bool:
    if not SECRET:
        return True  # no secret configured — skip check
    expected = "sha256=" + hmac.new(
        SECRET.encode(), payload, hashlib.sha256
    ).hexdigest()
    return hmac.compare_digest(expected, signature)


def run_command(args: list[str], cwd: str = PROJECT_DIR) -> subprocess.CompletedProcess:
    result = subprocess.run(
        args,
        cwd=cwd,
        capture_output=True,
        text=True,
    )
    print(f"$ {' '.join(args)}", flush=True)
    if result.stdout:
        print(result.stdout, end="", flush=True)
    if result.stderr:
        print(result.stderr, end="", flush=True)
    return result


def git_lines(args: list[str]) -> list[str] | None:
    result = run_command(["git", *args])
    if result.returncode != 0:
        return None
    return [line.strip() for line in result.stdout.splitlines() if line.strip()]


def backup_conflicting_untracked_files() -> bool:
    tracked_files = set(git_lines(["ls-tree", "-r", "--name-only", f"{REMOTE}/{BRANCH}"]) or [])
    untracked_files = set(git_lines(["ls-files", "--others"]) or [])
    conflicts = sorted(tracked_files & untracked_files)
    if not conflicts:
        return True

    stamp = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
    backup_dir = os.path.join(DEPLOY_BACKUP_DIR, f"git-untracked-{stamp}")
    os.makedirs(backup_dir, exist_ok=True)

    for rel_path in conflicts:
        src = os.path.abspath(os.path.join(PROJECT_DIR, rel_path))
        project_root = os.path.abspath(PROJECT_DIR)
        if not (src == project_root or src.startswith(project_root + os.sep)):
            print(f"Refusing to back up path outside project: {rel_path}", flush=True)
            return False

        dst = os.path.join(backup_dir, rel_path)
        os.makedirs(os.path.dirname(dst), exist_ok=True)
        print(f"Backing up untracked conflict: {rel_path} -> {dst}", flush=True)
        shutil.move(src, dst)

    return True


def pull_latest() -> bool:
    fetch = run_command(["git", "fetch", REMOTE, BRANCH])
    if fetch.returncode != 0:
        return False
    if not backup_conflicting_untracked_files():
        return False
    pull = run_command(["git", "pull", "--ff-only", REMOTE, BRANCH])
    return pull.returncode == 0


def deploy():
    print("=== Deploying ===", flush=True)

    if not pull_latest():
        print("=== Deploy aborted: git pull failed; keeping current bot files ===", flush=True)
        return

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
    # Применяется автоматически в StreamRequestHandler.setup() к каждому соединению
    timeout = SOCKET_TIMEOUT_SEC

    def _reply(self, code: int, body: bytes = b""):
        try:
            self.send_response(code)
            self.end_headers()
            if body:
                self.wfile.write(body)
        except OSError:
            pass  # клиент уже отвалился

    def do_POST(self):
        try:
            length = int(self.headers.get("Content-Length", 0) or 0)
        except ValueError:
            self._reply(400, b"Bad Content-Length")
            return
        if length <= 0 or length > MAX_BODY_BYTES:
            self._reply(413, b"Payload too large or empty")
            return

        try:
            payload = self.rfile.read(length)
        except OSError:
            return  # таймаут/обрыв соединения

        if not verify_signature(payload, self.headers.get("X-Hub-Signature-256", "")):
            self._reply(403, b"Invalid signature")
            return

        event = self.headers.get("X-GitHub-Event", "")
        if event == "push":
            self._reply(200, b"Deploying...")
            threading.Thread(target=deploy, daemon=True).start()
        else:
            self._reply(200, f"Ignored event: {event}".encode())

    def do_GET(self):
        self._reply(200, b"Webhook listener is running")


class ReusableThreadingServer(ThreadingHTTPServer):
    allow_reuse_address = True
    allow_reuse_port = True
    daemon_threads = True              # не ждать висящие треды при shutdown
    request_queue_size = LISTEN_BACKLOG


if __name__ == "__main__":
    server = ReusableThreadingServer(("0.0.0.0", PORT), WebhookHandler)
    print(f"Webhook server listening on port {PORT}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("Shutting down")
        server.server_close()
