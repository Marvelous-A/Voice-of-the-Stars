"""GitHub webhook for automatic ECHO bot deployments."""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import os
import sqlite3
import subprocess
import tempfile
import threading
import time
from contextlib import closing
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib import error as urlerror
from urllib import request as urlrequest

from dotenv import dotenv_values, load_dotenv


PROJECT_DIR = Path(__file__).resolve().parent
load_dotenv(PROJECT_DIR / ".env")


def load_admin_bot_token() -> str:
    token = os.getenv("ADMIN_BOT_TOKEN", "").strip()
    if token:
        return token
    # На сервере админ-бот и Voice используют общий защищённый env-файл.
    shared_env = dotenv_values("/home/bot/.env")
    return str(shared_env.get("ADMIN_BOT_TOKEN") or "").strip()

PORT = int(os.getenv("DEPLOY_PORT", "9001"))
SECRET = os.getenv("WEBHOOK_SECRET", "").encode()
REPOSITORY = os.getenv("GITHUB_REPOSITORY", "Marvelous-A/echo-dialog-bot")
BRANCH = os.getenv("DEPLOY_BRANCH", "main")
BOT_SERVICE = os.getenv("BOT_SERVICE", "echo-dialog-bot.service")
WEBHOOK_SERVICE = os.getenv("WEBHOOK_SERVICE", "echo-deploy-webhook.service")
MAX_BODY_BYTES = 1_048_576
COMMAND_TIMEOUT = 600
ADMIN_BOT_TOKEN = load_admin_bot_token()
DATABASE_PATH = Path(os.getenv("DATABASE_PATH", str(PROJECT_DIR / "echo.db")))
NOTIFY_CHAT_IDS = tuple(
    int(value.strip())
    for value in os.getenv("DEPLOY_NOTIFY_CHAT_IDS", os.getenv("ADMIN_IDS", "")).split(",")
    if value.strip().isdigit()
)
NOTIFY_USERNAMES = tuple(
    value.strip().lower().removeprefix("@")
    for value in os.getenv("DEPLOY_NOTIFY_USERNAMES", "bimbim2bambam").split(",")
    if value.strip()
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("echo-deploy")

_state_lock = threading.Lock()
_worker_running = False
_deploy_pending = False


class DeployError(RuntimeError):
    pass


def run_command(
    args: list[str],
    *,
    cwd: Path = PROJECT_DIR,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    logger.info("$ %s", " ".join(args))
    result = subprocess.run(
        args,
        cwd=cwd,
        capture_output=True,
        text=True,
        timeout=COMMAND_TIMEOUT,
    )
    if result.stdout.strip():
        logger.info(result.stdout.rstrip())
    if result.stderr.strip():
        logger.warning(result.stderr.rstrip())
    if check and result.returncode != 0:
        raise DeployError(f"Command failed ({result.returncode}): {' '.join(args)}")
    return result


def git_output(*args: str) -> str:
    return run_command(["git", *args]).stdout.strip()


def resolve_notify_chat_ids(
    database_path: Path = DATABASE_PATH,
    configured_ids: tuple[int, ...] = NOTIFY_CHAT_IDS,
    usernames: tuple[str, ...] = NOTIFY_USERNAMES,
) -> list[int]:
    chat_ids = set(configured_ids)
    if not usernames or not database_path.exists():
        return sorted(chat_ids)

    placeholders = ",".join("?" for _ in usernames)
    try:
        with closing(sqlite3.connect(database_path, timeout=5)) as connection:
            rows = connection.execute(
                f"SELECT user_id FROM users WHERE lower(username) IN ({placeholders})",
                usernames,
            ).fetchall()
        chat_ids.update(int(row[0]) for row in rows)
    except sqlite3.Error as error:
        logger.error("Could not resolve deploy notification recipients: %s", error)
    return sorted(chat_ids)


def send_telegram_notification(message: str) -> int:
    if not ADMIN_BOT_TOKEN:
        logger.error("Deploy notification skipped: ADMIN_BOT_TOKEN is missing")
        return 0

    chat_ids = resolve_notify_chat_ids()
    if not chat_ids:
        logger.error("Deploy notification skipped: no recipient was found")
        return 0

    endpoint = f"https://api.telegram.org/bot{ADMIN_BOT_TOKEN}/sendMessage"
    delivered = 0
    for chat_id in chat_ids:
        body = json.dumps(
            {
                "chat_id": chat_id,
                "text": message,
                "disable_web_page_preview": True,
            }
        ).encode()
        request = urlrequest.Request(
            endpoint,
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urlrequest.urlopen(request, timeout=15) as response:
                if response.status == 200:
                    delivered += 1
        except urlerror.HTTPError as error:
            logger.error("Telegram notification to %s failed with HTTP %s", chat_id, error.code)
        except urlerror.URLError as error:
            logger.error("Telegram notification to %s failed: %s", chat_id, error.reason)
    logger.info("Deploy notification delivered to %s recipient(s)", delivered)
    return delivered


def success_message(commit: str, subject: str) -> str:
    return (
        "✅ ЭХО обновлён и работает\n\n"
        f"Коммит: {commit[:7]}\n"
        f"Изменения: {subject}\n\n"
        "Проверка: тесты пройдены.\n"
        "Сервер: новый код сохранён, служба active."
    )


def validate_candidate(candidate_dir: Path) -> None:
    python = PROJECT_DIR / ".venv" / "bin" / "python3"
    if not python.exists():
        raise DeployError(f"Virtual environment Python not found: {python}")

    run_command(
        [str(python), "-m", "pip", "install", "-q", "-r", str(candidate_dir / "requirements.txt")]
    )
    run_command([str(python), "-m", "compileall", "-q", str(candidate_dir)])
    run_command(
        [str(python), "-m", "unittest", "discover", "-s", "tests", "-v"],
        cwd=candidate_dir,
    )
    run_command(
        [str(python), "-c", "import config, database, handlers, keyboards, main, texts"],
        cwd=candidate_dir,
    )


def restart_bot(commit_before_deploy: str) -> None:
    run_command(["systemctl", "restart", BOT_SERVICE])
    time.sleep(3)
    status = run_command(["systemctl", "is-active", "--quiet", BOT_SERVICE], check=False)
    if status.returncode == 0:
        logger.info("Service %s is active", BOT_SERVICE)
        return

    logger.error("New version failed; rolling back to %s", commit_before_deploy)
    run_command(["git", "reset", "--hard", commit_before_deploy])
    run_command(["systemctl", "restart", BOT_SERVICE])
    raise DeployError("Bot failed after deployment; rollback completed")


def schedule_webhook_restart() -> None:
    def restart() -> None:
        time.sleep(2)
        subprocess.run(["systemctl", "restart", WEBHOOK_SERVICE], check=False)

    threading.Thread(target=restart, name="webhook-restart", daemon=True).start()


def deploy_once() -> None:
    logger.info("Deploying %s branch %s", REPOSITORY, BRANCH)
    run_command(["git", "fetch", "--prune", "origin", BRANCH])

    current_commit = git_output("rev-parse", "HEAD")
    target_commit = git_output("rev-parse", f"origin/{BRANCH}")
    if current_commit == target_commit:
        logger.info("Commit %s is already deployed", current_commit)
        return

    ancestor = run_command(
        ["git", "merge-base", "--is-ancestor", current_commit, target_commit],
        check=False,
    )
    if ancestor.returncode != 0:
        raise DeployError("Remote history is not a fast-forward; deployment stopped")

    changed_files = set(git_output("diff", "--name-only", current_commit, target_commit).splitlines())

    with tempfile.TemporaryDirectory(prefix="echo-deploy-") as temp_dir:
        candidate = Path(temp_dir) / "candidate"
        run_command(["git", "worktree", "add", "--detach", str(candidate), target_commit])
        try:
            validate_candidate(candidate)
        finally:
            run_command(["git", "worktree", "remove", "--force", str(candidate)], check=False)

    run_command(["git", "merge", "--ff-only", target_commit])
    restart_bot(current_commit)
    deployed_commit = git_output("rev-parse", "HEAD")
    if deployed_commit != target_commit:
        raise DeployError("Server HEAD does not match the requested commit after deployment")

    subject = git_output("show", "-s", "--format=%s", target_commit)
    logger.info("Commit %s deployed successfully", target_commit)
    send_telegram_notification(success_message(target_commit, subject))

    if "deploy_webhook.py" in changed_files:
        schedule_webhook_restart()


def deploy_worker() -> None:
    global _deploy_pending, _worker_running

    while True:
        with _state_lock:
            _deploy_pending = False
        try:
            deploy_once()
        except Exception as error:
            logger.exception("Deployment failed")
            send_telegram_notification(
                "❌ ЭХО не удалось обновить\n\n"
                f"Причина: {str(error)[:500]}\n"
                "Рабочая версия сохранена или восстановлена."
            )

        with _state_lock:
            if _deploy_pending:
                continue
            _worker_running = False
            return


def request_deploy() -> None:
    global _deploy_pending, _worker_running

    with _state_lock:
        _deploy_pending = True
        if _worker_running:
            return
        _worker_running = True
    threading.Thread(target=deploy_worker, name="deploy", daemon=True).start()


def valid_signature(payload: bytes, signature: str) -> bool:
    expected = "sha256=" + hmac.new(SECRET, payload, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, signature)


class WebhookHandler(BaseHTTPRequestHandler):
    timeout = 10

    def reply(self, status: int, body: str) -> None:
        payload = body.encode()
        self.send_response(status)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def do_GET(self) -> None:
        self.reply(200, "ok") if self.path == "/health" else self.reply(404, "not found")

    def do_POST(self) -> None:
        if self.path != "/github":
            self.reply(404, "not found")
            return

        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            self.reply(400, "bad content length")
            return
        if not 0 < length <= MAX_BODY_BYTES:
            self.reply(413, "invalid payload size")
            return

        payload = self.rfile.read(length)
        if not valid_signature(payload, self.headers.get("X-Hub-Signature-256", "")):
            self.reply(403, "invalid signature")
            return

        event = self.headers.get("X-GitHub-Event", "")
        if event == "ping":
            self.reply(200, "pong")
            return
        if event != "push":
            self.reply(200, f"ignored event: {event}")
            return

        try:
            data = json.loads(payload)
        except json.JSONDecodeError:
            self.reply(400, "invalid json")
            return

        repository = data.get("repository", {}).get("full_name")
        if repository != REPOSITORY or data.get("ref") != f"refs/heads/{BRANCH}":
            self.reply(200, "ignored repository or branch")
            return

        request_deploy()
        self.reply(202, "deployment queued")

    def log_message(self, message: str, *args: object) -> None:
        logger.info("%s | %s", self.client_address[0], message % args)


class WebhookServer(ThreadingHTTPServer):
    allow_reuse_address = True
    daemon_threads = True
    request_queue_size = 64


def main() -> None:
    if not SECRET:
        raise RuntimeError("WEBHOOK_SECRET is not configured")
    server = WebhookServer(("0.0.0.0", PORT), WebhookHandler)
    logger.info("Webhook for %s is listening on port %s", REPOSITORY, PORT)
    server.serve_forever()


if __name__ == "__main__":
    main()
