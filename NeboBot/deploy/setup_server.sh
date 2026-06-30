#!/usr/bin/env bash
set -Eeuo pipefail

APP_DIR="/opt/nebo-alert-bot"
DATA_DIR="/var/lib/nebo-alert-bot"
ENV_FILE="/etc/nebo-alert-bot.env"
SERVICE_FILE="/etc/systemd/system/nebo-alert-bot.service"
SERVICE_USER="nebo-alert"
SOURCE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ "${EUID}" -ne 0 ]]; then
    echo "Запустите установщик от root." >&2
    exit 1
fi

EXISTING_BOT_TOKEN=""
EXISTING_WEBHOOK_SECRET=""
if [[ -f "${ENV_FILE}" ]]; then
    EXISTING_BOT_TOKEN="$(sed -n 's/^BOT_TOKEN=//p' "${ENV_FILE}" | head -n 1)"
    EXISTING_WEBHOOK_SECRET="$(sed -n 's/^WEBHOOK_SECRET=//p' "${ENV_FILE}" | head -n 1)"
fi

BOT_TOKEN="${BPLA_BOT_TOKEN:-${EXISTING_BOT_TOKEN}}"
if [[ -z "${BOT_TOKEN}" ]]; then
    read -r -s -p "Telegram bot token: " BOT_TOKEN
    echo
fi
if [[ "${BOT_TOKEN}" != *:* || ${#BOT_TOKEN} -lt 30 ]]; then
    echo "Токен выглядит некорректно." >&2
    exit 1
fi

WEBHOOK_SECRET="${WEBHOOK_SECRET:-${EXISTING_WEBHOOK_SECRET}}"
if [[ -z "${WEBHOOK_SECRET}" ]]; then
    WEBHOOK_SECRET="$(python3 -c 'import secrets; print(secrets.token_hex(32))')"
fi

if ! id -u "${SERVICE_USER}" >/dev/null 2>&1; then
    useradd --system --home-dir "${DATA_DIR}" --shell /usr/sbin/nologin "${SERVICE_USER}"
fi

install -d -m 0755 "${APP_DIR}"
install -d -o "${SERVICE_USER}" -g "${SERVICE_USER}" -m 0750 "${DATA_DIR}"

for file in config.py database.py deploy_webhook.py handlers.py keyboards.py main.py models.py notifier.py source.py requirements.txt; do
    install -m 0644 "${SOURCE_DIR}/${file}" "${APP_DIR}/${file}"
done

if [[ ! -x "${APP_DIR}/.venv/bin/python" ]]; then
    if ! python3 -m venv "${APP_DIR}/.venv"; then
        apt-get update
        DEBIAN_FRONTEND=noninteractive apt-get install -y python3-venv python3-pip
        python3 -m venv "${APP_DIR}/.venv"
    fi
fi
"${APP_DIR}/.venv/bin/python" -m pip install --disable-pip-version-check -r "${APP_DIR}/requirements.txt"

umask 077
{
    printf 'BOT_TOKEN=%s\n' "${BOT_TOKEN}"
    printf 'DATABASE_PATH=%s/bot.sqlite3\n' "${DATA_DIR}"
    printf 'SOURCE_BASE_URL=https://bplarussia.ru\n'
    printf 'POLL_INTERVAL_SECONDS=60\n'
    printf 'REQUEST_TIMEOUT_SECONDS=20\n'
    printf 'REGIONS_CACHE_TTL_SECONDS=600\n'
    printf 'STATS_CACHE_TTL_SECONDS=30\n'
    printf 'MAX_HISTORY_PAGES=50\n'
    printf 'LOG_LEVEL=INFO\n'
    printf 'WEBHOOK_SECRET=%s\n' "${WEBHOOK_SECRET}"
    printf 'DEPLOY_PORT=9002\n'
    printf 'GITHUB_REPOSITORY=Marvelous-A/Nebo-Ryadom-Bot\n'
    printf 'DEPLOY_BRANCH=main\n'
} > "${ENV_FILE}"
chmod 0600 "${ENV_FILE}"

install -m 0644 "${SOURCE_DIR}/deploy/nebo-alert-bot.service" "${SERVICE_FILE}"
install -m 0644 "${SOURCE_DIR}/deploy/nebo-deploy-webhook.service" "/etc/systemd/system/nebo-deploy-webhook.service"
systemctl daemon-reload
systemctl enable nebo-alert-bot.service nebo-deploy-webhook.service
systemctl restart nebo-alert-bot.service nebo-deploy-webhook.service

for _ in {1..20}; do
    if systemctl is-active --quiet nebo-alert-bot.service; then
        break
    fi
    sleep 1
done

if ! systemctl is-active --quiet nebo-alert-bot.service; then
    systemctl --no-pager --full status nebo-alert-bot.service || true
    journalctl -u nebo-alert-bot.service -n 80 --no-pager || true
    exit 1
fi

BOT_TOKEN="" BPLA_BOT_TOKEN="" WEBHOOK_SECRET="" EXISTING_BOT_TOKEN="" EXISTING_WEBHOOK_SECRET=""
echo "nebo-alert-bot.service запущен."
systemctl --no-pager --full status nebo-alert-bot.service | sed -n '1,15p'
