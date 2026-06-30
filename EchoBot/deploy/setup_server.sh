#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="/opt/echo-dialog-bot"
ENV_FILE="/etc/echo-dialog-bot.env"
DATA_DIR="/var/lib/echo-dialog-bot"

[[ "${EUID}" -eq 0 ]] || { echo "Run this script as root" >&2; exit 1; }
[[ -d "${PROJECT_DIR}/.git" ]] || { echo "Clone the repository into ${PROJECT_DIR} first" >&2; exit 1; }
[[ -f "${ENV_FILE}" ]] || { echo "Create ${ENV_FILE} with BOT_TOKEN and WEBHOOK_SECRET first" >&2; exit 1; }

apt-get update -qq
DEBIAN_FRONTEND=noninteractive apt-get install -y python3 python3-venv git

id -u echo-bot >/dev/null 2>&1 || useradd --system --home-dir "${DATA_DIR}" --shell /usr/sbin/nologin echo-bot
install -d -o echo-bot -g echo-bot -m 0750 "${DATA_DIR}"

python3 -m venv "${PROJECT_DIR}/.venv"
"${PROJECT_DIR}/.venv/bin/python3" -m pip install -q --upgrade pip
"${PROJECT_DIR}/.venv/bin/python3" -m pip install -q -r "${PROJECT_DIR}/requirements.txt"
"${PROJECT_DIR}/.venv/bin/python3" -m compileall -q "${PROJECT_DIR}"
cd "${PROJECT_DIR}"
"${PROJECT_DIR}/.venv/bin/python3" -m unittest discover -s tests -v

install -m 0644 "${PROJECT_DIR}/deploy/echo-dialog-bot.service" /etc/systemd/system/echo-dialog-bot.service
install -m 0644 "${PROJECT_DIR}/deploy/echo-deploy-webhook.service" /etc/systemd/system/echo-deploy-webhook.service

systemctl daemon-reload
systemctl enable echo-dialog-bot.service echo-deploy-webhook.service
systemctl restart echo-dialog-bot.service echo-deploy-webhook.service

sleep 3
systemctl --no-pager --full status echo-dialog-bot.service echo-deploy-webhook.service
