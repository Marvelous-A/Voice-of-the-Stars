#!/bin/bash
set -euo pipefail

SOURCE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ADMIN_DIR="${ADMIN_DIR:-/home/admin-bot}"
VOICE_APP_DIR="${VOICE_APP_DIR:-/home/bot}"
VOICE_DATA_DIR="${VOICE_DATA_DIR:-/home/bot}"
ECHO_DATABASE_PATH="${ECHO_DATABASE_PATH:-/var/lib/echo-dialog-bot/echo.db}"
NEBO_DATABASE_PATH="${NEBO_DATABASE_PATH:-/var/lib/nebo-alert-bot/bot.sqlite3}"

echo "=== Устанавливаем AdminBot в ${ADMIN_DIR} ==="
mkdir -p "${ADMIN_DIR}"

python3 -m venv "${ADMIN_DIR}/venv"
"${ADMIN_DIR}/venv/bin/python3" -m pip install -q -r "${SOURCE_DIR}/requirements.txt"

cp "${SOURCE_DIR}/main.py" "${ADMIN_DIR}/main.py"
cp "${SOURCE_DIR}/admin_projects.py" "${ADMIN_DIR}/admin_projects.py"
cp "${SOURCE_DIR}/project_runtime.py" "${ADMIN_DIR}/project_runtime.py"
cp "${SOURCE_DIR}/requirements.txt" "${ADMIN_DIR}/requirements.txt"

if [ -f "${SOURCE_DIR}/.env" ]; then
    cp "${SOURCE_DIR}/.env" "${ADMIN_DIR}/.env"
fi

cat > /etc/systemd/system/tarot-admin.service << EOF
[Unit]
Description=Shared Telegram Admin Bot
After=network.target tarot-bot.service

[Service]
Type=simple
User=root
WorkingDirectory=${ADMIN_DIR}
ExecStart=${ADMIN_DIR}/venv/bin/python3 -u ${ADMIN_DIR}/main.py
Restart=always
RestartSec=5
EnvironmentFile=-${VOICE_DATA_DIR}/.env
EnvironmentFile=-${ADMIN_DIR}/.env
Environment=VOICE_APP_DIR=${VOICE_APP_DIR}
Environment=VOICE_DATA_DIR=${VOICE_DATA_DIR}
Environment=ECHO_DATABASE_PATH=${ECHO_DATABASE_PATH}
Environment=NEBO_DATABASE_PATH=${NEBO_DATABASE_PATH}
Environment=PYTHONUNBUFFERED=1

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable tarot-admin.service
systemctl restart tarot-admin.service
systemctl status tarot-admin.service --no-pager -l
