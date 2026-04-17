#!/bin/bash
# ============================================================
# Настройка сервера для Voice of the Stars
# Запускать один раз: bash /root/Voice-of-the-Stars/setup_server.sh
# ============================================================

set -e

echo "=== Удаляем старые сервисы и cron ==="

# Остановить и удалить старый bot.service (если есть)
systemctl stop bot.service 2>/dev/null || true
systemctl disable bot.service 2>/dev/null || true
rm -f /etc/systemd/system/bot.service

# Удалить cron watchdog (если есть)
crontab -r 2>/dev/null || true

# Убить все старые процессы бота из /root/Voice-of-the-Stars/
pkill -f "/root/Voice-of-the-Stars/venv/bin/python3 main.py" 2>/dev/null || true

echo "=== Создаём tarot-bot.service ==="

cat > /etc/systemd/system/tarot-bot.service << 'EOF'
[Unit]
Description=Tarot Telegram Bot
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory=/home/bot
ExecStart=/home/bot/venv/bin/python3 -u /home/bot/main.py
Restart=always
RestartSec=5
EnvironmentFile=/home/bot/.env
Environment=PYTHONUNBUFFERED=1

[Install]
WantedBy=multi-user.target
EOF

echo "=== Создаём deploy-webhook.service ==="

cat > /etc/systemd/system/deploy-webhook.service << 'EOF'
[Unit]
Description=GitHub Deploy Webhook for Tarot Bot
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory=/root/Voice-of-the-Stars
ExecStart=/root/Voice-of-the-Stars/venv/bin/python3 -u deploy_webhook.py
Restart=always
RestartSec=5
Environment=PYTHONUNBUFFERED=1

[Install]
WantedBy=multi-user.target
EOF

echo "=== Копируем код в /home/bot/ ==="
mkdir -p /home/bot
cp /root/Voice-of-the-Stars/main.py /home/bot/main.py
cp /root/Voice-of-the-Stars/requirements.txt /home/bot/requirements.txt
cp /root/Voice-of-the-Stars/descriptions.json /home/bot/descriptions.json 2>/dev/null || true

echo "=== Перезагружаем systemd и запускаем сервисы ==="
systemctl daemon-reload

# Убить все nohup-процессы вебхука
pkill -f "deploy_webhook.py" 2>/dev/null || true
sleep 2

systemctl enable tarot-bot.service
systemctl enable deploy-webhook.service
systemctl restart deploy-webhook.service
systemctl restart tarot-bot.service

sleep 5

echo ""
echo "=== Статус сервисов ==="
systemctl status tarot-bot.service --no-pager -l | head -15
echo ""
systemctl status deploy-webhook.service --no-pager -l | head -15
echo ""

echo "=== Проверка: только один процесс бота ==="
ps aux | grep main.py | grep -v grep

echo ""
echo "✓ Готово! Теперь:"
echo "  - git push → вебхук подтянет код, скопирует в /home/bot/, рестартнет бота"
echo "  - Логи бота: sudo journalctl -u tarot-bot -f"
echo "  - Логи вебхука: sudo journalctl -u deploy-webhook -f"
