#!/bin/bash
# ============================================================
# Watchdog для deploy-webhook: раз в минуту curl'ит localhost:9000,
# если не отвечает — перезапускает сервис.
# Запускать один раз:
#   bash /root/Voice-of-the-Stars/setup_watchdog.sh
# Идемпотентный.
# ============================================================

set -euo pipefail

HEALTH_UNIT="/etc/systemd/system/deploy-webhook-healthcheck.service"
TIMER_UNIT="/etc/systemd/system/deploy-webhook-healthcheck.timer"

[[ $EUID -eq 0 ]] || { echo "Нужен root" >&2; exit 1; }

echo "=== deploy-webhook-healthcheck.service ==="
cat > "$HEALTH_UNIT" << 'EOF'
[Unit]
Description=Healthcheck for deploy-webhook (auto-restart if unresponsive)
After=deploy-webhook.service

[Service]
Type=oneshot
# Если GET /  не отвечает за 5 сек — перезапускаем сервис
ExecStart=/bin/bash -c 'curl -fsS --max-time 5 http://127.0.0.1:9000/ >/dev/null || { echo "webhook unresponsive — restarting"; systemctl restart deploy-webhook.service; }'
EOF

echo "=== deploy-webhook-healthcheck.timer ==="
cat > "$TIMER_UNIT" << 'EOF'
[Unit]
Description=Run deploy-webhook healthcheck every minute

[Timer]
# Первый запуск через 2 минуты после загрузки (даёт вебхуку время подняться)
OnBootSec=2min
OnUnitActiveSec=1min
# Если сервер спал/был выключен — запустить пропущенные запуски
Persistent=true
AccuracySec=10s

[Install]
WantedBy=timers.target
EOF

systemctl daemon-reload
systemctl enable --now deploy-webhook-healthcheck.timer

echo ""
echo "✓ Готово. Проверка каждую минуту, авторестарт при зависании."
echo ""
echo "Проверка:"
echo "  systemctl list-timers deploy-webhook-healthcheck.timer"
echo "  journalctl -u deploy-webhook-healthcheck -n 20 --no-pager"
echo "  # Симуляция зависания (покажет, как watchdog среагирует):"
echo "  # kill -STOP \$(pidof -s python3); sleep 90; kill -CONT \$(pidof -s python3)"
