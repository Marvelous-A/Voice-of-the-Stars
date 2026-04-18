#!/bin/bash
# ============================================================
# Фаервол: порт 9000 (deploy webhook) — только для IP GitHub.
# Запускать:
#   bash /root/Voice-of-the-Stars/setup_firewall.sh
# Скрипт идемпотентный — можно гонять сколько угодно раз.
# Еженедельный cron сам обновляет список IP (создаётся на первом запуске).
# ============================================================

set -euo pipefail

IPSET_NAME="gh-webhooks"
WEBHOOK_PORT=9000
IPTABLES_CHAIN="GH-WEBHOOK"
CONFIG_DIR="/etc/iptables"
RULES_FILE="${CONFIG_DIR}/gh-webhook.rules.v4"
IPSETS_FILE="${CONFIG_DIR}/gh-webhook.ipsets.conf"
RESTORE_UNIT="/etc/systemd/system/gh-webhook-firewall.service"
CRON_FILE="/etc/cron.d/gh-webhook-firewall-refresh"
SCRIPT_PATH="$(readlink -f "$0")"

[[ $EUID -eq 0 ]] || { echo "Нужен root" >&2; exit 1; }

echo "=== Зависимости ==="
APT_PKGS=()
command -v ipset >/dev/null || APT_PKGS+=("ipset")
command -v jq    >/dev/null || APT_PKGS+=("jq")
if [[ ${#APT_PKGS[@]} -gt 0 ]]; then
    DEBIAN_FRONTEND=noninteractive apt-get update -qq
    DEBIAN_FRONTEND=noninteractive apt-get install -y "${APT_PKGS[@]}"
fi

echo "=== Список IP GitHub-webhook ==="
HOOKS_JSON="$(curl -fsS https://api.github.com/meta)"
mapfile -t HOOKS < <(jq -r '.hooks[]?' <<<"$HOOKS_JSON" | grep -v ':')
[[ ${#HOOKS[@]} -gt 0 ]] || { echo "ERROR: GitHub вернул пустой список, прерываю" >&2; exit 1; }
echo "Получено ${#HOOKS[@]} IPv4-подсетей"

echo "=== ipset ${IPSET_NAME} ==="
TMP_SET="${IPSET_NAME}-new"
ipset destroy "$TMP_SET" 2>/dev/null || true
ipset create  "$TMP_SET" hash:net family inet
for cidr in "${HOOKS[@]}"; do
    ipset add "$TMP_SET" "$cidr"
done
if ipset list -name | grep -qx "$IPSET_NAME"; then
    ipset swap    "$IPSET_NAME" "$TMP_SET"
    ipset destroy "$TMP_SET"
else
    ipset rename  "$TMP_SET" "$IPSET_NAME"
fi

echo "=== iptables-цепочка ${IPTABLES_CHAIN} ==="
iptables -L "$IPTABLES_CHAIN" -n >/dev/null 2>&1 || iptables -N "$IPTABLES_CHAIN"
iptables -F "$IPTABLES_CHAIN"
iptables -A "$IPTABLES_CHAIN" -m set --match-set "$IPSET_NAME" src -j ACCEPT
iptables -A "$IPTABLES_CHAIN" -s 127.0.0.0/8                        -j ACCEPT
iptables -A "$IPTABLES_CHAIN" -j DROP

# Привязка к INPUT для нужного порта (чистим старые повторения)
while iptables -C INPUT -p tcp --dport "$WEBHOOK_PORT" -j "$IPTABLES_CHAIN" 2>/dev/null; do
    iptables -D INPUT -p tcp --dport "$WEBHOOK_PORT" -j "$IPTABLES_CHAIN"
done
iptables -I INPUT -p tcp --dport "$WEBHOOK_PORT" -j "$IPTABLES_CHAIN"

echo "=== Сохраняем состояние ==="
mkdir -p "$CONFIG_DIR"
ipset save "$IPSET_NAME" > "$IPSETS_FILE"
# Сохраняем ТОЛЬКО нашу цепочку и привязку — чтобы не конфликтовать с fail2ban
{
    echo "*filter"
    echo ":${IPTABLES_CHAIN} - [0:0]"
    iptables-save -t filter | grep -E "^-A ${IPTABLES_CHAIN} |^-A INPUT .*-j ${IPTABLES_CHAIN}\$"
    echo "COMMIT"
} > "$RULES_FILE"

if [[ ! -f "$RESTORE_UNIT" ]]; then
    echo "=== Создаём gh-webhook-firewall.service ==="
    cat > "$RESTORE_UNIT" << EOF
[Unit]
Description=Restore GitHub webhook firewall (ipset + iptables) at boot
DefaultDependencies=no
After=local-fs.target
Before=network-pre.target
Wants=network-pre.target

[Service]
Type=oneshot
ExecStart=/sbin/ipset restore -exist -f ${IPSETS_FILE}
ExecStart=/sbin/iptables-restore --noflush ${RULES_FILE}
RemainAfterExit=yes

[Install]
WantedBy=multi-user.target
EOF
    systemctl daemon-reload
    systemctl enable gh-webhook-firewall.service
fi

if [[ ! -f "$CRON_FILE" ]]; then
    echo "=== Еженедельное обновление списка IP ==="
    cat > "$CRON_FILE" << EOF
# Обновление списка IP GitHub webhook каждое воскресенье в 04:00 UTC
0 4 * * 0 root ${SCRIPT_PATH} >>/var/log/gh-webhook-firewall.log 2>&1
EOF
    chmod 644 "$CRON_FILE"
fi

echo ""
echo "✓ Готово. Порт ${WEBHOOK_PORT} открыт только для GitHub (${#HOOKS[@]} подсетей)."
echo ""
echo "Проверка:"
echo "  iptables -L ${IPTABLES_CHAIN} -v --line-numbers"
echo "  ipset list ${IPSET_NAME} | head -20"
echo ""
echo "Активные подсети:"
printf '  %s\n' "${HOOKS[@]}"
