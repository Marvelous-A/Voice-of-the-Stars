#!/bin/bash
# Watchdog script — checks bot and webhook health, restarts if needed.
# Runs via cron every 5 minutes.
# Install: crontab -e → */5 * * * * /root/Voice-of-the-Stars/watchdog.sh >> /root/Voice-of-the-Stars/watchdog.log 2>&1

BOT_TOKEN=$(grep BOT_TOKEN /root/Voice-of-the-Stars/.env | cut -d'=' -f2 | tr -d '"' | tr -d "'")
ADMIN_ID=$(grep ADMIN_ID /root/Voice-of-the-Stars/.env | cut -d'=' -f2 | tr -d '"' | tr -d "'")
NOW=$(date '+%Y-%m-%d %H:%M:%S')
ISSUES=""

# Check bot.service
if ! systemctl is-active --quiet bot.service; then
    ISSUES="${ISSUES}⚠️ bot.service was down — restarting\n"
    systemctl restart bot.service
    sleep 5
fi

# Check deploy-webhook.service
if ! systemctl is-active --quiet deploy-webhook.service; then
    ISSUES="${ISSUES}⚠️ deploy-webhook.service was down — restarting\n"
    systemctl restart deploy-webhook.service
    sleep 2
fi

# Check webhook actually responds
RESP=$(curl -s --max-time 5 http://localhost:9000)
if [ "$RESP" != "Webhook listener is running" ]; then
    ISSUES="${ISSUES}⚠️ Webhook not responding — restarting\n"
    systemctl restart deploy-webhook.service
    sleep 2
fi

# Check no duplicate bot processes
BOT_COUNT=$(pgrep -fc "python3 main.py" || true)
if [ "$BOT_COUNT" -gt 1 ]; then
    ISSUES="${ISSUES}⚠️ ${BOT_COUNT} bot processes found — killing all and restarting\n"
    pkill -9 -f "python3 main.py"
    sleep 3
    systemctl restart bot.service
    sleep 5
fi

# Send alert to admin if there were issues
if [ -n "$ISSUES" ]; then
    MSG="🛡 Watchdog [$NOW]:%0A${ISSUES}"
    curl -s "https://api.telegram.org/bot${BOT_TOKEN}/sendMessage?chat_id=${ADMIN_ID}&text=${MSG}" > /dev/null 2>&1
    echo "[$NOW] Fixed: $ISSUES"
else
    echo "[$NOW] OK"
fi
