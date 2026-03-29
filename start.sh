#!/bin/bash
# ═══════════════════════════════════════════
# TRAKR Startup Script
# Launches Flask (Gunicorn) FIRST so Render
# binds the correct port (5000), then starts
# WhatsApp bot after a short delay.
# ═══════════════════════════════════════════

echo "🚀 Starting Flask API on port ${PORT:-5000}..."
cd /app && gunicorn \
    --bind 0.0.0.0:${PORT:-5000} \
    --workers 2 \
    --timeout 120 \
    --access-logfile - \
    --error-logfile - \
    app:app &
GUNICORN_PID=$!

# Wait for Gunicorn to bind the port FIRST
sleep 5

echo "🤖 Starting WhatsApp Bot..."
cd /app/whatsapp-bot && node bot.js &
BOT_PID=$!

# Wait for either process to exit
wait -n $GUNICORN_PID $BOT_PID

# If either exits, kill both
kill $GUNICORN_PID 2>/dev/null
kill $BOT_PID 2>/dev/null
