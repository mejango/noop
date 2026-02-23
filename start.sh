#!/bin/sh
# Start bot in background, dashboard in foreground
echo "Starting bot..."
node bot/index.js &
BOT_PID=$!

echo "Starting dashboard..."
cd dashboard && npx next start -p ${PORT:-3000} &
DASH_PID=$!

# If either exits, kill the other and exit
trap "kill $BOT_PID $DASH_PID 2>/dev/null; exit" SIGTERM SIGINT

wait -n
kill $BOT_PID $DASH_PID 2>/dev/null
