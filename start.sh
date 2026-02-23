#!/bin/sh

cleanup() {
  echo "Shutting down..."
  kill $BOT_PID 2>/dev/null
  exit 0
}

trap cleanup SIGTERM SIGINT

echo "Starting bot..."
node bot/index.js &
BOT_PID=$!

echo "Starting dashboard on port ${PORT:-3000}..."
cd dashboard && ./node_modules/.bin/next start -p ${PORT:-3000}
