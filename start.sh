#!/bin/sh

cleanup() {
  echo "Shutting down..."
  kill $BOT_PID 2>/dev/null
  exit 0
}

trap cleanup SIGTERM SIGINT

# Initialize knowledge wiki if volume dir is empty (first deploy)
WIKI_PATH="${WIKI_DIR:-knowledge}"
mkdir -p "$WIKI_PATH"
if [ ! -f "$WIKI_PATH/schema.md" ]; then
  echo "Initializing knowledge wiki at $WIKI_PATH from templates..."
  cp -r knowledge-templates/* "$WIKI_PATH/" 2>/dev/null || true
fi

echo "Starting bot..."
node bot/index.js &
BOT_PID=$!

echo "Starting dashboard on port ${PORT:-3000}..."
cd dashboard && HOSTNAME=0.0.0.0 node server.js
