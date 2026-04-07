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

# Auto-seed wiki from journal history if not yet seeded
if [ ! -f "$WIKI_PATH/.meta.json" ] && [ -n "$ANTHROPIC_API_KEY" ]; then
  echo "Seeding knowledge wiki from journal history (one-time)..."
  node bot/seed-wiki.js || echo "Wiki seed failed (non-fatal), will populate via ingest cycles"
fi

echo "Starting bot..."
node bot/index.js &
BOT_PID=$!

echo "Starting dashboard on port ${PORT:-3000}..."
cd dashboard && HOSTNAME=0.0.0.0 node server.js
