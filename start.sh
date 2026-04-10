#!/bin/sh

cleanup() {
  echo "Shutting down..."
  kill $BOT_PID 2>/dev/null
  exit 0
}

trap cleanup SIGTERM SIGINT

# Initialize knowledge wiki if volume dir is empty (first deploy)
WIKI_PATH="${WIKI_DIR:-knowledge}"
mkdir -p "$WIKI_PATH" "$WIKI_PATH/regimes" "$WIKI_PATH/protection" "$WIKI_PATH/revenue" "$WIKI_PATH/indicators" "$WIKI_PATH/strategy"
if [ ! -f "$WIKI_PATH/schema.md" ]; then
  echo "Initializing knowledge wiki at $WIKI_PATH from templates..."
  cp -r knowledge-templates/* "$WIKI_PATH/" 2>/dev/null || true
fi

# Backfill any newly added wiki template files without overwriting existing content.
for template in $(find knowledge-templates -type f | sort); do
  target="${template#knowledge-templates/}"
  if [ ! -f "$WIKI_PATH/$target" ]; then
    mkdir -p "$(dirname "$WIKI_PATH/$target")"
    cp "$template" "$WIKI_PATH/$target"
    echo "Backfilled missing wiki template: $target"
  fi
done

# Auto-seed wiki from journal history if not yet seeded (background, non-blocking)
if [ ! -f "$WIKI_PATH/.meta.json" ] && [ -n "$ANTHROPIC_API_KEY" ]; then
  echo "Seeding knowledge wiki in background (one-time)..."
  (node bot/seed-wiki.js && echo "📚 Wiki seed complete" || echo "📚 Wiki seed failed (non-fatal)") &
fi

echo "Starting bot..."
node bot/index.js &
BOT_PID=$!

echo "Starting dashboard on port ${PORT:-3000}..."
cd dashboard && HOSTNAME=0.0.0.0 node server.js
