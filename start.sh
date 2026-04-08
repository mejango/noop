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

# Auto-seed wiki from journal history if not yet seeded (background, non-blocking)
if [ ! -f "$WIKI_PATH/.meta.json" ] && [ -n "$ANTHROPIC_API_KEY" ]; then
  echo "Seeding knowledge wiki in background (one-time)..."
  (node bot/seed-wiki.js && echo "📚 Wiki seed complete" || echo "📚 Wiki seed failed (non-fatal)") &
fi

# Fix wiki page headings to use proper titles (one-time cleanup of seeded content)
fix_heading() {
  local file="$WIKI_PATH/$1"
  local old_heading="$2"
  local new_heading="$3"
  if [ -f "$file" ] && head -1 "$file" | grep -qF "$old_heading"; then
    sed -i "1s/.*/$new_heading/" "$file"
    echo "Fixed heading: $1"
  fi
}
fix_heading "regimes/current.md" "regimes/" "# Current Regime"
fix_heading "regimes/history.md" "regimes/" "# Regime History"
fix_heading "protection/pricing.md" "protection/" "# Protection Pricing"
fix_heading "protection/windows.md" "protection/" "# Protection Windows"
fix_heading "protection/convexity.md" "protection/" "# Convexity Map"
fix_heading "indicators/leading.md" "indicators/" "# Leading Indicators"
fix_heading "indicators/correlations.md" "indicators/" "# Correlations"
fix_heading "indicators/divergences.md" "indicators/" "# Divergences"
fix_heading "strategy/lessons.md" "strategy/" "# Strategy Lessons"
fix_heading "strategy/mistakes.md" "strategy/" "# Mistakes \& Anti-Patterns"
fix_heading "strategy/playbook.md" "strategy/" "# Strategy Playbook"

echo "Starting bot..."
node bot/index.js &
BOT_PID=$!

echo "Starting dashboard on port ${PORT:-3000}..."
cd dashboard && HOSTNAME=0.0.0.0 node server.js
