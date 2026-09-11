FROM node:20-alpine AS dashboard-builder

RUN apk add --no-cache python3 make g++

WORKDIR /dashboard
COPY dashboard/package.json dashboard/package-lock.json ./
RUN npm ci
COPY dashboard/ ./
COPY bot/config.json bot/strategy-facts.json bot/economic-events.js /bot/
ENV NEXT_TELEMETRY_DISABLED=1
ENV BOT_CONFIG_PATH=/bot/config.json
RUN npm run build

# Production image
FROM node:20-alpine

RUN apk add --no-cache python3 make g++

WORKDIR /app

# Install bot dependencies
COPY package.json package-lock.json ./
RUN npm ci --omit=dev

# Copy bot
COPY bot/ ./bot/
COPY script.js ./
COPY scripts/archive-v2-data.js scripts/import-economic-events.js ./scripts/

# Copy knowledge wiki templates (used to seed empty volumes on first deploy)
COPY knowledge/ ./knowledge-templates/
COPY knowledge/ ./knowledge/

# Copy dashboard standalone build
COPY --from=dashboard-builder /dashboard/.next/standalone ./dashboard/
COPY --from=dashboard-builder /dashboard/.next/static ./dashboard/.next/static

# Supervise both services; a failed service must fail the container.
COPY start.sh ./
COPY scripts/process-supervisor.js ./scripts/process-supervisor.js
RUN chmod +x start.sh

ENV DATA_DIR=/data
ENV BOT_CONFIG_PATH=/app/bot/config.json
ENV NODE_ENV=production
ENV PORT=3000
ENV HOSTNAME=0.0.0.0
ENV NEXT_TELEMETRY_DISABLED=1

EXPOSE 3000

CMD ["./start.sh"]
