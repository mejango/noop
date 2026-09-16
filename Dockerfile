FROM node:20-alpine AS dashboard-builder

RUN apk add --no-cache python3 make g++

WORKDIR /dashboard
COPY dashboard/package.json dashboard/package-lock.json ./
RUN npm ci
COPY dashboard/ ./
COPY bot/config.json /bot/config.json
COPY integrations/derive-v3/profile.js integrations/derive-v3/isolation.js /integrations/derive-v3/
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
COPY integrations/derive-v3/*.js integrations/derive-v3/package*.json ./integrations/derive-v3/
RUN npm ci --prefix integrations/derive-v3 --omit=dev --ignore-scripts --no-audit --no-fund

# Copy knowledge wiki templates (used to seed empty volumes on first deploy)
COPY knowledge/ ./knowledge-templates/
COPY knowledge/ ./knowledge/

# Copy dashboard standalone build
COPY --from=dashboard-builder /dashboard/.next/standalone ./dashboard/
COPY --from=dashboard-builder /dashboard/.next/static ./dashboard/.next/static

# Start script runs both
COPY start.sh ./
RUN chmod +x start.sh

ENV DATA_DIR=/data
ENV BOT_CONFIG_PATH=/app/bot/config.json
ENV NODE_ENV=production
ENV PORT=3000
ENV HOSTNAME=0.0.0.0
ENV NEXT_TELEMETRY_DISABLED=1

EXPOSE 3000

CMD ["./start.sh"]
