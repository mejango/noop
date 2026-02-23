FROM node:20-alpine AS dashboard-builder

RUN apk add --no-cache python3 make g++

WORKDIR /dashboard
COPY dashboard/package.json dashboard/package-lock.json ./
RUN npm ci
COPY dashboard/ ./
ENV NEXT_TELEMETRY_DISABLED=1
RUN npm run build

# Production image
FROM node:20-alpine

RUN apk add --no-cache python3 make g++

WORKDIR /app

# Install bot dependencies
COPY package.json package-lock.json ./
RUN npm ci --production

# Install dashboard production dependencies
COPY dashboard/package.json dashboard/package-lock.json ./dashboard/
RUN cd dashboard && npm ci --production

# Copy bot
COPY bot/ ./bot/
COPY script.js ./

# Copy dashboard build
COPY --from=dashboard-builder /dashboard/.next ./dashboard/.next
COPY --from=dashboard-builder /dashboard/src ./dashboard/src
COPY --from=dashboard-builder /dashboard/next.config.mjs ./dashboard/

# Start script runs both
COPY start.sh ./
RUN chmod +x start.sh

ENV DATA_DIR=/data
ENV NODE_ENV=production
ENV PORT=3000
ENV NEXT_TELEMETRY_DISABLED=1

EXPOSE 3000

CMD ["./start.sh"]
