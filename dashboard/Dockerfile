FROM node:22-alpine AS dependencies
WORKDIR /app
COPY package.json ./
RUN npm install --no-audit --no-fund

FROM node:22-alpine AS builder
WORKDIR /app
ENV NEXT_TELEMETRY_DISABLED=1
COPY --from=dependencies /app/node_modules ./node_modules
COPY . .
RUN npm run build

FROM node:22-alpine AS runtime
WORKDIR /app
ENV NODE_ENV=production NEXT_TELEMETRY_DISABLED=1 HOSTNAME=0.0.0.0 PORT=3000
RUN addgroup -S agentmesh && adduser -S agentmesh -G agentmesh
COPY --from=builder --chown=agentmesh:agentmesh /app/.next/standalone ./
COPY --from=builder --chown=agentmesh:agentmesh /app/.next/static ./.next/static
USER agentmesh
EXPOSE 3000
CMD ["node", "server.js"]
