FROM node:22-alpine AS build

WORKDIR /app

# corepack installs the pnpm that package.json's packageManager pins, checked against its hash.
ENV COREPACK_ENABLE_DOWNLOAD_PROMPT=0
RUN corepack enable

COPY node/gateway/package.json node/gateway/pnpm-lock.yaml node/gateway/pnpm-workspace.yaml ./
COPY node/gateway/patches ./patches
RUN pnpm install --frozen-lockfile

COPY node/gateway/tsconfig.json ./
COPY node/gateway/src ./src
RUN pnpm build

FROM node:22-alpine AS runtime

WORKDIR /app

ENV COREPACK_ENABLE_DOWNLOAD_PROMPT=0
RUN corepack enable

COPY node/gateway/package.json node/gateway/pnpm-lock.yaml node/gateway/pnpm-workspace.yaml ./
COPY node/gateway/patches ./patches
RUN pnpm install --frozen-lockfile --prod

COPY --from=build /app/dist ./dist
COPY dashboard ./dashboard
COPY ops/instruments.yml ./ops/instruments.yml

ENV NODE_ENV=production
CMD ["node", "dist/server.js"]
