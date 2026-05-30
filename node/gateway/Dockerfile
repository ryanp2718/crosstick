FROM node:22-alpine AS build

WORKDIR /app

RUN npm install -g pnpm@9

COPY node/gateway/package.json ./
RUN pnpm install --prod=false

COPY node/gateway/tsconfig.json ./
COPY node/gateway/src ./src
RUN pnpm build

FROM node:22-alpine AS runtime

WORKDIR /app

RUN npm install -g pnpm@9

COPY node/gateway/package.json ./
RUN pnpm install --prod

COPY --from=build /app/dist ./dist
COPY dashboard ./dashboard
COPY ops/instruments.yml ./ops/instruments.yml

ENV NODE_ENV=production
CMD ["node", "dist/server.js"]
