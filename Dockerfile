# syntax=docker/dockerfile:1
FROM golang:1.23-alpine AS build
WORKDIR /src
COPY go.mod ./
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/agentmesh ./cmd/agentmesh

FROM alpine:3.21
RUN addgroup -S agentmesh && adduser -S -G agentmesh agentmesh
USER agentmesh
COPY --from=build /out/agentmesh /usr/local/bin/agentmesh
EXPOSE 8080
ENTRYPOINT ["agentmesh"]
