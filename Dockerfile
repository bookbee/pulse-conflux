# Build a static binary, then ship it on a minimal base.
#
# Pinned to the Go version go.mod declares (1.25), not pulse-gateway's 1.26.2 —
# see specs/001-event-agent-runtime/research.md D2.
FROM golang:1.25-alpine AS build

WORKDIR /src

# Dependencies first so a source-only change does not refetch them.
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/conflux ./cmd/conflux

FROM alpine:3.21

# Certificates for outbound HTTPS to real destinations; the stubs are plain HTTP
# locally, but the image should not need rebuilding when they are not.
RUN apk add --no-cache ca-certificates && adduser -D -u 10001 conflux

COPY --from=build /out/conflux /usr/local/bin/conflux

USER conflux

# Operational endpoints only. This is not an API surface.
EXPOSE 8090

# No CMD arguments: every tunable arrives through the environment, and each key
# is documented in .env.example (Constitution Principle I).
ENTRYPOINT ["/usr/local/bin/conflux"]
