# syntax=docker/dockerfile:1.7
# Multi-stage: build with the full toolchain, ship a ~10 MB distroless image
# that contains nothing but the static binary and CA certs.

FROM golang:1.27-alpine AS build
WORKDIR /src
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=bind,source=go.mod,target=go.mod \
    --mount=type=bind,source=go.sum,target=go.sum \
    go mod download
COPY . .
ARG VERSION=dev
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux go build -trimpath \
      -ldflags="-s -w -X main.version=${VERSION}" \
      -o /out/ledgerd ./cmd/ledgerd

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/ledgerd /ledgerd
USER nonroot:nonroot
EXPOSE 8080
ENTRYPOINT ["/ledgerd"]
