# syntax=docker/dockerfile:1
#
# kafpeek — single static binary with the Angular UI embedded.
#   docker build -t kafpeek --build-arg VERSION=$(cat VERSION) --build-arg COMMIT=$(git rev-parse --short HEAD) .
# Multi-arch:
#   docker buildx build --platform linux/amd64,linux/arm64 -t <registry>/kafpeek:<version> --push .

# ---- 1) Angular build (runs on the build machine's arch; output is platform-independent)
FROM --platform=$BUILDPLATFORM node:24-alpine AS web
WORKDIR /src/web
COPY web/package.json web/package-lock.json ./
RUN npm ci --no-audit --no-fund
COPY web/ ./
RUN npm run build

# ---- 2) Go build (cross-compiles for the target platform, no cgo)
FROM --platform=$BUILDPLATFORM golang:1.27-alpine AS build
ARG TARGETOS
ARG TARGETARCH
ARG VERSION=dev
ARG COMMIT=""
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd/ cmd/
COPY internal/ internal/
COPY web/embed.go web/embed.go
COPY --from=web /src/web/dist/ web/dist/
RUN BUILD_DATE=$(date -u +%Y-%m-%dT%H:%M:%SZ) && \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath \
      -ldflags "-s -w \
        -X github.com/stolato/kafpeek/internal/version.Version=${VERSION} \
        -X github.com/stolato/kafpeek/internal/version.Commit=${COMMIT} \
        -X github.com/stolato/kafpeek/internal/version.BuildDate=${BUILD_DATE}" \
      -o /out/kafpeek ./cmd/kafpeek

# ---- 3) Runtime: distroless (no shell, CA certificates included), non-root
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/kafpeek /kafpeek
# Inside a container the app must listen on all interfaces; restrict exposure
# with the port mapping (e.g. -p 127.0.0.1:8090:8090) and set UI_USERNAME/UI_PASSWORD.
ENV HTTP_ADDR=0.0.0.0:8090 \
    HOME=/home/nonroot
EXPOSE 8090
USER nonroot:nonroot
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 CMD ["/kafpeek", "healthcheck"]
ENTRYPOINT ["/kafpeek"]
