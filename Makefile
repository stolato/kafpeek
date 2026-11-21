.PHONY: kafka-up kafka-down seed run web web-dev build version image image-push demo

VERSION    := $(shell cat VERSION 2>/dev/null || echo dev)
COMMIT     := $(shell git rev-parse --short HEAD 2>/dev/null)
BUILD_DATE := $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
PKG        := github.com/stolato/kafpeek/internal/version
IMAGE      ?= kafpeek
CONTAINER  ?= $(shell command -v docker >/dev/null 2>&1 && echo docker || echo podman)
PLATFORMS  ?= linux/amd64,linux/arm64
# podman's default OCI format drops HEALTHCHECK; the docker format keeps it
FORMAT     := $(if $(findstring podman,$(CONTAINER)),--format docker,)
LDFLAGS    := -s -w -X $(PKG).Version=$(VERSION) -X $(PKG).Commit=$(COMMIT) -X $(PKG).BuildDate=$(BUILD_DATE)

kafka-up:        ## start local Kafka
	podman compose up -d

kafka-down:      ## stop local Kafka
	podman compose down

seed:            ## create demo topics and produce fake events (local only)
	go run ./cmd/seed

run:             ## run the backend (API on :8090; serves the UI if built)
	go run ./cmd/kafpeek

web-dev:         ## Angular dev server with hot reload on :4200 (proxies /api to :8090)
	cd web && npm start

web:             ## build the Angular app into web/dist (embedded by go build)
	cd web && npm ci && npm run build

build: web       ## single self-contained binary at bin/kafpeek
	CGO_ENABLED=0 go build -trimpath -ldflags="$(LDFLAGS)" -o bin/kafpeek ./cmd/kafpeek

image:           ## build the container image for this machine: make image [IMAGE=registry/kafpeek]
	$(CONTAINER) build $(FORMAT) --build-arg VERSION=$(VERSION) --build-arg COMMIT=$(COMMIT) \
		-t $(IMAGE):$(VERSION) -t $(IMAGE):latest .

image-push:      ## multi-arch build + push (docker buildx): make image-push IMAGE=registry/kafpeek
	docker buildx build --platform $(PLATFORMS) --build-arg VERSION=$(VERSION) --build-arg COMMIT=$(COMMIT) \
		-t $(IMAGE):$(VERSION) -t $(IMAGE):latest --push .

demo:            ## local Kafka + kafpeek in containers (http://localhost:8090)
	VERSION=$(VERSION) COMMIT=$(COMMIT) $(CONTAINER) compose --profile ui up -d --build

version:         ## print the version that will be embedded
	@echo $(VERSION) $(COMMIT) $(BUILD_DATE)
