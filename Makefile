# BUILD_VERSION is the latest tag.
BUILD_VERSION := $(shell git describe --tags --always)
GO=$(shell which go)
RM=rm -f ./bin/*

# fake-gcs-server variables
FAKE_GCS_DOCKER_IMAGE = fsouza/fake-gcs-server
FAKE_GCS_DOCKER_CONTAINER = fake-gcs-server
FAKE_GCS_SHEME = http
FAKE_GCS_PORT = 4443
FAKE_GCS_HOST = 0.0.0.0
STORAGE_EMULATOR_HOST = $(FAKE_GCS_SHEME)://$(FAKE_GCS_HOST):$(FAKE_GCS_PORT)

# windows specific commands
ifeq ($(OS), Windows_NT)
	MV=move bin\opair bin\opair.exe
	GO=go
	RM=IF exist bin (cmd /c del /s /q bin && rmdir bin)
endif

#
# Go sources management.
#

.PHONY: build
build: clean-bin
	$(GO) build -ldflags="-X 'optable-pair-cli/pkg/cmd/cli.version=${BUILD_VERSION}'" -o bin/opair pkg/cmd/main.go
	$(MV)

.PHONY: release
release: darwin linux windows

.PHONY: darwin
darwin: darwin-amd64 darwin-arm64
	$(GO) install github.com/randall77/makefat@7ddd0e42c8442593c87c1705a5545099604008e5
	makefat release/opair-darwin release/opair-darwin-amd64 release/opair-darwin-arm64
	rm release/opair-darwin-amd64 release/opair-darwin-arm64

.PHONY: darwin-amd64
darwin-amd64:
	make clean-bin ;\
	CGO_ENABLED=0 GOOS=darwin GOARCH=amd64 make build ;\
	mkdir -p release && cp bin/opair release/opair-darwin-amd64

.PHONY: darwin-arm64
darwin-arm64:
	make clean-bin ;\
	CGO_ENABLED=0 GOOS=darwin GOARCH=arm64 make build ;\
	mkdir -p release && cp bin/opair release/opair-darwin-arm64

.PHONY: linux
linux:
	make clean-bin ;\
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 make build ;\
	mkdir -p release && cp bin/opair release/opair-linux-amd64

.PHONY: windows
windows:
	make clean-bin ;\
	CGO_ENABLED=0 GOOS=windows GOARCH=amd64 make build ;\
	mkdir -p release && cp bin/opair release/opair-windows-amd64.exe

.PHONY: clean
clean: clean-bin clean-release clean-fake-gcs-server

.PHONY: clean-bin
clean-bin:
	$(RM)

.PHONY: clean-release
clean-release:
	rm -f release/*

.PHONY: start-fake-gcs-server
start-fake-gcs-server:
	@if [ $$(docker ps -q -f name=$(FAKE_GCS_DOCKER_CONTAINER)) ]; then \
		echo "Container $(FAKE_GCS_DOCKER_CONTAINER) is already running."; \
	elif [ $$(docker ps -aq -f name=$(FAKE_GCS_DOCKER_CONTAINER)) ]; then \
		echo "Starting existing container $(FAKE_GCS_DOCKER_CONTAINER)..."; \
		docker start $(FAKE_GCS_DOCKER_CONTAINER); \
	else \
		echo "Creating and starting container $(FAKE_GCS_DOCKER_CONTAINER)..."; \
		docker run -d --name $(FAKE_GCS_DOCKER_CONTAINER) -p $(FAKE_GCS_PORT):$(FAKE_GCS_PORT) $(FAKE_GCS_DOCKER_IMAGE) \
			-scheme $(FAKE_GCS_SHEME) \
			-public-host $(FAKE_GCS_HOST):$(FAKE_GCS_PORT); \
	fi

.PHONY: clean-fake-gcs-server
clean-fake-gcs-server:
	@if [ $$(docker ps -q -f name=$(FAKE_GCS_DOCKER_CONTAINER)) ]; then \
		echo "Stopping container $(FAKE_GCS_DOCKER_CONTAINER)..."; \
		docker stop $(FAKE_GCS_DOCKER_CONTAINER); \
	fi
	@if [ $$(docker ps -aq -f name=$(FAKE_GCS_DOCKER_CONTAINER)) ]; then \
		echo "Removing container $(FAKE_GCS_DOCKER_CONTAINER)..."; \
		docker rm $(FAKE_GCS_DOCKER_CONTAINER); \
	fi

.PHONY: test
test: start-fake-gcs-server
	$(GO) test ./...
