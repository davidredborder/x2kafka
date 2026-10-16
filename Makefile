.PHONY: all build clean test lint rpm srpm distclean

BINARY_NAME=x2kafka
EGRESS_BINARY_NAME=kafka2x
BIN=bin/$(BINARY_NAME)
EGRESS_BIN=bin/$(EGRESS_BINARY_NAME)
VERSION?= $(shell cat VERSION 2>/dev/null || git describe --abbrev=6 --tags HEAD --always 2>/dev/null || echo "1.0.0")
GIT_REVISION:= $(shell git describe --abbrev=6 --dirty --tags --always 2>/dev/null || echo "dev")

all: build

build:
	@mkdir -p bin
	go build -ldflags "-X main.version=$(VERSION) -X main.revision=$(GIT_REVISION)" -o $(BIN) ./cmd/x2kafka
	go build -ldflags "-X main.version=$(VERSION) -X main.revision=$(GIT_REVISION)" -o $(EGRESS_BIN) ./cmd/kafka2x

clean:
	rm -rf bin/
	$(MAKE) -C packaging/rpm clean

test:
	go test -v ./...

lint:
	go vet ./...
	@if command -v golangci-lint >/dev/null 2>&1; then \
		golangci-lint run ./...; \
	elif [ -f $$(go env GOPATH)/bin/golangci-lint ]; then \
		$$(go env GOPATH)/bin/golangci-lint run ./...; \
	else \
		echo "golangci-lint is not installed. Skipping extended lint."; \
	fi

rpm: clean
	$(MAKE) -C packaging/rpm rpm

srpm: clean
	$(MAKE) -C packaging/rpm srpm

distclean: clean
	$(MAKE) -C packaging/rpm distclean

install: build
	install -d $(DESTDIR)/usr/bin
	install -m 755 $(BIN) $(DESTDIR)/usr/bin/x2kafka
	install -d $(DESTDIR)/etc/x2kafka
	install -m 644 configs_example/x2kafka_config.json $(DESTDIR)/etc/x2kafka/config.json
