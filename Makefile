BIN     := bin
PKGS    := ./...
GOFILES := $(shell find . -name '*.go' -not -path './vendor/*')

VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
export VERSION

.PHONY: build test test-race cover vet fmt fmt-check lint tidy tidy-check check clean ko ko-local envtest e2e changelog-test

build:
	go build -o $(BIN)/roommate-node ./cmd/node

test:
	go test $(PKGS)

test-race:
	go test -race $(PKGS)

cover:
	go test -race -coverprofile=cover.out $(PKGS)

vet:
	go vet $(PKGS)

fmt:
	gofmt -s -w $(GOFILES)

fmt-check:
	@out=$$(gofmt -s -l $(GOFILES)); \
	if [ -n "$$out" ]; then echo "not gofmt-clean:"; echo "$$out"; exit 1; fi

lint:
	golangci-lint run

tidy:
	go mod tidy

tidy-check:
	@cp go.mod go.mod.bak; cp go.sum go.sum.bak; \
	go mod tidy; \
	if ! diff -q go.mod go.mod.bak >/dev/null || ! diff -q go.sum go.sum.bak >/dev/null; then \
	  mv go.mod.bak go.mod; mv go.sum.bak go.sum; echo "go mod tidy needed"; exit 1; \
	fi; \
	rm -f go.mod.bak go.sum.bak

check: fmt-check vet lint tidy-check cover build changelog-test

clean:
	rm -rf $(BIN) cover.out

ENVTEST_K8S_VERSION ?= 1.31.0

envtest:
	KUBEBUILDER_ASSETS="$$(setup-envtest use $(ENVTEST_K8S_VERSION) -p path)" \
	go test -tags=envtest ./test/apisemantics/... -v

IMAGE_REPO ?= ghcr.io/middlendian/roommate-csi

# Build the image for every platform in .ko.yaml and discard it: a "does it
# build" check that needs no registry and no Docker daemon.
ko:
	KO_DOCKER_REPO=$(IMAGE_REPO) ko build --bare --push=false ./cmd/node

# Build for the host architecture and load it into the local Docker daemon.
ko-local:
	KO_DOCKER_REPO=ko.local ko build --bare --local --platform=linux/$$(go env GOARCH) ./cmd/node

e2e:
	hack/e2e.sh

changelog-test:
	hack/changelog_test.sh
