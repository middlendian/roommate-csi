BIN     := bin
PKGS    := ./...
GOFILES := $(shell find . -name '*.go' -not -path './vendor/*')

.PHONY: build test test-race cover vet fmt fmt-check lint tidy tidy-check check clean docker envtest

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

check: fmt-check vet lint tidy-check cover build

clean:
	rm -rf $(BIN) cover.out

ENVTEST_K8S_VERSION ?= 1.31.0

envtest:
	KUBEBUILDER_ASSETS="$$(setup-envtest use $(ENVTEST_K8S_VERSION) -p path)" \
	go test -tags=envtest ./test/apisemantics/... -v

IMAGE   ?= ghcr.io/middlendian/roommate-csi
TAG     ?= dev

docker:
	docker build --build-arg VERSION=$(TAG) -t $(IMAGE):$(TAG) .
