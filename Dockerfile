FROM golang:1.25 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=dev
ARG TARGETARCH
RUN CGO_ENABLED=0 GOOS=linux GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" \
    -o /out/roommate-node ./cmd/node

# fuse3 provides fusermount3, which go-fuse invokes to unmount.
FROM debian:bookworm-slim
RUN apt-get update \
 && apt-get install -y --no-install-recommends fuse3 ca-certificates \
 && rm -rf /var/lib/apt/lists/*
COPY --from=build /out/roommate-node /usr/local/bin/roommate-node
ENTRYPOINT ["/usr/local/bin/roommate-node"]
