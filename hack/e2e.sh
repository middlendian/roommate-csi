#!/usr/bin/env bash
set -euo pipefail

CLUSTER="${CLUSTER:-roommate-e2e}"
IMAGE="ghcr.io/middlendian/roommate-csi:dev"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

cleanup() {
  if [[ "${KEEP_CLUSTER:-}" != "1" ]]; then
    kind delete cluster --name "$CLUSTER" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

# The driver mounts FUSE inside its DaemonSet pod. If the host kernel module
# isn't loaded, every NodePublishVolume call fails and every test times out
# on a symptom instead of the cause — surface it up front instead.
if [[ ! -e /dev/fuse ]]; then
  sudo modprobe fuse || true
fi
if [[ ! -e /dev/fuse ]]; then
  echo "error: /dev/fuse is not present on this host; the e2e suite needs the fuse kernel module" >&2
  exit 1
fi

if ! kind get clusters | grep -qx "$CLUSTER"; then
  kind create cluster --name "$CLUSTER" --config "$ROOT/hack/kind.yaml"
fi

docker build --build-arg VERSION=e2e -t "$IMAGE" "$ROOT"
kind load docker-image "$IMAGE" --name "$CLUSTER"

kubectl --context "kind-$CLUSTER" apply -k "$ROOT/deploy/kustomize/base"
kubectl --context "kind-$CLUSTER" -n roommate-system rollout status ds/roommate-node --timeout=180s

KUBECONFIG_FILE="$(mktemp)"
kind get kubeconfig --name "$CLUSTER" > "$KUBECONFIG_FILE"
KUBECONFIG="$KUBECONFIG_FILE" go test -tags=e2e -timeout=20m -count=1 -v "$ROOT/test/e2e/..."
