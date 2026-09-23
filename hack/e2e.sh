#!/usr/bin/env bash
set -euo pipefail

CLUSTER="${CLUSTER:-roommate-e2e}"
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

# ko builds for the host architecture and, via KO_DOCKER_REPO=kind.local,
# loads the image straight into every node of the kind cluster.
IMAGE_REF="$(cd "$ROOT" && KO_DOCKER_REPO=kind.local KIND_CLUSTER_NAME="$CLUSTER" \
  VERSION=e2e ko build --bare --platform="linux/$(go env GOARCH)" ./cmd/node)"
echo "built $IMAGE_REF"

# Point base at the image just built. kustomize won't take an absolute
# path to a base from a temporary overlay, so render and substitute —
# and fail loudly if the image line wasn't found, rather than deploying
# whatever tag base happens to reference.
MANIFESTS="$(kubectl kustomize "$ROOT/deploy/kustomize/base" \
  | sed "s|image: ghcr.io/middlendian/roommate-csi:.*|image: ${IMAGE_REF}|")"
if ! grep -qF "image: ${IMAGE_REF}" <<<"$MANIFESTS"; then
  echo "error: failed to substitute the roommate image into the rendered manifests" >&2
  exit 1
fi
kubectl --context "kind-$CLUSTER" apply -f - <<<"$MANIFESTS"
kubectl --context "kind-$CLUSTER" -n roommate-system rollout status ds/roommate-node --timeout=180s

KUBECONFIG_FILE="$(mktemp)"
kind get kubeconfig --name "$CLUSTER" > "$KUBECONFIG_FILE"
KUBECONFIG="$KUBECONFIG_FILE" go test -tags=e2e -timeout=20m -count=1 -v "$ROOT/test/e2e/..."
