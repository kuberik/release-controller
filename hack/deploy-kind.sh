#!/usr/bin/env bash
# Build and deploy release-controller to a kind cluster.
#   hack/deploy-kind.sh <kind-cluster-name> <kubectl-context> [--readers] [--build-only]
# e.g. hack/deploy-kind.sh rollout-dev kind-rollout-dev --readers
# Idempotent. Uses `ko resolve` + `kubectl --context`; never `ko apply`, which
# uses the CURRENT kubectl context and can deploy to the wrong cluster.
set -euo pipefail

CLUSTER=${1:?usage: $0 <kind-cluster-name> <kubectl-context> [--readers] [--build-only]}
CONTEXT=${2:?usage: $0 <kind-cluster-name> <kubectl-context> [--readers] [--build-only]}
shift 2
READERS=0 BUILD_ONLY=0
for a in "$@"; do
  case $a in
    --readers) READERS=1 ;;
    --build-only) BUILD_ONLY=1 ;;
    *) echo "unknown flag $a" >&2; exit 2 ;;
  esac
done

ROOT=$(cd "$(dirname "$0")/.." && pwd)
KUSTOMIZE=${KUSTOMIZE:-$ROOT/bin/kustomize}
OUT=${OUT_DIR:-$ROOT/../.work/release-controller}
mkdir -p "$OUT"
RESOLVED=$OUT/resolved-$CLUSTER.yaml

cd "$ROOT"
"$KUSTOMIZE" build config/dev \
  | KIND_CLUSTER_NAME="$CLUSTER" KO_DOCKER_REPO=kind.local ko resolve -f - >| "$RESOLVED"
if [ "$READERS" = 1 ]; then
  { echo '---'; "$KUSTOMIZE" build config/readers; } >> "$RESOLVED"
fi
echo "resolved manifests: $RESOLVED"
[ "$BUILD_ONLY" = 1 ] && exit 0

kubectl --context "$CONTEXT" apply -f "$RESOLVED"
kubectl --context "$CONTEXT" -n kuberik-system rollout status deploy/release-controller-controller-manager
