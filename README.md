# release-controller

Discovers the releases of an OCI image and keeps each image's catalogue in
`ReleaseSlice` objects of up to 100 releases, so a Rollout's status no longer grows
with your commit rate. rollout-controller reads the catalogue when started with
`--release-slices`; the dashboard and environment-controller read it too.
Design: `internal/design/release-controller.md` (§9 migration, §10 RBAC, §11 delivery).

## CRDs (`kuberik.com/v1alpha1`)

| Kind | Short | Who writes it | Purpose |
|---|---|---|---|
| `ReleaseSource` | `relsrc` | users (one per image, in the Rollout's namespace) | Where releases come from (`spec.oci`: an `imagePolicyRef`, or `image` + `secretRef` + `filterTags` + `policy`) and `spec.retention` (`days`, default 7; `minCount`, default 30). |
| `ReleaseSlice` | | release-controller only | A sealed or open chunk of the catalogue. Never user-writable. |

There is no automatic materialisation: you create one `ReleaseSource` per image and
set `spec.releaseSourceRef` on each Rollout yourself.

## Flags

| Flag | Default | |
|---|---|---|
| `--max-releases-per-slice` | 100 | 1-1000 |
| `--max-slice-bytes` | 262144 | byte valve beside the count cap |
| `--max-resolves-per-sync` | 50 | |
| `--concurrent` | 4 | concurrent ReleaseSource reconciles |
| `--leader-elect` | true | |
| `--health-probe-bind-address` | `:8081` | |
| `--metrics-bind-address`, `--metrics-secure` | `0`, true | |

## Install

```sh
bin/kustomize build config/default | kubectl apply -f -     # CRDs, RBAC, Deployment (namespace release-controller-system)
```

Optional, where the rest of kuberik is installed: bind the `release-controller-reader`
ClusterRole to the rollout-controller, rollout-dashboard and environment-controller
service accounts (all in `kuberik-system`):

```sh
bin/kustomize build config/readers | kubectl apply -f -
```

RBAC: the controller has full access to `releasesources`, `releasesources/status` and
`releaseslices`, read on secrets, ImagePolicies and ImageRepositories, events and
leases. `release-controller-reader` (get/list/watch on both kinds) is aggregated into
`view`; `release-controller-source-editor` is aggregated into `edit`.

## Manual upgrade step

**Manual upgrade step** (before turning `--release-slices` on):
1. Install release-controller.
2. For each image a Rollout uses, create a `ReleaseSource` in the Rollout's namespace, either pointing at the existing ImagePolicy or naming the image directly, with the retention the Rollout had:
   ```yaml
   apiVersion: kuberik.com/v1alpha1
   kind: ReleaseSource
   metadata: {name: hello-world-app, namespace: hello-world-dev}
   spec:
     oci:
       imagePolicyRef: {name: hello-world-app}   # or: image + secretRef + filterTags + policy
     retention: {days: 7, minCount: 30}          # from availableReleasesRetentionDays / MinCount
   ```
3. Wait for `kubectl get relsrc` to show `Ready=True`, then set `spec.releaseSourceRef: {name: hello-world-app}` on the Rollout (in Git, where Flux owns it).
4. Turn on `--release-slices`. Any Rollout missed in step 3 shows `ReleaseSourceReady=False` with the object to create, and does nothing until it is fixed.

Ready-made ReleaseSources and Rollout patches for the kuberik-testing examples are in
`config/samples/kuberik-testing/{spoke,hub}/` (`releasesources.yaml` is what
`kustomize build` renders; `rollout-patches.yaml` holds the per-Rollout
`releaseSourceRef` snippets, which belong in Git where Flux owns the Rollouts).
Regenerate from a parity baseline with
`hack/gen-examples/gen.py <baseline.json> <out-dir>`.

## Dev deploy (kind)

`ko apply` uses the current kubectl context, so never use it. Build with `ko resolve`
and apply with an explicit `--context`; `hack/deploy-kind.sh` does exactly this:

```sh
hack/deploy-kind.sh rollout-dev kind-rollout-dev --readers   # cluster name, kubectl context
hack/deploy-kind.sh rollout-dev kind-rollout-dev --build-only # resolve + load into kind, no apply
```

Equivalent by hand:

```sh
bin/kustomize build config/dev \
  | KIND_CLUSTER_NAME=rollout-dev KO_DOCKER_REPO=kind.local ko resolve -f - >| ../.work/release-controller/resolved-rollout-dev.yaml
kubectl --context kind-rollout-dev apply -f ../.work/release-controller/resolved-rollout-dev.yaml
```

`config/dev` deploys into the existing `kuberik-system` namespace (the Namespace object
is dropped) with the image `ko://github.com/kuberik/release-controller/cmd`. The script
is idempotent.

## Development

`command make test` (envtest), `command make manifests generate`, `command make build`.
