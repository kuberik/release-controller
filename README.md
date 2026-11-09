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

A policy is required: `spec.oci.policy` with `image` (a Flux ImagePolicy always
carries one, so `imagePolicyRef` needs nothing extra).

## How releases are admitted and ordered

Every release has a `sequence`; readers sort by it (not by `created`). Sequence order
is policy order, numbers are never reused or renumbered, so sealed slices stay sealed.

- **Incremental**: a listed tag above the catalogue's maximum under the policy is
  admitted, oldest first within `--max-resolves-per-sync`, numbered max+1, max+2, ...
  Tags below the maximum are never resolved again (a restart costs one tag listing).
- **Backfill** (empty catalogue, or `status.backfillComplete=false`): walks the listed
  tags below the catalogue's minimum, newest first, writing each batch. A tag is
  admitted iff its `created` is not later than that of every admitted tag above it
  (it was the newest when pushed; backports and retags fail this). Releases with no
  created annotation are admitted by position (`created` = first seen, flagged
  `createdObserved`); they neither get rejected by the rule nor tighten it. The walk
  stops once `retention.minCount` releases are admitted and the last one is older than
  `retention.days`, or when the tags run out, then `backfillComplete=true`. A restart
  resumes from the catalogue's minimum.
- **Rejections**: a bounds violation is stored once with `metadataUnresolved: true` and
  `metadata["kuberik.com/rejected"]=<reason>`, one `MetadataRejected` Event, never
  retried. Transient failures retry per tag with backoff (10 s up to 1 h).
- **Retention** drops whole sealed slices, oldest by minimum sequence, while every
  release in them is expired and `minCount` stays met. The open slice and the slice
  holding the newest release are never dropped; a backfill leaves the lowest sequences
  in the open slice, so retention starts once it seals and a newer slice opens.
- **Rebuild**: slices are not user-editable. To rebuild a catalogue delete **all** of
  its slices; the controller backfills from scratch. Changing the image (or the image
  of the referenced ImageRepository) rebuilds too, after the new config lists
  successfully.

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
