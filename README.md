# rollout-controller

The `rollout-controller` is a Kubernetes controller that manages the rollout of new application versions. It monitors a specified OCI registry for new image tags (versions) and updates the status of a `Rollout` custom resource to reflect the desired version based on gate checks and health checks. It does not directly deploy images but rather signals the desired version, which can then be acted upon by other components in a GitOps workflow (e.g., Flux CD).

## Description

This controller implements a progressive delivery strategy. Key functionalities include:

- **Version Discovery**: Periodically polls an OCI image registry (specified in `spec.releasesRepository`) to discover available application versions (tags).
- **Version Selection**: Selects the latest suitable version based on semantic versioning. It can also be overridden by `spec.wantedVersion`.
- **Gate Evaluation**: Integrates with `RolloutGate` custom resources. A rollout proceeds only if all relevant gates are passing or allow the selected version.
- **Health Checks & Bake Time**: After a version is notionally "deployed" (i.e., its selection is recorded in status), it can monitor `HealthCheck` resources for a specified `bakeTime` before considering the version stable.
- **Status Update**: Updates the `Rollout` custom resource's status with the history of deployed versions, available releases, and the status of gates and health checks.
- **Integration with Flux (via `FluxOCIRepositoryReconciler`)**: While the `Rollout` controller itself doesn't deploy images, it's designed to work in tandem with the `FluxOCIRepositoryReconciler`. The `Rollout` controller updates the `Rollout` status, and the `FluxOCIRepositoryReconciler` watches `Rollout` resources. When a `Rollout` indicates a new version, the `FluxOCIRepositoryReconciler` updates the tag of a linked Flux `OCIRepository` custom resource. This change is then picked up by Flux's source-controller to pull the new image manifest, and subsequently by kustomize-controller/helm-controller to deploy it.

The `Rollout` controller itself does not directly interact with deployment workloads (like Kubernetes Deployments or Argo Rollouts). It focuses on the pre-deployment phase of selecting and validating a version, relying on other controllers (typically GitOps tools like Flux) to enact the deployment based on the updated `OCIRepository`.

## Getting Started

### Prerequisites
- go version v1.24.2+
- docker version 17.03+.
- kubectl version v1.11.3+.
- Access to a Kubernetes v1.11.3+ cluster.

### To Deploy on the cluster
**Build and push your image to the location specified by `IMG`:**

```sh
make docker-build docker-push IMG=<some-registry>/rollout-controller:tag
```

**NOTE:** This image ought to be published in the personal registry you specified.
And it is required to have access to pull the image from the working environment.
Make sure you have the proper permission to the registry if the above commands don’t work.

**Install the CRDs into the cluster:**

```sh
make install
```

**Deploy the Manager to the cluster with the image specified by `IMG`:**

```sh
make deploy IMG=<some-registry>/rollout-controller:tag
```

> **NOTE**: If you encounter RBAC errors, you may need to grant yourself cluster-admin
privileges or be logged in as admin.

**Create instances of your solution**
You can apply the samples (examples) from the config/sample:

```sh
kubectl apply -k config/samples/
```

>**NOTE**: Ensure that the samples has default values to test it out.

### To Uninstall
**Delete the instances (CRs) from the cluster:**

```sh
kubectl delete -k config/samples/
```

**Delete the APIs(CRDs) from the cluster:**

```sh
make uninstall
```

**UnDeploy the controller from the cluster:**

```sh
make undeploy
```

## Project Distribution

Following the options to release and provide this solution to the users.

### By providing a bundle with all YAML files

1. Build the installer for the image built and published in the registry:

```sh
make build-installer IMG=<some-registry>/rollout-controller:tag
```

**NOTE:** The makefile target mentioned above generates an 'install.yaml'
file in the dist directory. This file contains all the resources built
with Kustomize, which are necessary to install this project without its
dependencies.

2. Using the installer

Users can just run 'kubectl apply -f <URL for YAML BUNDLE>' to install
the project, i.e.:

```sh
kubectl apply -f https://raw.githubusercontent.com/<org>/rollout-controller/<tag or branch>/dist/install.yaml
```

## Linking OCIRepositories to Rollouts

For a `Rollout` custom resource to trigger an update in a Flux `OCIRepository` (via the `FluxOCIRepositoryReconciler`), the `OCIRepository` resource must be annotated. This annotation allows the `FluxOCIRepositoryReconciler` component of the `rollout-controller` project to identify which `OCIRepository` it should update when its associated `Rollout` resource's deployment history changes.

The required annotation is:
- Key: `kuberik.com/rollout-name`
- Value: The name of the `Rollout` resource that should manage this `OCIRepository`.

When a `Rollout` (e.g., `my-app-rollout`) is reconciled by the main `Rollout` controller, its status is updated to reflect the latest chosen version. The `FluxOCIRepositoryReconciler` (a separate controller part of this project) observes these `Rollout` resources. If an `OCIRepository` in the same namespace has the annotation `kuberik.com/rollout-name: "my-app-rollout"`, the `FluxOCIRepositoryReconciler` will update that `OCIRepository`'s `spec.ref.tag` to the latest version from the `Rollout`'s status history. Any existing `digest` or `semver` in `spec.ref` will be cleared to ensure the `tag` takes precedence. This change to the `OCIRepository` is then processed by Flux CD to deploy the new version.

Here is an example snippet of an `OCIRepository` manifest:

```yaml
apiVersion: source.toolkit.fluxcd.io/v1beta2
kind: OCIRepository
metadata:
  name: my-app-image-repo
  namespace: default
  annotations:
    kuberik.com/rollout-name: "my-app-rollout" # Links this OCIRepository to the 'my-app-rollout' Rollout
spec:
  interval: 1m
  url: oci://ghcr.io/my-org/my-app
  # The tag will be updated by the Rollout controller
  # ref:
  #   tag: "initial-tag" # This will be overwritten
```

### By providing a Helm Chart

1. Build the chart using the optional helm plugin

```sh
kubebuilder edit --plugins=helm/v1-alpha
```

2. See that a chart was generated under 'dist/chart', and users
can obtain this solution from there.

**NOTE:** If you change the project, you need to update the Helm Chart
using the same command above to sync the latest changes. Furthermore,
if you create webhooks, you need to use the above command with
the '--force' flag and manually ensure that any custom configuration
previously added to 'dist/chart/values.yaml' or 'dist/chart/manager/manager.yaml'
is manually re-applied afterwards.

## Contributing
// TODO(user): Add detailed information on how you would like others to contribute to this project

This project also includes a controller that interacts with Flux CD's `OCIRepository` resources. See the "Linking OCIRepositories to Rollouts" section for more details on its configuration.

**NOTE:** Run `make help` for more information on all potential `make` targets

More information can be found via the [Kubebuilder Documentation](https://book.kubebuilder.io/introduction.html)

## License

Copyright 2025.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
