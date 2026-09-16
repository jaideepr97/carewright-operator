# cpgtoacp-operator

## Description

The cpgtoacp operator manages the CPG Ingester and Care Plan Writer pipelines.
Pipeline components are represented as OpenShell `SandboxRequest` resources so
they run as policy-controlled sandboxes rather than ordinary Deployments.

## Pipeline configuration

`CPGIngester` and `CarePlanWriter` expose typed configuration for artifact
storage, MLflow tracing, LLM access, external service endpoints, and component
behavior. Each component still owns its image. Credential fields refer to named
OpenShell providers rather than embedding Kubernetes secret values in
environment variables. The operator derives internal component and SonataFlow
addresses and publishes user-facing entry points in status.

`extraEnv` and `extraProviders` are available on components for settings that do
not yet have first-class fields. `extraEnv` is restricted to non-secret string
values. The former component and workflow `env` arrays are no longer part of the
v1alpha1 schema. The samples under `config/samples` show the complete first-pass
fields.

## OpenShell sandboxes

A `SandboxRequest` declares an image, command, non-secret environment values,
resource limits, credential providers, and an optional policy stored in a
ConfigMap. The controller uses the OpenShell CLI to create and observe the
sandbox. It replaces the sandbox when the request or referenced policy changes
and deletes it when the request is deleted.

For local development, install the OpenShell CLI and make sure its active
gateway is reachable before running `make run`. The operator container includes
the CLI; in-cluster requests should normally set `spec.gateway.endpoint` to a
gateway URL reachable from the manager pod. `gateway.name` and
`gateway.endpoint` are mutually exclusive.

For focused local development, run only selected controllers with
`--controllers`. For example, start only the SandboxRequest controller with:

```sh
go run ./cmd/main.go --controllers=sandboxrequest
```

Environment values in `spec.env` are passed on the command line and must not
contain secrets. Attach configured OpenShell credential providers through
`spec.providers` instead.

See `config/samples/apps_v1alpha1_sandboxrequest.yaml` for a minimal request and
ConfigMap-backed policy.

## Getting Started

### Prerequisites
- go version v1.24.0+
- docker version 17.03+.
- kubectl version v1.11.3+.
- Access to a Kubernetes v1.11.3+ cluster.

### To Deploy on the cluster
**Build and push your image to the location specified by `IMG`:**

```sh
make docker-build docker-push IMG=<some-registry>/cpgtoacp-operator:tag
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
make deploy IMG=<some-registry>/cpgtoacp-operator:tag
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
make build-installer IMG=<some-registry>/cpgtoacp-operator:tag
```

**NOTE:** The makefile target mentioned above generates an 'install.yaml'
file in the dist directory. This file contains all the resources built
with Kustomize, which are necessary to install this project without its
dependencies.

2. Using the installer

Users can just run 'kubectl apply -f <URL for YAML BUNDLE>' to install
the project, i.e.:

```sh
kubectl apply -f https://raw.githubusercontent.com/<org>/cpgtoacp-operator/<tag or branch>/dist/install.yaml
```

### By providing a Helm Chart

1. Build the chart using the optional helm plugin

```sh
operator-sdk edit --plugins=helm/v1-alpha
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

**NOTE:** Run `make help` for more information on all potential `make` targets

More information can be found via the [Kubebuilder Documentation](https://book.kubebuilder.io/introduction.html)

## License

Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
