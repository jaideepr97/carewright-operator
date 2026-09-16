# Build the manager binary on the host architecture and cross-compile it for
# the requested target. This avoids relying on QEMU during multi-arch builds.
ARG BUILDPLATFORM
FROM --platform=${BUILDPLATFORM} golang:1.24 AS builder
ARG TARGETOS
ARG TARGETARCH

WORKDIR /workspace
# Copy the Go Modules manifests
COPY go.mod go.mod
COPY go.sum go.sum
# cache deps before building and copying source so that we don't need to re-download as much
# and so that source changes don't invalidate our downloaded layer
RUN go mod download

# Copy the go source
COPY cmd/main.go cmd/main.go
COPY api/ api/
COPY internal/ internal/

# Build
# the GOARCH has not a default value to allow the binary be built according to the host where the command
# was called. For example, if we call make docker-build in a local env which has the Apple Silicon M1 SO
# the docker BUILDPLATFORM arg will be linux/arm64 when for Apple x86 it will be linux/amd64. Therefore,
# by leaving it empty we can ensure that the container and binary shipped on it will have the same platform.
RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH} go build -a -o manager cmd/main.go

# Fetch the target architecture's static OpenShell CLI while keeping the
# download stage on the host architecture.
FROM --platform=${BUILDPLATFORM} alpine:3.22 AS openshell-cli
ARG TARGETARCH
ARG OPENSHELL_VERSION=v0.0.111
RUN apk add --no-cache curl \
    && case "${TARGETARCH}" in \
         amd64) openshell_arch=x86_64; openshell_sha=eea22e10a1d21c92c843c609e2a391456073b339ebc5e1fca293aff4a0d6dcdc ;; \
         arm64) openshell_arch=aarch64; openshell_sha=5e9689f5e3522e84bd6e12fc48594046f312f356dc2f1779f628eb42e8259bf5 ;; \
         *) echo "unsupported architecture: ${TARGETARCH}" >&2; exit 1 ;; \
       esac \
    && openshell_archive="openshell-${openshell_arch}-unknown-linux-musl.tar.gz" \
    && curl -fsSL "https://github.com/NVIDIA/OpenShell/releases/download/${OPENSHELL_VERSION}/${openshell_archive}" -o "/tmp/${openshell_archive}" \
    && echo "${openshell_sha}  /tmp/${openshell_archive}" | sha256sum -c - \
    && tar -xzf "/tmp/${openshell_archive}" -C /usr/local/bin openshell \
    && chmod 0755 /usr/local/bin/openshell

# Use distroless as minimal base image to package the manager binary
# Refer to https://github.com/GoogleContainerTools/distroless for more details
FROM gcr.io/distroless/static:nonroot
WORKDIR /
COPY --from=builder /workspace/manager .
COPY --from=openshell-cli /usr/local/bin/openshell /usr/local/bin/openshell
USER 65532:65532

ENTRYPOINT ["/manager"]
