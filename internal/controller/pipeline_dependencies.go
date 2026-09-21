/*
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
*/

package controller

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	appsv1alpha1 "carewright.io/carewright-operator/api/v1alpha1"
)

var objectStoreGVK = schema.GroupVersionKind{Group: "aistor.min.io", Version: "v1", Kind: "ObjectStore"}

const artifactStoreManagedByValue = "artifact-store-controller"

type artifactStoreSummary struct {
	Spec  *appsv1alpha1.ArtifactStoreSpec
	Name  string
	URL   string
	Ready bool
}

func reconcileArtifactStore(
	ctx context.Context,
	k8sClient client.Client,
	owner client.Object,
	requested *appsv1alpha1.ArtifactStoreSpec,
) (artifactStoreSummary, error) {
	if requested == nil {
		return artifactStoreSummary{}, nil
	}

	effective := *requested
	if requested.Managed == nil {
		return artifactStoreSummary{Spec: &effective, URL: effective.URL, Ready: effective.URL != ""}, nil
	}
	managed := *requested.Managed
	effective.Managed = &managed

	name := managed.Name
	if name == "" {
		name = dependencyResourceName(owner.GetName(), "artifacts", 63)
	}
	serviceName := dependencyResourceName(name, "api", 63)
	secretName := dependencyResourceName(name, "configuration", 63)
	effective.URL = fmt.Sprintf("http://%s.%s.svc.cluster.local:9000", serviceName, owner.GetNamespace())

	configuration := &corev1.Secret{}
	configuration.Name = secretName
	configuration.Namespace = owner.GetNamespace()
	_, err := controllerutil.CreateOrUpdate(ctx, k8sClient, configuration, func() error {
		if configuration.ResourceVersion != "" && configuration.Labels[managedByLabel] != artifactStoreManagedByValue {
			return fmt.Errorf("Secret %s/%s already exists and is not managed by CareWright", configuration.Namespace, configuration.Name)
		}
		if configuration.Labels == nil {
			configuration.Labels = map[string]string{}
		}
		configuration.Labels[managedByLabel] = artifactStoreManagedByValue
		if len(configuration.Data["config.env"]) == 0 {
			password, err := randomCredential()
			if err != nil {
				return err
			}
			configuration.Data = map[string][]byte{
				"config.env": []byte("export MINIO_ROOT_USER=carewright\nexport MINIO_ROOT_PASSWORD=" + password + "\n"),
			}
		}
		return nil
	})
	if err != nil {
		return artifactStoreSummary{}, fmt.Errorf("reconcile MinIO configuration Secret: %w", err)
	}

	storageSize := managed.StorageSize.String()
	if storageSize == "0" || storageSize == "" {
		storageSize = "5Gi"
	}
	servers := int64(managed.Servers)
	if servers == 0 {
		servers = 1
	}
	volumesPerServer := int64(managed.VolumesPerServer)
	if volumesPerServer == 0 {
		volumesPerServer = 1
	}
	pvcSpec := map[string]any{
		"accessModes": []any{"ReadWriteOnce"},
		"resources":   map[string]any{"requests": map[string]any{"storage": storageSize}},
	}
	if managed.StorageClassName != "" {
		pvcSpec["storageClassName"] = managed.StorageClassName
	}

	objectStore := objectStoreObject()
	objectStore.SetName(name)
	objectStore.SetNamespace(owner.GetNamespace())
	_, err = controllerutil.CreateOrUpdate(ctx, k8sClient, objectStore, func() error {
		if objectStore.GetResourceVersion() != "" && objectStore.GetLabels()[managedByLabel] != artifactStoreManagedByValue {
			return fmt.Errorf("ObjectStore %s/%s already exists and is not managed by CareWright", objectStore.GetNamespace(), objectStore.GetName())
		}
		labels := objectStore.GetLabels()
		if labels == nil {
			labels = map[string]string{}
		}
		labels[managedByLabel] = artifactStoreManagedByValue
		objectStore.SetLabels(labels)
		objectStore.Object["spec"] = map[string]any{
			"cache":               map[string]any{"enabled": false},
			"certificates":        map[string]any{"disableAutoCert": true},
			"configuration":       map[string]any{"name": secretName},
			"firewall":            map[string]any{"enabled": false},
			"mountPath":           "/export",
			"podManagementPolicy": "Parallel",
			"services": map[string]any{
				"minio":   map[string]any{"name": serviceName, "serviceType": "ClusterIP"},
				"console": map[string]any{"name": dependencyResourceName(name, "console", 63), "serviceType": "ClusterIP"},
			},
			"pools": []any{map[string]any{
				"name":             "pool-0",
				"servers":          servers,
				"volumesPerServer": volumesPerServer,
				"volumeClaimTemplate": map[string]any{
					"apiVersion": "v1",
					"kind":       "persistentvolumeclaims",
					"spec":       pvcSpec,
				},
			}},
			"rollingTimeoutSeconds": int64(600),
		}
		return nil
	})
	if err != nil {
		return artifactStoreSummary{}, fmt.Errorf("reconcile MinIO ObjectStore: %w", err)
	}

	currentState, _, _ := unstructured.NestedString(objectStore.Object, "status", "currentState")
	healthStatus, _, _ := unstructured.NestedString(objectStore.Object, "status", "healthStatus")
	ready := strings.EqualFold(currentState, "initialized") || strings.EqualFold(healthStatus, "green")
	return artifactStoreSummary{Spec: &effective, Name: name, URL: effective.URL, Ready: ready}, nil
}

func objectStoreObject() *unstructured.Unstructured {
	object := &unstructured.Unstructured{}
	object.SetGroupVersionKind(objectStoreGVK)
	return object
}

func dependencyResourceName(base, suffix string, maxLength int) string {
	candidate := strings.Trim(base+"-"+suffix, "-")
	if len(candidate) <= maxLength && len(validation.IsDNS1123Label(candidate)) == 0 {
		return candidate
	}
	hash := shortHash(candidate)
	maxPrefix := maxLength - len(hash) - len(suffix) - 2
	if maxPrefix < 1 {
		maxPrefix = maxLength - len(hash) - 1
		suffix = ""
	}
	prefix := strings.Trim(base[:min(len(base), maxPrefix)], "-")
	if suffix == "" {
		return prefix + "-" + hash
	}
	return prefix + "-" + suffix + "-" + hash
}

func randomCredential() (string, error) {
	value := make([]byte, 24)
	if _, err := rand.Read(value); err != nil {
		return "", fmt.Errorf("generate MinIO root password: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(value), nil
}
