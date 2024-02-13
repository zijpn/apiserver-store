package rest

import (
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// EnsureObjectNamespaceMatchesRequestNamespace returns an error if obj.Namespace and requestNamespace
// are both populated and do not match. If either is unpopulated, it modifies obj as needed to ensure
// obj.GetNamespace() == requestNamespace.
func EnsureObjectNamespaceMatchesRequestNamespace(requestNamespace string, obj metav1.Object) error {
	objNamespace := obj.GetNamespace()
	switch {
	case objNamespace == requestNamespace:
		// already matches, no-op
		return nil

	case objNamespace == metav1.NamespaceNone:
		// unset, default to request namespace
		obj.SetNamespace(requestNamespace)
		return nil

	case requestNamespace == metav1.NamespaceNone:
		// cluster-scoped, clear namespace
		obj.SetNamespace(metav1.NamespaceNone)
		return nil

	default:
		// mismatch, error
		return errors.NewBadRequest("the namespace of the provided object does not match the namespace sent on the request")
	}
}

// ExpectedNamespaceForScope returns the expected namespace for a resource, given the request namespace and resource scope.
func ExpectedNamespaceForScope(requestNamespace string, namespaceScoped bool) string {
	if namespaceScoped {
		return requestNamespace
	}
	return ""
}
