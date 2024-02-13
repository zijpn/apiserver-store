package rest

import (
	"context"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// RESTDeleteStrategy defines deletion behavior on an object that follows Kubernetes
// API conventions.
type RESTDeleteStrategy interface {
	runtime.ObjectTyper

	// AfterDelete implements a further operation to run after a resource is
	// deleted and before it is decorated, optional.
	AfterDelete(obj runtime.Object, options *metav1.DeleteOptions)

	Delete(ctx context.Context, key string) error
}
