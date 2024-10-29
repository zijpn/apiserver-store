package rest

import (
	"context"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
)

// RESTDeleteStrategy defines deletion behavior on an object that follows Kubernetes
// API conventions.
type RESTDeleteStrategy interface {
	runtime.ObjectTyper

	// BeginDelete is an optional hook that can be used to indicate the method is supported
	BeginDelete(ctx context.Context) error

	// called when async procedure is implemented by the storage layer
	InvokeDelete(ctx context.Context, obj runtime.Object, recusrion bool) (runtime.Object, error)

	Delete(ctx context.Context, key types.NamespacedName, obj runtime.Object, dryrun bool) (runtime.Object, error)
}
