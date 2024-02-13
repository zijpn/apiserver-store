package rest

import (
	"context"

	"k8s.io/apimachinery/pkg/runtime"
)

// RESTDeleteStrategy defines deletion behavior on an object that follows Kubernetes
// API conventions.
type RESTDeleteStrategy interface {
	runtime.ObjectTyper

	// BeginDelete is an optional hook that can be used to indicate the method is supported
	BeginDelete(ctx context.Context) error

	Delete(ctx context.Context, key string) error
}
