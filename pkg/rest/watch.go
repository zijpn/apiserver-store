package rest

import (
	"context"

	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
)

// RESWatchStrategy defines watch behavior on an object that follows Kubernetes
// API conventions.
type RESTWatchStrategy interface {
	runtime.ObjectTyper

	// BeginWatch is an optional hook that can be used to indicate the method is supported
	BeginWatch(ctx context.Context) error

	Watch(ctx context.Context, options *metainternalversion.ListOptions) (watch.Interface, error)
}
