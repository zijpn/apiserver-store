package rest

import (
	"context"

	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
)

// RESWatchStrategy defines watch behavior on an object that follows Kubernetes
// API conventions.
type RESWatchStrategy interface {
	runtime.ObjectTyper

	Watch(ctx context.Context, options *metainternalversion.ListOptions) (watch.Interface, error)
}
