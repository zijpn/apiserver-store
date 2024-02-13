package rest

import (
	"context"

	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	"k8s.io/apimachinery/pkg/runtime"
)

type ListStrategy interface {
	List(ctx context.Context, options *metainternalversion.ListOptions) (runtime.Object, error)
}
