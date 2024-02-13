package rest

import (
	"context"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
)

type RESTGetStrategy interface {
	Get(ctx context.Context, key types.NamespacedName) (runtime.Object, error)
}
