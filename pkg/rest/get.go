package rest

import (
	"context"

	"k8s.io/apimachinery/pkg/runtime"
)

type GetStrategy interface {
	Get(ctx context.Context, key string) (runtime.Object, error)
}
