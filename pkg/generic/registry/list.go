package registry

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel/trace"
	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	"k8s.io/apimachinery/pkg/runtime"
)

// List returns a list of items matching labels and field according to the
// store's PredicateFunc.
func (r *Store) List(ctx context.Context, options *metainternalversion.ListOptions) (runtime.Object, error) {
	ctx, span := r.Tracer.Start(ctx, fmt.Sprintf("%s:list", r.DefaultQualifiedResource.Resource), trace.WithAttributes())
	defer span.End()

	/*
		label := labels.Everything()
		if options != nil && options.LabelSelector != nil {
			label = options.LabelSelector
		}
		field := fields.Everything()
		if options != nil && options.FieldSelector != nil {
			field = options.FieldSelector
		}
	*/
	// TODO

	return r.ListStrategy.List(ctx, options)
}
