package registry

import (
	"context"
	"fmt"

	"github.com/henderiw/logger/log"
	"go.opentelemetry.io/otel/trace"
	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
)

// List returns a list of items matching labels and field according to the
// store's PredicateFunc.
func (r *Store) List(ctx context.Context, options *metainternalversion.ListOptions) (runtime.Object, error) {
	ctx, span := r.Tracer.Start(ctx, fmt.Sprintf("%s:list", r.DefaultQualifiedResource.Resource), trace.WithAttributes())
	defer span.End()

	log := log.FromContext(ctx)
	log.Info("list", "listOptions", options)

	logOptions(ctx, options)

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

func logOptions(ctx context.Context, options *metainternalversion.ListOptions) {
	logFieldSelectorOptions(ctx, options.FieldSelector)
	logLabelsSelectorOptions(ctx, options.LabelSelector)

}

func logFieldSelectorOptions(ctx context.Context, selector fields.Selector) {
	log := log.FromContext(ctx)
	if selector == nil {
		log.Info("field requirement nil")
		return
	}

	requirements := selector.Requirements()
	for _, requirement := range requirements {
		log.Info("field requirement", "operator", requirement.Operator, "field", requirement.Field, "value", requirement.Value)
	}
}

func logLabelsSelectorOptions(ctx context.Context, selector labels.Selector) {
	log := log.FromContext(ctx)
	if selector == nil {
		log.Info("label requirement nil")
		return
	}

	requirements, _ := selector.Requirements()
	for _, requirement := range requirements {
		log.Info("label requirement", "operator", requirement.Operator(), "key", requirement.Key(), "values", requirement.Values())
	}
}
