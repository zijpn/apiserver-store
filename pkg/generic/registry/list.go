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
	log.Info("list")

	logOptions(options)

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

func logOptions(options *metainternalversion.ListOptions) {
	logFieldSelectorOptions(options.FieldSelector)
	logLabelsSelectorOptions(options.LabelSelector)

}

func logFieldSelectorOptions(selector fields.Selector) {
	if selector == nil {
		return
	}

	requirements := selector.Requirements()
	for _, requirement := range requirements {
		fmt.Println("requirement.Operator", requirement.Operator)
		fmt.Println("requirement.Field", requirement.Field)
		fmt.Println("requirement.Value", requirement.Value)
	}
}

func logLabelsSelectorOptions(selector labels.Selector) {
	if selector == nil {
		return
	}

	requirements, _ := selector.Requirements()
	for _, requirement := range requirements {
		fmt.Println("requirement.Operator", requirement.Operator())
		fmt.Println("requirement.Field", requirement.Key())
		fmt.Println("requirement.Value", requirement.Values())
	}
}
