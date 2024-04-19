package registry

import (
	"context"
	"fmt"

	"github.com/henderiw/logger/log"
	"go.opentelemetry.io/otel/trace"
	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	"k8s.io/apimachinery/pkg/watch"
)

func (r *Store) Watch(ctx context.Context, options *metainternalversion.ListOptions) (watch.Interface, error) {
	ctx, span := r.Tracer.Start(ctx, fmt.Sprintf("%s:watch", r.DefaultQualifiedResource.Resource), trace.WithAttributes())
	defer span.End()

	log := log.FromContext(ctx)
	log.Debug("watch")

	if err := r.WatchStrategy.BeginWatch(ctx); err != nil {
		return nil, err
	}

	return r.WatchStrategy.Watch(ctx, options)
}
