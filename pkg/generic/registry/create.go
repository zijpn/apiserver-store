package registry

import (
	"context"
	"fmt"

	"github.com/henderiw/logger/log"
	"go.opentelemetry.io/otel/trace"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/registry/rest"
	reststore "github.com/henderiw/apiserver-store/pkg/rest"
)

func (r *Store) Create(ctx context.Context, obj runtime.Object, createValidation rest.ValidateObjectFunc, options *metav1.CreateOptions) (runtime.Object, error) {
	ctx, span := r.Tracer.Start(ctx, fmt.Sprintf("%s:create", r.DefaultQualifiedResource.Resource), trace.WithAttributes())
	defer span.End()

	log := log.FromContext(ctx)
	log.Debug("create")

	if err := r.CreateStrategy.BeginCreate(ctx); err != nil {
		return nil, err
	}

	if objectMeta, err := meta.Accessor(obj); err != nil {
		return nil, err
	} else {
		// set createTimestamp and set UUID
		rest.FillObjectMetaSystemFields(objectMeta)
		if len(objectMeta.GetGenerateName()) > 0 && len(objectMeta.GetName()) == 0 {
			objectMeta.SetName(r.CreateStrategy.GenerateName(objectMeta.GetGenerateName()))
		}
		objectMeta.SetResourceVersion("0")
	}

	if err := reststore.BeforeCreate(r.CreateStrategy, ctx, obj); err != nil {
		return nil, err
	}
	// at this point we have a fully formed object.  It is time to call the validators that the apiserver
	// handling chain wants to enforce.
	if createValidation != nil {
		if err := createValidation(ctx, obj.DeepCopyObject()); err != nil {
			return nil, err
		}
	}
	name, err := r.ObjectNameFunc(obj)
	if err != nil {
		return nil, err
	}
	key, err := r.KeyFunc(ctx, name)
	if err != nil {
		return nil, err
	}
	//qualifiedResource := r.qualifiedResourceFromContext(ctx)
	obj, err = r.CreateStrategy.Create(ctx, key, obj, isDryRun(options.DryRun))
	if err != nil {
		// TODO see if we need to return more errors
		return nil, apierrors.NewInternalError(err)
	}

	// The operation has succeeded. 
	return obj, nil
}
