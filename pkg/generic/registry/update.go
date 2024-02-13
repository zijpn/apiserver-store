package registry

import (
	"context"
	"fmt"
	"strings"

	reststore "github.com/henderiw/apiserver-store/pkg/rest"
	"github.com/henderiw/logger/log"
	"go.opentelemetry.io/otel/trace"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/registry/rest"
)

// Update performs an atomic update and set of the object. Returns the result of the update
// or an error. If the registry allows create-on-update, the create flow will be executed.
// A bool is returned along with the object and any errors, to indicate object creation.
func (r *Store) Update(ctx context.Context, name string, objInfo rest.UpdatedObjectInfo, createValidation rest.ValidateObjectFunc, updateValidation rest.ValidateObjectUpdateFunc, forceAllowCreate bool, options *metav1.UpdateOptions) (runtime.Object, bool, error) {
	ctx, span := r.Tracer.Start(ctx, fmt.Sprintf("%s:update", r.DefaultQualifiedResource.Resource), trace.WithAttributes())
	defer span.End()

	log := log.FromContext(ctx)
	log.Info("update")

	if err := r.UpdateStrategy.BeginUpdate(ctx); err != nil {
		return nil, false, err
	}

	key, err := r.KeyFunc(ctx, name)
	if err != nil {
		return nil, false, err
	}
	qualifiedResource := r.qualifiedResourceFromContext(ctx)
	creating := false

	existing, err := r.GetStrategy.Get(ctx, key)
	if err != nil {
		log.Error("update", "err", err.Error())
		if forceAllowCreate && strings.Contains(err.Error(), "not found") {
			// For server-side apply, we can create the object here
			creating = true
		} else {
			return nil, creating, apierrors.NewNotFound(qualifiedResource, name)
		}
	}

	obj, err := objInfo.UpdatedObject(ctx, existing)
	if err != nil {
		log.Error("update failed to construct UpdatedObject", "error", err.Error())
		return nil, creating, err
	}

	if err := reststore.BeforeUpdate(r.UpdateStrategy, ctx, obj, existing); err != nil {
		return nil, creating, err
	}
	// at this point we have a fully formed object.  It is time to call the validators that the apiserver
	// handling chain wants to enforce.
	if updateValidation != nil {
		if err := updateValidation(ctx, obj.DeepCopyObject(), existing.DeepCopyObject()); err != nil {
			return nil, creating, err
		}
	}

	if err := r.UpdateStrategy.Update(ctx, key, obj); err != nil {
		// TODO see if we need to return more errors
		return nil, creating, apierrors.NewInternalError(err)
	}

	// The operation has succeeded.  
	return obj, creating, nil
}
