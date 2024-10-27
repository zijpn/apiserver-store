package registry

import (
	"context"
	"errors"
	"fmt"

	reststore "github.com/henderiw/apiserver-store/pkg/rest"
	"github.com/henderiw/logger/log"
	"go.opentelemetry.io/otel/trace"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/registry/rest"
)

const (
	OptimisticLockErrorMsg = "the object has been modified; please apply your changes to the latest version and try again"
)

// Update performs an atomic update and set of the object. Returns the result of the update
// or an error. If the registry allows create-on-update, the create flow will be executed.
// A bool is returned along with the object and any errors, to indicate object creation.
func (r *Store) Update(ctx context.Context, name string, objInfo rest.UpdatedObjectInfo, createValidation rest.ValidateObjectFunc, updateValidation rest.ValidateObjectUpdateFunc, forceAllowCreate bool, options *metav1.UpdateOptions) (runtime.Object, bool, error) {
	ctx, span := r.Tracer.Start(ctx, fmt.Sprintf("%s:update", r.DefaultQualifiedResource.Resource), trace.WithAttributes())
	defer span.End()

	log := log.FromContext(ctx)
	log.Debug("update", "name", name, "objInfo", objInfo, "forceAllowCreate", forceAllowCreate, "options", options)

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
		log.Debug("update allowcreate", "allow create", r.UpdateStrategy.AllowCreateOnUpdate(), "forceAllowCreate", forceAllowCreate)
		if !r.UpdateStrategy.AllowCreateOnUpdate() && !forceAllowCreate {
			return nil, creating, apierrors.NewNotFound(qualifiedResource, name)
		}
		creating = true
	}

	obj, err := objInfo.UpdatedObject(ctx, existing)
	if err != nil {
		log.Error("update failed to construct UpdatedObject", "error", err.Error())
		return nil, creating, err
	}

	if creating {
		obj, err := r.Create(ctx, obj, createValidation, &metav1.CreateOptions{
			TypeMeta:        options.TypeMeta,
			DryRun:          options.DryRun,
			FieldManager:    options.FieldManager,
			FieldValidation: options.FieldValidation,
		})
		if err != nil {
			return nil, creating, err
		}
		return obj, creating, nil
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

	newaccessor, err := meta.Accessor(obj)
	if err != nil {
		return nil, creating, err
	}

	oldaccessor, err := meta.Accessor(existing)
	if err != nil {
		return nil, creating, err
	}

	if newaccessor.GetResourceVersion() != oldaccessor.GetResourceVersion() {
		return nil, false, apierrors.NewConflict(r.DefaultQualifiedResource, oldaccessor.GetName(), errors.New(OptimisticLockErrorMsg))
	}
	if oldaccessor.GetDeletionTimestamp() != nil && len(newaccessor.GetFinalizers()) == 0 {
		obj, err := r.DeleteStrategy.Delete(ctx, key, obj, isDryRun(options.DryRun))
		if err != nil {
			return nil, false, apierrors.NewInternalError(err)
		}
		// deleted
		return obj, false, nil
	}

	recursion := false
	if len(options.DryRun) == 1 && options.DryRun[0] == "recursion" {
		recursion = true
	}
	if err := r.UpdateStrategy.InvokeUpdate(ctx, obj, existing, recursion); err != nil {
		return nil, creating, err
	}

	obj, err = r.UpdateStrategy.Update(ctx, key, obj, existing, isDryRun(options.DryRun))
	if err != nil {
		// TODO see if we need to return more errors
		return obj, creating, apierrors.NewInternalError(err)
	}

	// The operation has succeeded.
	return obj, creating, nil
}
