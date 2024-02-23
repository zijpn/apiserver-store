package registry

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/henderiw/logger/log"
	"go.opentelemetry.io/otel/trace"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/apiserver/pkg/storage"
)

// Delete removes the item from storage.
// options can be mutated by rest.BeforeDelete due to a graceful deletion strategy.
func (r *Store) Delete(ctx context.Context, name string, deleteValidation rest.ValidateObjectFunc, options *metav1.DeleteOptions) (runtime.Object, bool, error) {
	ctx, span := r.Tracer.Start(ctx, fmt.Sprintf("%s:delete", r.DefaultQualifiedResource.Resource), trace.WithAttributes())
	defer span.End()

	log := log.FromContext(ctx).With("name", name)
	log.Info("delete")

	if err := r.DeleteStrategy.BeginDelete(ctx); err != nil {
		return nil, false, err
	}

	key, err := r.KeyFunc(ctx, name)
	if err != nil {
		return nil, false, err
	}
	qualifiedResource := r.qualifiedResourceFromContext(ctx)

	obj, err := r.GetStrategy.Get(ctx, key)
	if err != nil {
		return nil, false, apierrors.NewNotFound(qualifiedResource, name)
	}

	// support older consumers of delete by treating "nil" as delete immediately
	if options == nil {
		options = metav1.NewDeleteOptions(0)
	}
	var preconditions storage.Preconditions
	if options.Preconditions != nil {
		preconditions.UID = options.Preconditions.UID
		preconditions.ResourceVersion = options.Preconditions.ResourceVersion
	}
	graceful, pendingGraceful, err := rest.BeforeDelete(r.DeleteStrategy, ctx, obj, options)
	if err != nil {
		return nil, false, err
	}
	// this means finalizers cannot be updated via DeleteOptions if a deletion is already pending
	if pendingGraceful {
		out, err := r.finalizeDelete(ctx, obj, false, options)
		return out, false, err
	}
	// check if obj has pending finalizers
	accessor, err := meta.Accessor(obj)
	if err != nil {
		return nil, false, apierrors.NewInternalError(err)
	}
	pendingFinalizers := len(accessor.GetFinalizers()) != 0
	var deleteImmediately bool = true
	var out runtime.Object

	// Handle combinations of graceful deletion and finalization by issuing
	// the correct updates.
	shouldUpdateFinalizers, _ := deletionFinalizersForGarbageCollection(ctx, r, accessor, options)
	// TODO: remove the check, because we support no-op updates now.
	if graceful || pendingFinalizers || shouldUpdateFinalizers {
		deleteImmediately, out, err = r.updateForGracefulDeletionAndFinalizers(ctx, name, key, options, preconditions, deleteValidation, obj)
	}
	// !deleteImmediately covers all cases where err != nil. We keep both to be future-proof.
	if !deleteImmediately || err != nil {
		return out, false, err
	}

	if derr := r.DeleteStrategy.Delete(ctx, key, obj, isDryRun(options.DryRun)); derr != nil {
		obj, err = r.finalizeDelete(ctx, obj, true, options)
		return obj, false, apierrors.NewInternalError(errors.Join(derr, err))
	}
	obj, err = r.finalizeDelete(ctx, obj, true, options)
	return obj, true, err
}

// qualifiedResourceFromContext attempts to retrieve a GroupResource from the context's request info.
// If the context has no request info, DefaultQualifiedResource is used.
func (r *Store) qualifiedResourceFromContext(ctx context.Context) schema.GroupResource {
	if info, ok := genericapirequest.RequestInfoFrom(ctx); ok {
		return schema.GroupResource{Group: info.APIGroup, Resource: info.Resource}
	}
	// some implementations access storage directly and thus the context has no RequestInfo
	return r.DefaultQualifiedResource
}

// finalizeDelete runs the Store's AfterDelete hook if runHooks is set and
// returns the decorated deleted object if appropriate.
func (r *Store) finalizeDelete(ctx context.Context, obj runtime.Object, runHooks bool, options *metav1.DeleteOptions) (runtime.Object, error) {
	if r.ReturnDeletedObject {
		return obj, nil
	}
	// Return information about the deleted object, which enables clients to
	// verify that the object was actually deleted and not waiting for finalizers.
	accessor, err := meta.Accessor(obj)
	if err != nil {
		return nil, err
	}
	qualifiedResource := r.qualifiedResourceFromContext(ctx)
	details := &metav1.StatusDetails{
		Name:  accessor.GetName(),
		Group: qualifiedResource.Group,
		Kind:  qualifiedResource.Resource, // Yes we set Kind field to resource.
		UID:   accessor.GetUID(),
	}
	status := &metav1.Status{Status: metav1.StatusSuccess, Details: details}
	return status, nil
}

// deletionFinalizersForGarbageCollection analyzes the object and delete options
// to determine whether the object is in need of finalization by the garbage
// collector. If so, returns the set of deletion finalizers to apply and a bool
// indicating whether the finalizer list has changed and is in need of updating.
//
// The finalizers returned are intended to be handled by the garbage collector.
// If garbage collection is disabled for the store, this function returns false
// to ensure finalizers aren't set which will never be cleared.
func deletionFinalizersForGarbageCollection(ctx context.Context, r *Store, accessor metav1.Object, options *metav1.DeleteOptions) (bool, []string) {
	if !r.EnableGarbageCollection {
		return false, []string{}
	}
	shouldOrphan := shouldOrphanDependents(ctx, r, accessor, options)
	shouldDeleteDependentInForeground := shouldDeleteDependents(ctx, r, accessor, options)
	newFinalizers := []string{}

	// first remove both finalizers, add them back if needed.
	for _, f := range accessor.GetFinalizers() {
		if f == metav1.FinalizerOrphanDependents || f == metav1.FinalizerDeleteDependents {
			continue
		}
		newFinalizers = append(newFinalizers, f)
	}

	if shouldOrphan {
		newFinalizers = append(newFinalizers, metav1.FinalizerOrphanDependents)
	}
	if shouldDeleteDependentInForeground {
		newFinalizers = append(newFinalizers, metav1.FinalizerDeleteDependents)
	}

	oldFinalizerSet := sets.NewString(accessor.GetFinalizers()...)
	newFinalizersSet := sets.NewString(newFinalizers...)
	if oldFinalizerSet.Equal(newFinalizersSet) {
		return false, accessor.GetFinalizers()
	}
	return true, newFinalizers
}

// shouldOrphanDependents returns true if the finalizer for orphaning should be set
// updated for FinalizerOrphanDependents. In the order of highest to lowest
// priority, there are three factors affect whether to add/remove the
// FinalizerOrphanDependents: options, existing finalizers of the object,
// and e.DeleteStrategy.DefaultGarbageCollectionPolicy.
func shouldOrphanDependents(ctx context.Context, e *Store, accessor metav1.Object, options *metav1.DeleteOptions) bool {
	// Get default GC policy from this REST object type
	gcStrategy, ok := e.DeleteStrategy.(rest.GarbageCollectionDeleteStrategy)
	var defaultGCPolicy rest.GarbageCollectionPolicy
	if ok {
		defaultGCPolicy = gcStrategy.DefaultGarbageCollectionPolicy(ctx)
	}

	if defaultGCPolicy == rest.Unsupported {
		// return  false to indicate that we should NOT orphan
		return false
	}

	// An explicit policy was set at deletion time, that overrides everything
	//nolint:staticcheck // SA1019 backwards compatibility
	if options != nil && options.OrphanDependents != nil {
		//nolint:staticcheck // SA1019 backwards compatibility
		return *options.OrphanDependents
	}
	if options != nil && options.PropagationPolicy != nil {
		switch *options.PropagationPolicy {
		case metav1.DeletePropagationOrphan:
			return true
		case metav1.DeletePropagationBackground, metav1.DeletePropagationForeground:
			return false
		}
	}

	// If a finalizer is set in the object, it overrides the default
	// validation should make sure the two cases won't be true at the same time.
	finalizers := accessor.GetFinalizers()
	for _, f := range finalizers {
		switch f {
		case metav1.FinalizerOrphanDependents:
			return true
		case metav1.FinalizerDeleteDependents:
			return false
		}
	}

	// Get default orphan policy from this REST object type if it exists
	return defaultGCPolicy == rest.OrphanDependents
}

// shouldDeleteDependents returns true if the finalizer for foreground deletion should be set
// updated for FinalizerDeleteDependents. In the order of highest to lowest
// priority, there are three factors affect whether to add/remove the
// FinalizerDeleteDependents: options, existing finalizers of the object, and
// e.DeleteStrategy.DefaultGarbageCollectionPolicy.
func shouldDeleteDependents(ctx context.Context, e *Store, accessor metav1.Object, options *metav1.DeleteOptions) bool {
	// Get default GC policy from this REST object type
	if gcStrategy, ok := e.DeleteStrategy.(rest.GarbageCollectionDeleteStrategy); ok && gcStrategy.DefaultGarbageCollectionPolicy(ctx) == rest.Unsupported {
		// return false to indicate that we should NOT delete in foreground
		return false
	}

	// If an explicit policy was set at deletion time, that overrides both
	//nolint:staticcheck // SA1019 backwards compatibility
	if options != nil && options.OrphanDependents != nil {
		return false
	}
	if options != nil && options.PropagationPolicy != nil {
		switch *options.PropagationPolicy {
		case metav1.DeletePropagationForeground:
			return true
		case metav1.DeletePropagationBackground, metav1.DeletePropagationOrphan:
			return false
		}
	}

	// If a finalizer is set in the object, it overrides the default
	// validation has made sure the two cases won't be true at the same time.
	finalizers := accessor.GetFinalizers()
	for _, f := range finalizers {
		switch f {
		case metav1.FinalizerDeleteDependents:
			return true
		case metav1.FinalizerOrphanDependents:
			return false
		}
	}

	return false
}

// updateForGracefulDeletionAndFinalizers updates the given object for
// graceful deletion and finalization by setting the deletion timestamp and
// grace period seconds (graceful deletion) and updating the list of
// finalizers (finalization); it returns:
//
//  1. a boolean indicating that the object's grace period is exhausted and it
//     should be deleted immediately
//  2. a new output object with the state that was updated
//  3. a copy of the last existing state of the object
//  4. an error
func (r *Store) updateForGracefulDeletionAndFinalizers(ctx context.Context, name string, key types.NamespacedName, options *metav1.DeleteOptions, preconditions storage.Preconditions, deleteValidation rest.ValidateObjectFunc, obj runtime.Object) (deleteImmediately bool, out runtime.Object, err error) {
	log := log.FromContext(ctx)
	lastGraceful := int64(0)
	var pendingFinalizers bool

	if err := deleteValidation(ctx, obj); err != nil {
		return false, obj, err
	}

	graceful, pendingGraceful, err := rest.BeforeDelete(r.DeleteStrategy, ctx, obj, options)
	if err != nil {
		return false, obj, err
	}
	if pendingGraceful {
		// already deleting
		out, err = r.finalizeDelete(ctx, obj, true, options)
		return false, out, err
	}

	old := obj.DeepCopyObject()

	// Add/remove the orphan finalizer as the options dictates.
	// Note that this occurs after checking pendingGraceufl, so
	// finalizers cannot be updated via DeleteOptions if deletion has
	// started.
	existingAccessor, err := meta.Accessor(obj)
	if err != nil {
		return false, obj, err
	}
	needsUpdate, newFinalizers := deletionFinalizersForGarbageCollection(ctx, r, existingAccessor, options)
	if needsUpdate {
		existingAccessor.SetFinalizers(newFinalizers)
	}
	pendingFinalizers = len(existingAccessor.GetFinalizers()) != 0
	if !graceful {
		// set the DeleteGracePeriods to 0 if the object has pendingFinalizers but not supporting graceful deletion
		if pendingFinalizers {
			log.Info("Object has pending finalizers, so the registry is going to update its status to deleting", "object", existingAccessor.GetName())
			err = markAsDeleting(obj, time.Now())
			if err != nil {
				return false, obj, err
			}
			obj, err := r.UpdateStrategy.Update(ctx, key, obj, old, isDryRun(options.DryRun))
			if err != nil {
				return false, obj, err
			}
			// If there are pending finalizers, we never delete the object immediately.
			if pendingFinalizers {
				return false, obj, nil
			}
			// If we are here, the registry supports grace period mechanism and
			// we are intentionally delete gracelessly. In this case, we may
			// enter a race with other k8s components. If other component wins
			// the race, the object will not be found, and we should tolerate
			// the NotFound error. See
			// https://github.com/kubernetes/kubernetes/issues/19403 for
			// details.
			return true, obj, nil
		}
		// deleteNow
		// we've updated the object to have a zero grace period, or it's already at 0, so
		// we should fall through and truly delete the object.
		return true, obj, nil
	}
	obj, err = r.UpdateStrategy.Update(ctx, key, obj, old, isDryRun(options.DryRun))
	if err != nil {
		return false, obj, err
	}
	lastGraceful = *options.GracePeriodSeconds
	// If there are pending finalizers, we never delete the object immediately.
	if pendingFinalizers {
		return false, obj, nil
	}
	if lastGraceful > 0 {
		return false, obj, nil
	}
	// If we are here, the registry supports grace period mechanism and
	// we are intentionally delete gracelessly. In this case, we may
	// enter a race with other k8s components. If other component wins
	// the race, the object will not be found, and we should tolerate
	// the NotFound error. See
	// https://github.com/kubernetes/kubernetes/issues/19403 for
	// details.
	return true, obj, nil
}

// markAsDeleting sets the obj's DeletionGracePeriodSeconds to 0, and sets the
// DeletionTimestamp to "now" if there is no existing deletionTimestamp or if the existing
// deletionTimestamp is further in future. Finalizers are watching for such updates and will
// finalize the object if their IDs are present in the object's Finalizers list.
func markAsDeleting(obj runtime.Object, now time.Time) (err error) {
	objectMeta, kerr := meta.Accessor(obj)
	if kerr != nil {
		return kerr
	}
	// This handles Generation bump for resources that don't support graceful
	// deletion. For resources that support graceful deletion is handle in
	// pkg/api/rest/delete.go
	if objectMeta.GetDeletionTimestamp() == nil && objectMeta.GetGeneration() > 0 {
		objectMeta.SetGeneration(objectMeta.GetGeneration() + 1)
	}
	existingDeletionTimestamp := objectMeta.GetDeletionTimestamp()
	if existingDeletionTimestamp == nil || existingDeletionTimestamp.After(now) {
		metaNow := metav1.NewTime(now)
		objectMeta.SetDeletionTimestamp(&metaNow)
	}
	var zero int64 = 0
	objectMeta.SetDeletionGracePeriodSeconds(&zero)
	return nil
}
