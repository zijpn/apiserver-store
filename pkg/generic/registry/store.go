package registry

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	reststore "github.com/henderiw/apiserver-store/pkg/rest"
	"github.com/henderiw/logger/log"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/registry/generic"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/klog/v2"
)

var _ rest.StandardStorage = &Store{}
var _ rest.TableConvertor = &Store{}
var _ rest.SingularNameProvider = &Store{}

type Store struct {
	Tracer trace.Tracer
	// NewFunc returns a new instance of the type this registry returns for a
	// GET of a single object, e.g.:
	//
	// curl GET /apis/group/version/namespaces/my-ns/myresource/name-of-object
	NewFunc func() runtime.Object

	// NewListFunc returns a new list of the type this registry; it is the
	// type returned when the resource is listed, e.g.:
	//
	// curl GET /apis/group/version/namespaces/my-ns/myresource
	NewListFunc func() runtime.Object

	// DefaultQualifiedResource is the pluralized name of the resource.
	// This field is used if there is no request info present in the context.
	// See qualifiedResourceFromContext for details.
	DefaultQualifiedResource schema.GroupResource

	// SingularQualifiedResource is the singular name of the resource.
	SingularQualifiedResource schema.GroupResource

	// KeyRootFunc returns the root etcd key for this resource; should not
	// include trailing "/".  This is used for operations that work on the
	// entire collection (listing and watching).
	//
	// KeyRootFunc and KeyFunc must be supplied together or not at all.
	KeyRootFunc func(ctx context.Context) string

	// KeyFunc returns the key for a specific object in the collection.
	// KeyFunc is called for Create/Update/Get/Delete. Note that 'namespace'
	// can be gotten from ctx.
	//
	// KeyFunc and KeyRootFunc must be supplied together or not at all.
	KeyFunc func(ctx context.Context, name string) (string, error)

	// ObjectNameFunc returns the name of an object or an error.
	ObjectNameFunc func(obj runtime.Object) (string, error)

	// PredicateFunc returns a matcher corresponding to the provided labels
	// and fields. The SelectionPredicate returned should return true if the
	// object matches the given field and label selectors.
	PredicateFunc func(label labels.Selector, field fields.Selector) storage.SelectionPredicate

	// EnableGarbageCollection affects the handling of Update and Delete
	// requests. Enabling garbage collection allows finalizers to do work to
	// finalize this object before the store deletes it.
	//
	// If any store has garbage collection enabled, it must also be enabled in
	// the kube-controller-manager.
	EnableGarbageCollection bool

	// DeleteCollectionWorkers is the maximum number of workers in a single
	// DeleteCollection call. Delete requests for the items in a collection
	// are issued in parallel.
	DeleteCollectionWorkers int

	// GetStrategy implements resource-specific behavior during get.
	GetStrategy reststore.GetStrategy

	// ListStrategy implements resource-specific behavior during list.
	ListStrategy reststore.ListStrategy

	// CreateStrategy implements resource-specific behavior during creation.
	CreateStrategy reststore.RESTCreateStrategy

	// UpdateStrategy implements resource-specific behavior during updates.
	UpdateStrategy reststore.RESTUpdateStrategy

	// DeleteStrategy implements resource-specific behavior during deletion.
	DeleteStrategy reststore.RESTDeleteStrategy

	WatchStrategy reststore.RESWatchStrategy

	// ReturnDeletedObject determines whether the Store returns the object
	// that was deleted. Otherwise, return a generic success status response.
	ReturnDeletedObject bool

	// TableConvertor is an optional interface for transforming items or lists
	// of items into tabular output. If unset, the default will be used.
	TableConvertor rest.TableConvertor

	// DestroyFunc cleans up clients used by the underlying Storage; optional.
	// If set, DestroyFunc has to be implemented in thread-safe way and
	// be prepared for being called more than once.
	DestroyFunc func()
}

// CompleteWithOptions updates the store with the provided options and
// defaults common fields.
func (r *Store) CompleteWithOptions(options *generic.StoreOptions) error {
	if r.DefaultQualifiedResource.Empty() {
		return fmt.Errorf("store %#v must have a non-empty qualified resource", r)
	}
	if r.SingularQualifiedResource.Empty() {
		return fmt.Errorf("store %#v must have a non-empty singular qualified resource", r)
	}
	if r.DefaultQualifiedResource.Group != r.SingularQualifiedResource.Group {
		return fmt.Errorf("store for %#v, singular and plural qualified resource's group name's must match", r)
	}
	if r.NewFunc == nil {
		return fmt.Errorf("store for %s must have NewFunc set", r.DefaultQualifiedResource.String())
	}
	if r.NewListFunc == nil {
		return fmt.Errorf("store for %s must have NewListFunc set", r.DefaultQualifiedResource.String())
	}
	if r.TableConvertor == nil {
		return fmt.Errorf("store for %s must set TableConvertor; rest.NewDefaultTableConvertor(e.DefaultQualifiedResource) can be used to output just name/creation time", r.DefaultQualifiedResource.String())
	}
	if r.Tracer == nil {
		r.Tracer = otel.Tracer(r.DefaultQualifiedResource.Resource)
	}
	var isNamespaced bool
	switch {
	case r.CreateStrategy != nil:
		isNamespaced = r.CreateStrategy.NamespaceScoped()
	case r.UpdateStrategy != nil:
		isNamespaced = r.UpdateStrategy.NamespaceScoped()
	default:
		return fmt.Errorf("store for %s must have CreateStrategy or UpdateStrategy set", r.DefaultQualifiedResource.String())
	}

	if r.CreateStrategy == nil {
		return fmt.Errorf("store for %s must have CreateStrategy set", r.DefaultQualifiedResource.String())
	}
	if r.UpdateStrategy == nil {
		return fmt.Errorf("store for %s must have UpdateStrategy set", r.DefaultQualifiedResource.String())
	}
	if r.DeleteStrategy == nil {
		return fmt.Errorf("store for %s must have DeleteStrategy set", r.DefaultQualifiedResource.String())
	}
	if r.GetStrategy == nil {
		return fmt.Errorf("store for %s must have GetStrategy set", r.DefaultQualifiedResource.String())
	}
	if r.ListStrategy == nil {
		return fmt.Errorf("store for %s must have ListStrategy set", r.DefaultQualifiedResource.String())
	}

	if options.RESTOptions == nil {
		return fmt.Errorf("options for %s must have RESTOptions set", r.DefaultQualifiedResource.String())
	}
	attrFunc := options.AttrFunc
	if attrFunc == nil {
		if isNamespaced {
			attrFunc = storage.DefaultNamespaceScopedAttr
		} else {
			attrFunc = storage.DefaultClusterScopedAttr
		}
	}
	if r.PredicateFunc == nil {
		r.PredicateFunc = func(label labels.Selector, field fields.Selector) storage.SelectionPredicate {
			return storage.SelectionPredicate{
				Label:    label,
				Field:    field,
				GetAttrs: attrFunc,
			}
		}
	}

	opts, err := options.RESTOptions.GetRESTOptions(r.DefaultQualifiedResource)
	if err != nil {
		return err
	}
	// ResourcePrefix must come from the underlying factory
	prefix := opts.ResourcePrefix
	if !strings.HasPrefix(prefix, "/") {
		prefix = "/" + prefix
	}
	if prefix == "/" {
		return fmt.Errorf("store for %s has an invalid prefix %q", r.DefaultQualifiedResource.String(), opts.ResourcePrefix)
	}

	// Set the default behavior for storage key generation
	if r.KeyRootFunc == nil && r.KeyFunc == nil {
		if isNamespaced {
			r.KeyRootFunc = func(ctx context.Context) string {
				return NamespaceKeyRootFunc(ctx, prefix)
			}
			r.KeyFunc = func(ctx context.Context, name string) (string, error) {
				return NamespaceKeyFunc(ctx, prefix, name)
			}
		} else {
			r.KeyRootFunc = func(ctx context.Context) string {
				return prefix
			}
			r.KeyFunc = func(ctx context.Context, name string) (string, error) {
				return NoNamespaceKeyFunc(ctx, prefix, name)
			}
		}
	}

	if r.ObjectNameFunc == nil {
		r.ObjectNameFunc = func(obj runtime.Object) (string, error) {
			accessor, err := meta.Accessor(obj)
			if err != nil {
				return "", err
			}
			return accessor.GetName(), nil
		}
	}

	// TODO store

	return nil
}

// New implements RESTStorage.New.
func (r *Store) New() runtime.Object {
	return r.NewFunc()
}

// Destroy cleans up its resources on shutdown.
func (r *Store) Destroy() {
	if r.DestroyFunc != nil {
		r.DestroyFunc()
	}
}

// NewList implements rest.Lister.
func (r *Store) NewList() runtime.Object {
	return r.NewListFunc()
}

// NamespaceScoped indicates whether the resource is namespaced
func (r *Store) NamespaceScoped() bool {
	if r.CreateStrategy != nil {
		return r.CreateStrategy.NamespaceScoped()
	}
	if r.UpdateStrategy != nil {
		return r.UpdateStrategy.NamespaceScoped()
	}

	panic("programmer error: no CRUD for resource, override NamespaceScoped too")
}

func (r *Store) GetSingularName() string {
	return r.SingularQualifiedResource.Resource
}

func (r *Store) ConvertToTable(ctx context.Context, object runtime.Object, tableOptions runtime.Object) (*metav1.Table, error) {
	if r.TableConvertor != nil {
		return r.TableConvertor.ConvertToTable(ctx, object, tableOptions)
	}
	return rest.NewDefaultTableConvertor(r.DefaultQualifiedResource).ConvertToTable(ctx, object, tableOptions)
}

func (r *Store) Get(ctx context.Context, name string, options *metav1.GetOptions) (runtime.Object, error) {
	ctx, span := r.Tracer.Start(ctx, fmt.Sprintf("%s:get", r.DefaultQualifiedResource.Resource), trace.WithAttributes())
	defer span.End()

	log := log.FromContext(ctx).With("name", name)
	log.Info("get")

	key, err := r.KeyFunc(ctx, name)
	if err != nil {
		return nil, err
	}

	return r.GetStrategy.Get(ctx, key)
}

// List returns a list of items matching labels and field according to the
// store's PredicateFunc.
func (r *Store) List(ctx context.Context, options *metainternalversion.ListOptions) (runtime.Object, error) {
	ctx, span := r.Tracer.Start(ctx, fmt.Sprintf("%s:list", r.DefaultQualifiedResource.Resource), trace.WithAttributes())
	defer span.End()

	log := log.FromContext(ctx)
	log.Info("list")

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

func (r *Store) Create(ctx context.Context, obj runtime.Object, createValidation rest.ValidateObjectFunc, options *metav1.CreateOptions) (runtime.Object, error) {
	ctx, span := r.Tracer.Start(ctx, fmt.Sprintf("%s:create", r.DefaultQualifiedResource.Resource), trace.WithAttributes())
	defer span.End()

	log := log.FromContext(ctx)
	log.Info("create")

	var finishCreate reststore.FinishFunc = reststore.FinishNothing

	if objectMeta, err := meta.Accessor(obj); err != nil {
		return nil, err
	} else {
		// set createTimestamp and set UUID
		rest.FillObjectMetaSystemFields(objectMeta)
		if len(objectMeta.GetGenerateName()) > 0 && len(objectMeta.GetName()) == 0 {
			objectMeta.SetName(r.CreateStrategy.GenerateName(objectMeta.GetGenerateName()))
		}
	}

	fn, err := r.CreateStrategy.BeginCreate(ctx, obj, options)
	if err != nil {
		return nil, err
	}
	finishCreate = fn
	defer func() {
		finishCreate(ctx, false)
	}()

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
	if err := r.CreateStrategy.Create(ctx, key, obj); err != nil {
		// TODO see if we need to return more errors
		return nil, apierrors.NewInternalError(err)
	}

	// The operation has succeeded.  Call the finish function if there is one,
	// and then make sure the defer doesn't call it again.
	fn = finishCreate
	finishCreate = reststore.FinishNothing
	fn(ctx, true)

	r.CreateStrategy.AfterCreate(obj, options)

	return obj, nil
}

// Update performs an atomic update and set of the object. Returns the result of the update
// or an error. If the registry allows create-on-update, the create flow will be executed.
// A bool is returned along with the object and any errors, to indicate object creation.
func (r *Store) Update(ctx context.Context, name string, objInfo rest.UpdatedObjectInfo, createValidation rest.ValidateObjectFunc, updateValidation rest.ValidateObjectUpdateFunc, forceAllowCreate bool, options *metav1.UpdateOptions) (runtime.Object, bool, error) {
	ctx, span := r.Tracer.Start(ctx, fmt.Sprintf("%s:update", r.DefaultQualifiedResource.Resource), trace.WithAttributes())
	defer span.End()

	log := log.FromContext(ctx)
	log.Info("update")

	var finishUpdate reststore.FinishFunc = reststore.FinishNothing

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

	fn, err := r.UpdateStrategy.BeginUpdate(ctx, obj, existing, options)
	if err != nil {
		return nil, creating, err
	}
	finishUpdate = fn
	defer func() {
		finishUpdate(ctx, false)
	}()

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

	// The operation has succeeded.  Call the finish function if there is one,
	// and then make sure the defer doesn't call it again.
	fn = finishUpdate
	finishUpdate = reststore.FinishNothing
	fn(ctx, true)

	if creating {
		r.CreateStrategy.AfterCreate(obj, newCreateOptionsFromUpdateOptions(options))

	} else {
		r.UpdateStrategy.AfterUpdate(obj, options)

	}

	return obj, creating, nil
}

// Delete removes the item from storage.
// options can be mutated by rest.BeforeDelete due to a graceful deletion strategy.
func (r *Store) Delete(ctx context.Context, name string, deleteValidation rest.ValidateObjectFunc, options *metav1.DeleteOptions) (runtime.Object, bool, error) {
	ctx, span := r.Tracer.Start(ctx, fmt.Sprintf("%s:delete", r.DefaultQualifiedResource.Resource), trace.WithAttributes())
	defer span.End()

	log := log.FromContext(ctx)
	log.Info("delete")

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
	_, pendingGraceful, err := rest.BeforeDelete(r.DeleteStrategy, ctx, obj, options)
	if err != nil {
		return nil, false, err
	}
	// this means finalizers cannot be updated via DeleteOptions if a deletion is already pending
	if pendingGraceful {
		out, err := r.finalizeDelete(ctx, obj, false, options)
		return out, false, err
	}
	// check if obj has pending finalizers
	// TODO finalizers

	if derr := r.DeleteStrategy.Delete(ctx, key); derr != nil {
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

// This is a helper to convert UpdateOptions to CreateOptions for the
// create-on-update path.
func newCreateOptionsFromUpdateOptions(in *metav1.UpdateOptions) *metav1.CreateOptions {
	co := &metav1.CreateOptions{
		DryRun:          in.DryRun,
		FieldManager:    in.FieldManager,
		FieldValidation: in.FieldValidation,
	}
	co.TypeMeta.SetGroupVersionKind(metav1.SchemeGroupVersion.WithKind("CreateOptions"))
	return co
}

// finalizeDelete runs the Store's AfterDelete hook if runHooks is set and
// returns the decorated deleted object if appropriate.
func (r *Store) finalizeDelete(ctx context.Context, obj runtime.Object, runHooks bool, options *metav1.DeleteOptions) (runtime.Object, error) {
	if runHooks {
		r.DeleteStrategy.AfterDelete(obj, options)
	}
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

/*
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
*/

/*
var (
	errAlreadyDeleting   = fmt.Errorf("abort delete")
	errDeleteNow         = fmt.Errorf("delete now")
	errEmptiedFinalizers = fmt.Errorf("emptied finalizers")
)
*/

/*
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
*/

/*
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
*/

// deleteCollectionPageSize is the size of the page used when
// listing objects from storage during DeleteCollection calls.
// It's a variable to make allow overwriting in tests.
var deleteCollectionPageSize = int64(10000)

// DeleteCollection removes all items returned by List with a given ListOptions from storage.
//
// DeleteCollection is currently NOT atomic. It can happen that only subset of objects
// will be deleted from storage, and then an error will be returned.
// In case of success, the list of deleted objects will be returned.
func (r *Store) DeleteCollection(ctx context.Context, deleteValidation rest.ValidateObjectFunc, options *metav1.DeleteOptions, listOptions *metainternalversion.ListOptions) (runtime.Object, error) {
	if listOptions == nil {
		listOptions = &metainternalversion.ListOptions{}
	} else {
		listOptions = listOptions.DeepCopy()
	}

	var items []runtime.Object

	// TODO(wojtek-t): Decide if we don't want to start workers more opportunistically.
	workersNumber := r.DeleteCollectionWorkers
	if workersNumber < 1 {
		workersNumber = 1
	}
	wg := sync.WaitGroup{}
	// Ensure that chanSize is not too high (to avoid wasted work) but
	// at the same time high enough to start listing before we process
	// the whole page.
	chanSize := 2 * workersNumber
	if chanSize < 256 {
		chanSize = 256
	}
	toProcess := make(chan runtime.Object, chanSize)
	errs := make(chan error, workersNumber+1)
	workersExited := make(chan struct{})

	wg.Add(workersNumber)
	for i := 0; i < workersNumber; i++ {
		go func() {
			// panics don't cross goroutine boundaries
			defer utilruntime.HandleCrash(func(panicReason interface{}) {
				errs <- fmt.Errorf("DeleteCollection goroutine panicked: %v", panicReason)
			})
			defer wg.Done()

			for item := range toProcess {
				accessor, err := meta.Accessor(item)
				if err != nil {
					errs <- err
					return
				}
				// DeepCopy the deletion options because individual graceful deleters communicate changes via a mutating
				// function in the delete strategy called in the delete method.  While that is always ugly, it works
				// when making a single call.  When making multiple calls via delete collection, the mutation applied to
				// pod/A can change the option ultimately used for pod/B.
				if _, _, err := r.Delete(ctx, accessor.GetName(), deleteValidation, options.DeepCopy()); err != nil && !apierrors.IsNotFound(err) {
					klog.V(4).InfoS("Delete object in DeleteCollection failed", "object", klog.KObj(accessor), "err", err)
					errs <- err
					return
				}
			}
		}()
	}
	// In case of all workers exit, notify distributor.
	go func() {
		defer utilruntime.HandleCrash(func(panicReason interface{}) {
			errs <- fmt.Errorf("DeleteCollection workers closer panicked: %v", panicReason)
		})
		wg.Wait()
		close(workersExited)
	}()

	hasLimit := listOptions.Limit > 0
	if listOptions.Limit == 0 {
		listOptions.Limit = deleteCollectionPageSize
	}

	// Paginate the list request and throw all items into workers.
	listObj, err := func() (runtime.Object, error) {
		defer close(toProcess)

		processedItems := 0
		var originalList runtime.Object
		for {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			default:
			}

			listObj, err := r.List(ctx, listOptions)
			if err != nil {
				return nil, err
			}

			newItems, err := meta.ExtractList(listObj)
			if err != nil {
				return nil, err
			}
			items = append(items, newItems...)

			for i := 0; i < len(newItems); i++ {
				select {
				case toProcess <- newItems[i]:
				case <-workersExited:
					klog.V(4).InfoS("workers already exited, and there are some items waiting to be processed", "queued/finished", i, "total", processedItems+len(newItems))
					// Try to propagate an error from the workers if possible.
					select {
					case err := <-errs:
						return nil, err
					default:
						return nil, fmt.Errorf("all DeleteCollection workers exited")
					}
				}
			}
			processedItems += len(newItems)

			// If the original request was setting the limit, finish after running it.
			if hasLimit {
				return listObj, nil
			}

			if originalList == nil {
				originalList = listObj
				meta.SetList(originalList, nil)
			}

			// If there are no more items, return the list.
			m, err := meta.ListAccessor(listObj)
			if err != nil {
				return nil, err
			}
			if len(m.GetContinue()) == 0 {
				meta.SetList(originalList, items)
				return originalList, nil
			}

			// Set up the next loop.
			listOptions.Continue = m.GetContinue()
			listOptions.ResourceVersion = ""
			listOptions.ResourceVersionMatch = ""
		}
	}()
	if err != nil {
		return nil, err
	}

	// Wait for all workers to exit.
	<-workersExited

	select {
	case err := <-errs:
		return nil, err
	default:
		return listObj, nil
	}
}
