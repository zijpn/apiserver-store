package registry

import (
	"context"
	"fmt"

	reststore "github.com/henderiw/apiserver-store/pkg/rest"
	"github.com/henderiw/apiserver-store/pkg/storebackend"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apiserver/pkg/registry/generic"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/apiserver/pkg/storage"
	"sigs.k8s.io/structured-merge-diff/v6/fieldpath"
)

var _ rest.StandardStorage = &Store{}
var _ rest.TableConvertor = &Store{}
var _ rest.SingularNameProvider = &Store{}
var _ rest.ShortNamesProvider = &Store{}
var _ rest.CategoriesProvider = &Store{}
var _ rest.ResetFieldsStrategy = &Store{}

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

	// KeyFunc returns the key for a specific object in the collection.
	// KeyFunc is called for Create/Update/Get/Delete. Note that 'namespace'
	// can be gotten from ctx.
	KeyFunc func(ctx context.Context, name string) (types.NamespacedName, error)

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
	GetStrategy reststore.RESTGetStrategy

	// ListStrategy implements resource-specific behavior during list.
	ListStrategy reststore.RESTListStrategy

	// CreateStrategy implements resource-specific behavior during creation.
	CreateStrategy reststore.RESTCreateStrategy

	// UpdateStrategy implements resource-specific behavior during updates.
	UpdateStrategy reststore.RESTUpdateStrategy

	// DeleteStrategy implements resource-specific behavior during deletion.
	DeleteStrategy reststore.RESTDeleteStrategy

	WatchStrategy reststore.RESTWatchStrategy

	// ReturnDeletedObject determines whether the Store returns the object
	// that was deleted. Otherwise, return a generic success status response.
	ReturnDeletedObject bool

	// TableConvertor is an optional interface for transforming items or lists
	// of items into tabular output. If unset, the default will be used.
	TableConvertor rest.TableConvertor

	// ResetFieldsStrategy provides the fields reset by the strategy that
	// should not be modified by the user.
	ResetFieldsStrategy rest.ResetFieldsStrategy

	// DestroyFunc cleans up clients used by the underlying Storage; optional.
	// If set, DestroyFunc has to be implemented in thread-safe way and
	// be prepared for being called more than once.
	DestroyFunc func()

	ShortNameList []string

	CategoryList []string

	Storage storebackend.Storer[runtime.Object]
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

	/*
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
	*/

	// Set the default behavior for storage key generation
	if r.KeyFunc == nil {
		if isNamespaced {
			r.KeyFunc = func(ctx context.Context, name string) (types.NamespacedName, error) {
				return NamespaceKeyFunc(ctx, name)
			}
		} else {
			r.KeyFunc = func(ctx context.Context, name string) (types.NamespacedName, error) {
				return NoNamespaceKeyFunc(ctx, name)
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

func (r *Store) Categories() []string {
	return r.CategoryList
}

func (r *Store) ShortNames() []string {
	return r.ShortNameList
}

// GetResetFields implements rest.ResetFieldsStrategy
func (e *Store) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
	if e.ResetFieldsStrategy == nil {
		return nil
	}
	return e.ResetFieldsStrategy.GetResetFields()
}
