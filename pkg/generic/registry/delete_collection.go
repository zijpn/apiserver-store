package registry

import (
	"context"
	"fmt"
	"sync"

	"github.com/henderiw/logger/log"
	"go.opentelemetry.io/otel/trace"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apiserver/pkg/registry/rest"
)

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
	ctx, span := r.Tracer.Start(ctx, fmt.Sprintf("%s:deleteCollection", r.DefaultQualifiedResource.Resource), trace.WithAttributes())
	defer span.End()

	log := log.FromContext(ctx)
	log.Debug("deleteCollection")

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
					log.Info("Delete object in DeleteCollection failed", "object", accessor.GetName(), "err", err)
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
					log.Info("workers already exited, and there are some items waiting to be processed", "queued/finished", i, "total", processedItems+len(newItems))
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
