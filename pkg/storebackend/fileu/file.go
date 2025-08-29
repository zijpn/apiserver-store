/*
Copyright 2024 Nokia.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package fileu

import (
	"context"
	"fmt"

	"github.com/henderiw/apiserver-store/pkg/storebackend"
	"github.com/henderiw/logger/log"
	"k8s.io/apimachinery/pkg/runtime"
)

const (
	// errors
	NotFound = "not found"
)

func NewStore(cfg *storebackend.Config) (storebackend.UnstructuredStorer, error) {
	if err := ensureDir(cfg.Prefix); err != nil {
		return nil, fmt.Errorf("unable to write data dir: %s", err)
	}
	return &file{
		grPrefix:    fmt.Sprintf("%s_%s", cfg.GroupResource.Group, cfg.GroupResource.Resource),
		group:       cfg.GroupResource.Group,
		resource:    cfg.GroupResource.Resource,
		objRootPath: cfg.Prefix,
		codec:       cfg.Codec,
		newFunc:     cfg.NewFunc,
	}, nil
}

type file struct {
	grPrefix    string
	group       string
	resource    string
	objRootPath string
	codec       runtime.Codec
	newFunc     func() runtime.Object
}

// Get return the type
func (r *file) Get(ctx context.Context, key storebackend.Key) (runtime.Unstructured, error) {
	return r.readFile(ctx, key)
}

func (r *file) List(ctx context.Context, visitorFunc func(ctx context.Context, key storebackend.Key, obj runtime.Unstructured)) error {
	log := log.FromContext(ctx)

	if err := r.visitDir(ctx, visitorFunc); err != nil {
		log.Error("cannot list visiting dir failed", "error", err.Error())
		return err
	}
	return nil
}

func (r *file) UpdateWithFn(ctx context.Context, updateFunc func(ctx context.Context, key storebackend.Key, obj runtime.Unstructured) runtime.Unstructured) error {
	// not implemented
	return nil
}

func (r *file) UpdateWithKeyFn(ctx context.Context, key storebackend.Key, updateFunc func(ctx context.Context, obj runtime.Unstructured) runtime.Unstructured) error {
	obj, err := r.readFile(ctx, key)
	if err != nil {
		return err
	}
	if updateFunc != nil {
		obj = updateFunc(ctx, obj)
		return r.update(ctx, key, obj)
	}
	return nil
}

func (r *file) Create(ctx context.Context, key storebackend.Key, data runtime.Unstructured) error {
	// if an error is returned the entry already exists
	if _, err := r.Get(ctx, key); err == nil {
		return fmt.Errorf("duplicate entry %v", key.String())
	}
	// update the store before calling the callback since the cb fn will use this data
	if err := r.update(ctx, key, data); err != nil {
		return err
	}

	// TODO notify watchers
	return nil
}

// Upsert creates or updates the entry in the cache
func (r *file) Update(ctx context.Context, key storebackend.Key, data runtime.Unstructured) error {
	// update the cache before calling the callback since the cb fn will use this data
	if err := r.update(ctx, key, data); err != nil {
		return err
	}

	// // notify watchers based on the fact the data got modified or not
	/*
		if exists {
			if !reflect.DeepEqual(oldd, data) {
				// TODO watchers
			}
		} else {
			// TODO watchers
		}
	*/
	return nil
}

func (r *file) update(ctx context.Context, key storebackend.Key, newd runtime.Unstructured) error {
	return r.writeFile(ctx, key, newd)
}

func (r *file) delete(ctx context.Context, key storebackend.Key) error {
	return r.deleteFile(ctx, key)
}

// Delete deletes the entry in the cache
func (r *file) Delete(ctx context.Context, key storebackend.Key) error {
	// only if an exisitng object gets deleted we
	// call the registered callbacks
	//exists := true
	if _, err := r.Get(ctx, key); err != nil {
		return nil
	}
	// if exists call the callback
	//if exists {
	// TODO watchers
	//}
	// delete the entry to ensure the cb uses the proper data
	return r.delete(ctx, key)
}

/*
func (r *file[T1]) Watch(ctx context.Context) (watch.Interface[T1], error) {
	// lock is not required here
	log := log.FromContext(ctx)
	log.Info("watch file store")
	if r.watchers.IsExhausted() {
		return nil, fmt.Errorf("cannot allocate watcher, out of resources")
	}
	w := r.watchers.GetWatchContext()

	// On initial watch, send all the existing objects
	items := map[store.Key]T1{}
	r.List(ctx, func(ctx context.Context, key store.Key, obj T1) {
		items[key] = obj
	})
	log.Info("watch list items", "len", len(items))
	for _, obj := range items {
		w.ResultCh <- watch.Event[T1]{
			Type:   watch.Added,
			Object: obj,
		}
	}
	// this ensures the initial events from the list
	// get processed first
	log.Info("watcher add")
	if err := r.watchers.Add(w); err != nil {
		log.Info("cannot add watcher", "error", err.Error())
		return nil, err
	}
	log.Info("watcher added")
	return w, nil
}
*/
