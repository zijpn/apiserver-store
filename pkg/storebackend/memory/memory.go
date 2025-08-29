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

package memory

import (
	"context"
	"fmt"
	"sync"

	"github.com/henderiw/apiserver-store/pkg/storebackend"
)

const (
	// errors
	NotFound = "not found"
)

func NewStore[T1 any]() storebackend.Storer[T1] {
	return &mem[T1]{
		db: map[storebackend.Key]T1{},
	}
}

type mem[T1 any] struct {
	m        sync.RWMutex
	db       map[storebackend.Key]T1
	keyLocks sync.Map
}

// Get return the type
func (r *mem[T1]) Get(ctx context.Context, key storebackend.Key) (T1, error) {
	r.m.RLock()
	defer r.m.RUnlock()

	x, ok := r.db[key]
	if !ok {
		return *new(T1), fmt.Errorf("%s, nsn: %s", NotFound, key.String())
	}
	return x, nil
}

func (r *mem[T1]) List(ctx context.Context, visitorFunc func(ctx context.Context, key storebackend.Key, obj T1)) error {
	r.m.RLock()
	defer r.m.RUnlock()

	for key, obj := range r.db {
		if visitorFunc != nil {
			visitorFunc(ctx, key, obj)
		}
	}
	return nil
}

func (r *mem[T1]) UpdateWithFn(ctx context.Context, updateFunc func(ctx context.Context, key storebackend.Key, obj T1) T1) error {
	r.m.Lock()
	defer r.m.Unlock()

	for key, obj := range r.db {
		if updateFunc != nil {
			r.db[key] = updateFunc(ctx, key, obj)
		}
	}
	return nil
}

func (r *mem[T1]) UpdateWithKeyFn(ctx context.Context, key storebackend.Key, updateFunc func(ctx context.Context, obj T1) T1) error {
	// Get or create a per-key mutex
	lockIface, _ := r.keyLocks.LoadOrStore(key, &sync.Mutex{})
	lock := lockIface.(*sync.Mutex)

	lock.Lock()
	defer lock.Unlock()

	r.m.Lock()
	defer r.m.Unlock()

	obj, ok := r.db[key]
	if !ok {
		return fmt.Errorf("key not found: %s", key.String())
	}
	if updateFunc != nil {
		r.db[key] = updateFunc(ctx, obj)
	}
	return nil
}

func (r *mem[T1]) Create(ctx context.Context, key storebackend.Key, data T1) error {
	// if an error is returned the entry already exists
	if _, err := r.Get(ctx, key); err == nil {
		return fmt.Errorf("duplicate entry %v", key.String())
	}
	// update the cache before calling the callback since the cb fn will use this data
	r.update(ctx, key, data)
	// notify watchers
	return nil
}

// Apply data with the given key in the storage
func (r *mem[T1]) Apply(ctx context.Context, key storebackend.Key, obj T1) error {
	r.update(ctx, key, obj)
	return nil
}

// Update creates or updates the entry in the cache
func (r *mem[T1]) Update(ctx context.Context, key storebackend.Key, data T1) error {

	// update the cache before calling the callback since the cb fn will use this data
	r.update(ctx, key, data)
	r.keyLocks.LoadOrStore(key, &sync.Mutex{})
	return nil
}

func (r *mem[T1]) update(_ context.Context, key storebackend.Key, newd T1) {
	r.m.Lock()
	defer r.m.Unlock()
	r.db[key] = newd
}

func (r *mem[T1]) delete(_ context.Context, key storebackend.Key) {
	r.m.Lock()
	defer r.m.Unlock()
	delete(r.db, key)
	r.keyLocks.Delete(key)
}

// Delete deletes the entry in the cache
func (r *mem[T1]) Delete(ctx context.Context, key storebackend.Key) error {
	// only if an exisitng object gets deleted we
	// call the registered callbacks
	//exists := true
	if _, err := r.Get(ctx, key); err != nil {
		return nil
	}
	// if exists call the callback
	/*
		if exists {
			// notify watchers
		}
	*/
	// delete the entry to ensure the cb uses the proper data
	r.delete(ctx, key)
	return nil
}

/*
func (r *mem[T1]) Watch(ctx context.Context) (watch.Interface[T1], error) {
	//r.m.Lock()
	//defer r.m.Unlock()

	log := log.FromContext(ctx)
	log.Info("watch memory store")
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
