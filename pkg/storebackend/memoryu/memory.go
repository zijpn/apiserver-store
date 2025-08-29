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

package memoryu

import (
	"context"
	"fmt"
	"sync"

	"github.com/zijpn/apiserver-store/pkg/storebackend"
	"k8s.io/apimachinery/pkg/runtime"
)

const (
	// errors
	NotFound = "not found"
)

func NewStore() storebackend.UnstructuredStorer {
	return &mem{
		db: map[storebackend.Key]runtime.Unstructured{},
	}
}

type mem struct {
	m  sync.RWMutex
	db map[storebackend.Key]runtime.Unstructured
}

// Get return the type
func (r *mem) Get(ctx context.Context, key storebackend.Key) (runtime.Unstructured, error) {
	r.m.RLock()
	defer r.m.RUnlock()

	x, ok := r.db[key]
	if !ok {
		return nil, fmt.Errorf("%s, nsn: %s", NotFound, key.String())
	}
	return x, nil
}

func (r *mem) List(ctx context.Context, visitorFunc func(ctx context.Context, key storebackend.Key, obj runtime.Unstructured)) error {
	r.m.RLock()
	defer r.m.RUnlock()

	for key, obj := range r.db {
		if visitorFunc != nil {
			visitorFunc(ctx, key, obj)
		}
	}
	return nil
}

func (r *mem) UpdateWithFn(ctx context.Context, updateFunc func(ctx context.Context, key storebackend.Key, obj runtime.Unstructured) runtime.Unstructured) error {
	r.m.Lock()
	defer r.m.Unlock()

	for key, obj := range r.db {
		if updateFunc != nil {
			r.db[key] = updateFunc(ctx, key, obj)
		}
	}
	return nil
}

func (r *mem) UpdateWithKeyFn(ctx context.Context, key storebackend.Key, updateFunc func(ctx context.Context, obj runtime.Unstructured) runtime.Unstructured) error {
	r.m.Lock()
	defer r.m.Unlock()

	obj := r.db[key]
	if updateFunc != nil {
		r.db[key] = updateFunc(ctx, obj)
	}
	return nil
}

func (r *mem) Create(ctx context.Context, key storebackend.Key, data runtime.Unstructured) error {
	// if an error is returned the entry already exists
	if _, err := r.Get(ctx, key); err == nil {
		return fmt.Errorf("duplicate entry %v", key.String())
	}
	// update the cache before calling the callback since the cb fn will use this data
	r.update(ctx, key, data)

	// notify watchers
	return nil
}

// Update creates or updates the entry in the cache
func (r *mem) Update(ctx context.Context, key storebackend.Key, data runtime.Unstructured) error {
	// update the cache before calling the callback since the cb fn will use this data
	r.update(ctx, key, data)
	return nil
}

func (r *mem) update(_ context.Context, key storebackend.Key, newd runtime.Unstructured) {
	r.m.Lock()
	defer r.m.Unlock()
	r.db[key] = newd
}

func (r *mem) delete(_ context.Context, key storebackend.Key) {
	r.m.Lock()
	defer r.m.Unlock()
	delete(r.db, key)
}

// Delete deletes the entry in the cache
func (r *mem) Delete(ctx context.Context, key storebackend.Key) error {
	// only if an exisitng object gets deleted we
	// call the registered callbacks
	if _, err := r.Get(ctx, key); err != nil {
		return nil
	}
	// delete the entry to ensure the cb uses the proper data
	r.delete(ctx, key)
	return nil
}
