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

package storebackend

import (
	"context"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// Storer defines the interface for a generic storage system.
type Storer[T1 any] interface {
	// Retrieve retrieves data for the given key from the storage
	Get(ctx context.Context, key Key) (T1, error)

	// Retrieve retrieves data for the given key from the storage
	List(ctx context.Context, visitorFunc func(context.Context, Key, T1)) error

	// Create data with the given key in the storage
	Create(ctx context.Context, key Key, data T1) error

	// Apply data with the given key in the storage
	Apply(ctx context.Context, key Key, data T1) error

	// Update data with the given key in the storage
	Update(ctx context.Context, key Key, data T1) error

	// Update data in a concurrent way through a function
	UpdateWithFn(ctx context.Context, updateFunc func(ctx context.Context, key Key, obj T1) T1) error

	// Update data in a concurrent way through a function
	UpdateWithKeyFn(ctx context.Context, key Key, updateFunc func(ctx context.Context, obj T1) T1) error

	// Delete deletes data and key from the storage
	Delete(ctx context.Context, key Key) error

	// Watch watches change
	//Watch(ctx context.Context) (watch.Interface[T1], error)
}

type Config struct {
	GroupResource schema.GroupResource
	Prefix        string
	Codec         runtime.Codec
	NewFunc       func() runtime.Object
}


// Storer defines the interface for a generic storage system.
type UnstructuredStorer interface {
	// Retrieve retrieves data for the given key from the storage
	Get(ctx context.Context, key Key) (runtime.Unstructured, error)

	// Retrieve retrieves data for the given key from the storage
	List(ctx context.Context, visitorFunc func(context.Context, Key, runtime.Unstructured)) error

	// Create data with the given key in the storage
	Create(ctx context.Context, key Key, data runtime.Unstructured) error

	// Update data with the given key in the storage
	Update(ctx context.Context, key Key, data runtime.Unstructured) error

	// Update data in a concurrent way through a function
	UpdateWithFn(ctx context.Context, updateFunc func(ctx context.Context, key Key, obj runtime.Unstructured) runtime.Unstructured) error

	// Update data in a concurrent way through a function
	UpdateWithKeyFn(ctx context.Context, key Key, updateFunc func(ctx context.Context, obj runtime.Unstructured) runtime.Unstructured) error

	// Delete deletes data and key from the storage
	Delete(ctx context.Context, key Key) error

	// Watch watches change
	//Watch(ctx context.Context) (watch.Interface[T1], error)
}