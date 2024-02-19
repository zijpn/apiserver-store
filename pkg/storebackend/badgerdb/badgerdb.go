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

package badgerdb

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"

	"github.com/dgraph-io/badger/v4"
	"github.com/henderiw/apiserver-store/pkg/storebackend"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
)

const (
	dummyNamespace = "__"
)

var (
	dummyNamespaceBytes = []byte(dummyNamespace)
	separator           = []byte("/")
)

func NewStore[T1 any](db *badger.DB, cfg *storebackend.Config[T1]) (storebackend.Storer[T1], error) {

	r := &badgerDB[T1]{
		cfg:       cfg,
		db:        db,
		prefixKey: createKeyPrefix(cfg.GroupResource),
	}
	return r, nil
}

type badgerDB[T1 any] struct {
	prefixKey []byte
	db        *badger.DB
	cfg       *storebackend.Config[T1]
}

// Retrieve retrieves data for the given key from the storage
func (r *badgerDB[T1]) Get(ctx context.Context, key storebackend.Key) (T1, error) {
	k := r.createKey(key)

	var b []byte
	var obj T1
	var ok bool
	if err := r.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(k)
		if err != nil {
			return err
		}
		b, err = item.ValueCopy(nil)
		return err
	}); err != nil {
		return obj, err
	}
	newObj := r.cfg.NewFunc()
	decodeObj, _, err := r.cfg.Codec.Decode(b, nil, newObj)
	if err != nil {
		return obj, err
	}
	obj, ok = decodeObj.(T1)
	if !ok {
		return obj, fmt.Errorf("unexpected object, got: %s", reflect.TypeOf(decodeObj).Name())
	}
	return obj, nil
}

// Retrieve retrieves data for the given key from the storage
func (r *badgerDB[T1]) List(ctx context.Context, visitorFunc func(context.Context, storebackend.Key, T1)) error {
	return r.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Seek(r.prefixKey); it.ValidForPrefix(r.prefixKey); it.Next() {
			item := it.Item()
			k := item.KeyCopy(nil)
			key, err := r.decodeKey(k)
			if err != nil {
				return err
			}
			if err := item.Value(func(b []byte) error {
				newObj := r.cfg.NewFunc()
				decodeObj, _, err := r.cfg.Codec.Decode(b, nil, newObj)
				if err != nil {
					return err
				}
				obj, ok := decodeObj.(T1)
				if !ok {
					return fmt.Errorf("unexpected object, got: %s", reflect.TypeOf(decodeObj).Name())
				}
				if visitorFunc != nil {
					visitorFunc(ctx, key, obj)
				}
				return nil
			}); err != nil {
				return err
			}
		}
		return nil
	})
}

// Create data with the given key in the storage
func (r *badgerDB[T1]) Create(ctx context.Context, key storebackend.Key, obj T1) error {
	k := r.createKey(key)

	if err := r.db.Update(func(txn *badger.Txn) error {
		_, err := txn.Get(k)
		if err == nil {
			return fmt.Errorf("AlreadyExists")
		}
		if !errors.Is(badger.ErrKeyNotFound, err) {
			return err
		}
		runtimeObj, err := convert(obj)
		if err != nil {
			return err
		}
		buf := new(bytes.Buffer)
		if err := r.cfg.Codec.Encode(runtimeObj, buf); err != nil {
			return err
		}
		return txn.Set(k, buf.Bytes())
	}); err != nil {
		return err
	}
	return nil
}

// Update data with the given key in the storage
func (r *badgerDB[T1]) Update(ctx context.Context, key storebackend.Key, obj T1) error {
	k := r.createKey(key)
	runtimeObj, err := convert(obj)
	if err != nil {
		return err
	}

	buf := new(bytes.Buffer)
	if err := r.cfg.Codec.Encode(runtimeObj, buf); err != nil {
		return err
	}

	if err := r.db.Update(func(txn *badger.Txn) error {
		return txn.Set(k, buf.Bytes())
	}); err != nil {
		return err
	}
	return nil
}

// Update data in a concurrent way through a function
func (r *badgerDB[T1]) UpdateWithFn(ctx context.Context, updateFunc func(ctx context.Context, key storebackend.Key, obj T1) T1) error {
	return r.db.Update(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Seek(r.prefixKey); it.ValidForPrefix(r.prefixKey); it.Next() {
			item := it.Item()
			k := item.KeyCopy(nil)
			key, err := r.decodeKey(k)
			if err != nil {
				return err
			}
			if err := item.Value(func(b []byte) error {
				newObj := r.cfg.NewFunc()
				decodeObj, _, err := r.cfg.Codec.Decode(b, nil, newObj)
				if err != nil {
					return err
				}
				obj, ok := decodeObj.(T1)
				if !ok {
					return fmt.Errorf("unexpected object, got: %s", reflect.TypeOf(decodeObj).Name())
				}
				if updateFunc != nil {
					newObj := updateFunc(ctx, key, obj)
					runtimeObj, err := convert(newObj)
					if err != nil {
						return err
					}

					buf := new(bytes.Buffer)
					if err := r.cfg.Codec.Encode(runtimeObj, buf); err != nil {
						return err
					}

					if err := txn.Set(k, buf.Bytes()); err != nil {
						return err
					}
				}
				return nil
			}); err != nil {
				return err
			}
		}
		return nil
	})
}

// Update data in a concurrent way through a function
func (r *badgerDB[T1]) UpdateWithKeyFn(ctx context.Context, key storebackend.Key, updateFunc func(ctx context.Context, obj T1) T1) error {

	return r.db.Update(func(txn *badger.Txn) error {
		k := r.createKey(key)

		item, err := txn.Get(k)
		if err != nil {
			return err
		}
		b, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		newObj := r.cfg.NewFunc()
		decodeObj, _, err := r.cfg.Codec.Decode(b, nil, newObj)
		if err != nil {
			return err
		}
		obj, ok := decodeObj.(T1)
		if !ok {
			return fmt.Errorf("unexpected object, got: %s", reflect.TypeOf(decodeObj).Name())
		}

		if updateFunc != nil {
			newObj := updateFunc(ctx, obj)
			runtimeObj, err := convert(newObj)
			if err != nil {
				return err
			}

			buf := new(bytes.Buffer)
			if err := r.cfg.Codec.Encode(runtimeObj, buf); err != nil {
				return err
			}

			if err := txn.Set(k, buf.Bytes()); err != nil {
				return err
			}
		}

		return nil
	})

}

// Delete deletes data and key from the storage
func (r *badgerDB[T1]) Delete(ctx context.Context, key storebackend.Key) error {
	return r.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(r.createKey(key))
	})
}

func (r *badgerDB[T1]) createKey(key storebackend.Key) []byte {
	if key.Namespace == "" {
		return []byte(fmt.Sprintf("%s/%s/%s/%s", r.cfg.GroupResource.Group, r.cfg.GroupResource.Resource, dummyNamespace, key.Name))
	}
	return []byte(fmt.Sprintf("%s/%s/%s/%s", r.cfg.GroupResource.Group, r.cfg.GroupResource.Resource, key.Namespace, key.Name))
}

func convert(obj any) (runtime.Object, error) {
	runtimeObj, ok := obj.(runtime.Object)
	if !ok {
		return nil, fmt.Errorf("unsupported type: %v", reflect.TypeOf(obj))
	}
	return runtimeObj, nil
}

func createKeyPrefix(gr schema.GroupResource) []byte {
	return []byte(fmt.Sprintf("%s/%s/", gr.Group, gr.Resource))
}

func (r *badgerDB[T1]) decodeKey(k []byte) (storebackend.Key, error) {
	b := bytes.TrimPrefix(k, r.prefixKey)
	parts := bytes.SplitN(b, separator, 2)
	if len(parts) != 2 {
		return storebackend.Key{}, fmt.Errorf("malformed key, got: %s", string(b))
	}
	if bytes.Equal(parts[0], dummyNamespaceBytes) {
		return storebackend.ToKey(string(parts[1])), nil
	}
	return storebackend.Key{
		NamespacedName: types.NamespacedName{Namespace: string(parts[0]), Name: string(parts[1])},
	}, nil
}
