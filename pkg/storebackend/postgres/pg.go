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

package postgres

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"

	"github.com/henderiw/apiserver-store/pkg/storebackend"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
)

func NewStore[T1 any](db *sql.DB, cfg *storebackend.Config) (storebackend.Storer[T1], error) {
	r := &PostgresDB[T1]{
		cfg:  cfg,
		pgdb: &pgdb{},
	}

	err := r.pgdb.Initialize(db, cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize database: %v", err)
	}

	return r, nil
}

type PostgresDB[T1 any] struct {
	cfg  *storebackend.Config
	pgdb *pgdb
}

// Retrieve retrieves data for the given key from the storage
func (r *PostgresDB[T1]) Get(ctx context.Context, key storebackend.Key) (T1, error) {
	var b []byte
	var obj T1
	var ok bool
	b, err := r.pgdb.retrieveData(key, nil) // transaction managed by procedures
	if err != nil {
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
func (r *PostgresDB[T1]) List(ctx context.Context, visitorFunc func(context.Context, storebackend.Key, T1)) error {
	rows, err := r.pgdb.retrieveDataList(nil) // transaction managed by procedures
	if err != nil {
		return fmt.Errorf("unable to fetch resource list %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var name string
		var namespace string
		var b []byte
		if err := rows.Scan(&namespace, &name, &b); err != nil {
			return fmt.Errorf("unable to fetch resource list, failed to scan row: %v", err)
		}

		key := storebackend.Key{
			NamespacedName: types.NamespacedName{
				Namespace: namespace,
				Name:      name,
			},
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
		if visitorFunc != nil {
			visitorFunc(ctx, key, obj)
		}

	}

	if err := rows.Err(); err != nil {
		return err
	}
	return nil
}

// Create data with the given key in the storage
func (r *PostgresDB[T1]) Create(ctx context.Context, key storebackend.Key, obj T1) error {
	runtimeObj, err := convert(obj)
	if err != nil {
		return err
	}
	buf := new(bytes.Buffer)
	if err := r.cfg.Codec.Encode(runtimeObj, buf); err != nil {
		return err
	}
	err = r.pgdb.insertEntry(key, buf.Bytes())
	if err != nil {
		if errors.Is(err, errUnique_key_voilation) {
			return fmt.Errorf("AlreadyExists")
		}
	}

	return nil
}

// Update data with the given key in the storage
func (r *PostgresDB[T1]) Update(ctx context.Context, key storebackend.Key, obj T1) error {
	runtimeObj, err := convert(obj)
	if err != nil {
		return err
	}

	buf := new(bytes.Buffer)
	if err := r.cfg.Codec.Encode(runtimeObj, buf); err != nil {
		return err
	}

	err = r.pgdb.updateOnConflict(key, buf.Bytes(), nil) // transaction managed by procedures
	if err != nil {
		return err
	}

	return nil
}

// Update data in a concurrent way through a function
func (r *PostgresDB[T1]) UpdateWithFn(ctx context.Context, updateFunc func(ctx context.Context, key storebackend.Key, obj T1) T1) error {
	tx, err := r.pgdb.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: false})
	if err != nil {
		return fmt.Errorf("error starting transaction. %v", err)
	}

	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()

	rows, err := r.pgdb.retrieveDataList(tx)
	if err != nil {
		return err
	}

	defer rows.Close()
	for rows.Next() {
		var name string
		var namespace string
		var b []byte
		if err := rows.Scan(&namespace, &name, &b); err != nil {
			return fmt.Errorf("unable to fetch resource list, failed to scan row: %v", err)
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
			key := storebackend.Key{
				NamespacedName: types.NamespacedName{
					Namespace: namespace,
					Name:      name,
				},
			}
			newObj := updateFunc(ctx, key, obj)
			runtimeObj, err := convert(newObj)
			if err != nil {
				return err
			}

			buf := new(bytes.Buffer)
			if err := r.cfg.Codec.Encode(runtimeObj, buf); err != nil {
				return err
			}

			err = r.pgdb.updateOnConflict(key, buf.Bytes(), tx)
			if err != nil {
				return err
			}
		}

	}

	if err := rows.Err(); err != nil {
		return err
	}

	err = tx.Commit()
	if err != nil {
		return err
	}

	return nil
}

// Update data in a concurrent way through a function
func (r *PostgresDB[T1]) UpdateWithKeyFn(ctx context.Context, key storebackend.Key, updateFunc func(ctx context.Context, obj T1) T1) error {
	tx, err := r.pgdb.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: false})
	if err != nil {
		return fmt.Errorf("error starting transaction. %v", err)
	}

	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()

	b, err := r.pgdb.retrieveData(key, tx)
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

		err = r.pgdb.updateOnConflict(key, buf.Bytes(), tx)
		if err != nil {
			return err
		}
	}

	err = tx.Commit()
	if err != nil {
		return err
	}

	return nil
}

// Delete deletes data and key from the storage
func (r *PostgresDB[T1]) Delete(ctx context.Context, key storebackend.Key) error {
	return r.pgdb.delete_entry(key)
}

func convert(obj any) (runtime.Object, error) {
	runtimeObj, ok := obj.(runtime.Object)
	if !ok {
		return nil, fmt.Errorf("unsupported type: %v", reflect.TypeOf(obj))
	}
	return runtimeObj, nil
}
