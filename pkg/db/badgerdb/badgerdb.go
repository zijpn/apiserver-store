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
	"context"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/badger/v4/options"
)

func OpenDB(ctx context.Context, path string) (*badger.DB, error) {
	opts := badger.DefaultOptions(path).
		WithLoggingLevel(badger.WARNING).
		WithCompression(options.None).
		WithBlockCacheSize(0)

	bdb, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}
	go func() {
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			again:
				err = bdb.RunValueLogGC(0.7)
				if err == nil {
					goto again
				}
			}
		}
	}()
	return bdb, nil
}
