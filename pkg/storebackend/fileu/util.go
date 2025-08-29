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
	"os"
	"path/filepath"
	"strings"

	"github.com/zijpn/apiserver-store/pkg/storebackend"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/yaml"
)

func (r *file) filename(key storebackend.Key) string {
	if key.Namespace != "" {
		return fmt.Sprintf("%s_%s_%s_%s.yaml", r.objRootPath, r.grPrefix, key.Namespace, key.Name)
	}
	return fmt.Sprintf("%s_%s_%s_%s.yaml", r.objRootPath, r.grPrefix, r.resource, key.Name)
}

func (r *file) readFile(_ context.Context, key storebackend.Key) (runtime.Unstructured, error) {
	//log := log.FromContext(ctx)
	var obj runtime.Unstructured
	content, err := os.ReadFile(filepath.Clean(r.filename(key)))
	if err != nil {
		return obj, err
	}
	object := map[string]any{}
	if err := yaml.Unmarshal(content, &object); err != nil {
		return obj, err
	}
	return &unstructured.Unstructured{
		Object: object,
	}, nil
}

func (r *file) writeFile(_ context.Context, key storebackend.Key, obj runtime.Unstructured) error {
	b, err := yaml.Marshal(obj)
	if err != nil {
		return err
	}
	if err := ensureDir(filepath.Dir(r.filename(key))); err != nil {
		return err
	}
	return os.WriteFile(r.filename(key), b, 0644)
}

func (r *file) deleteFile(_ context.Context, key storebackend.Key) error {
	return os.Remove(r.filename(key))
}

func (r *file) visitDir(ctx context.Context, visitorFunc func(ctx context.Context, key storebackend.Key, obj runtime.Unstructured)) error {
	return filepath.Walk(r.objRootPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		// skip any non yaml file
		if !strings.HasSuffix(info.Name(), ".yaml") {
			return nil
		}
		// skip if the group resource prefix does not match
		if !strings.HasPrefix(info.Name(), r.grPrefix) {
			return nil
		}
		// this is a yaml file by now
		// next step is find the key (namespace and name)
		name := filepath.Base(path)
		name = strings.TrimSuffix(name, ".yaml")
		name = strings.TrimPrefix(name, r.grPrefix)
		namespace := ""
		parts := strings.Split(name, "_")
		if len(parts) > 1 {
			namespace = parts[0]
			name = parts[1]
		}

		key := storebackend.KeyFromNSN(types.NamespacedName{
			Name:      name,
			Namespace: namespace,
		})

		newObj, err := r.readFile(ctx, key)
		if err != nil {
			return err
		}
		if visitorFunc != nil {
			visitorFunc(ctx, key, newObj)
		}

		return nil
	})
}

func exists(filepath string) bool {
	_, err := os.Stat(filepath)
	return err == nil
}

func ensureDir(dirname string) error {
	if !exists(dirname) {
		return os.MkdirAll(dirname, 0755)
	}
	return nil
}
