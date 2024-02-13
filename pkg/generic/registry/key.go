package registry

import (
	"context"
	"fmt"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/validation/path"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
)

// NamespaceKeyRootFunc is the default function for constructing storage paths
// to resource directories enforcing namespace rules.
func NamespaceKeyRootFunc(ctx context.Context, prefix string) string {
	key := prefix
	ns, ok := genericapirequest.NamespaceFrom(ctx)
	if ok && len(ns) > 0 {
		key = key + "/" + ns
	}
	return key
}

// NamespaceKeyFunc is the default function for constructing storage paths to
// a resource relative to the given prefix enforcing namespace rules. If the
// context does not contain a namespace, it errors.
func NamespaceKeyFunc(ctx context.Context, prefix string, name string) (string, error) {
	key := NamespaceKeyRootFunc(ctx, prefix)
	ns, ok := genericapirequest.NamespaceFrom(ctx)
	if !ok || len(ns) == 0 {
		return "", apierrors.NewBadRequest("Namespace parameter required.")
	}
	if len(name) == 0 {
		return "", apierrors.NewBadRequest("Name parameter required.")
	}
	if msgs := path.IsValidPathSegmentName(name); len(msgs) != 0 {
		return "", apierrors.NewBadRequest(fmt.Sprintf("Name parameter invalid: %q: %s", name, strings.Join(msgs, ";")))
	}
	key = key + "/" + name
	return key, nil
}

// NoNamespaceKeyFunc is the default function for constructing storage paths
// to a resource relative to the given prefix without a namespace.
func NoNamespaceKeyFunc(ctx context.Context, prefix string, name string) (string, error) {
	if len(name) == 0 {
		return "", apierrors.NewBadRequest("Name parameter required.")
	}
	if msgs := path.IsValidPathSegmentName(name); len(msgs) != 0 {
		return "", apierrors.NewBadRequest(fmt.Sprintf("Name parameter invalid: %q: %s", name, strings.Join(msgs, ";")))
	}
	key := prefix + "/" + name
	return key, nil
}
