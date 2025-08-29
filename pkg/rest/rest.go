package rest

import "sigs.k8s.io/structured-merge-diff/v6/fieldpath"

// ResetFieldsStrategy is an optional interface that a storage object can
// implement if it wishes to provide the fields reset by its strategies.
type ResetFieldsStrategy interface {
	GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set
}
