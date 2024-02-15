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

import "fmt"

const (
	ErrNotFound int = iota + 1
	ErrAlreadyExists
	ErrInvalidObj
	ErrTimeout
)

var errCodeToMessage = map[int]string{
	ErrNotFound:      "NotFound",
	ErrAlreadyExists: "AlreadyExists",
	ErrInvalidObj:    "Invalid",
	ErrTimeout:       "Timout",
}

type StorageError struct {
	Code               int
	Key                string
	AdditionalErrorMsg string
}

func (r *StorageError) Error() string {
	return fmt.Sprintf("StorageError: %s, Code: %d, Key: %s, AdditionalErrorMsg: %s",
		errCodeToMessage[r.Code], r.Code, r.Key, r.AdditionalErrorMsg)
}

func NewNotFoundError(key string) *StorageError {
	return &StorageError{
		Code: ErrNotFound,
		Key:  key,
	}
}

func NewAlreadyExistsError(key string) *StorageError {
	return &StorageError{
		Code: ErrAlreadyExists,
		Key:  key,
	}
}

func NewInvalidObjError(key, msg string) *StorageError {
	return &StorageError{
		Code:               ErrInvalidObj,
		Key:                key,
		AdditionalErrorMsg: msg,
	}
}

// IsNotFound returns true if and only if err is "key" not found error.
func IsNotFound(err error) bool {
	return isErrCode(err, ErrNotFound)
}

// IsExist returns true if and only if err is "key" already exists error.
func IsExist(err error) bool {
	return isErrCode(err, ErrAlreadyExists)
}

// IsRequestTimeout returns true if and only if err indicates that the request has timed out.
func IsRequestTimeout(err error) bool {
	return isErrCode(err, ErrTimeout)
}

// IsInvalidObj returns true if and only if err is invalid error
func IsInvalidObj(err error) bool {
	return isErrCode(err, ErrInvalidObj)
}

func isErrCode(err error, code int) bool {
	if err == nil {
		return false
	}
	if e, ok := err.(*StorageError); ok {
		return e.Code == code
	}
	return false
}
