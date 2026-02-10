// Copyright 2026 The OPA Authors.  All rights reserved.
// Use of this source code is governed by an Apache2
// license that can be found in the LICENSE file.

//go:build go1.23

package arena

import "unique"

// StringHandle is an interned string handle (Go 1.23+).
type StringHandle = unique.Handle[string]

// InternString interns a string.
func InternString(s string) StringHandle {
	return unique.Make(s)
}

// GetString retrieves the value from a handle.
func GetString(h StringHandle) string {
	return h.Value()
}

// EmptyHandle returns an empty handle.
func EmptyHandle() StringHandle {
	return unique.Handle[string]{}
}
