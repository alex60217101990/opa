// Copyright 2026 The OPA Authors.  All rights reserved.
// Use of this source code is governed by an Apache2
// license that can be found in the LICENSE file.

//go:build !go1.23

package arena

import (
	"sync"
)

// StringHandle is a string (fallback for Go < 1.23).
// Without unique.Handle, we just store strings directly.
// This means no interning optimization, but the code still works.
type StringHandle string

var (
	internMu    sync.RWMutex
	internCache = make(map[string]string)
)

// InternString interns a string (fallback implementation).
func InternString(s string) StringHandle {
	internMu.RLock()
	if cached, ok := internCache[s]; ok {
		internMu.RUnlock()
		return StringHandle(cached)
	}
	internMu.RUnlock()

	internMu.Lock()
	// Double-check after acquiring write lock
	if cached, ok := internCache[s]; ok {
		internMu.Unlock()
		return StringHandle(cached)
	}

	internCache[s] = s
	internMu.Unlock()
	return StringHandle(s)
}

// GetString retrieves the value from a handle.
func GetString(h StringHandle) string {
	return string(h)
}

// EmptyHandle returns an empty handle.
func EmptyHandle() StringHandle {
	return ""
}
