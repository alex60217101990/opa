// Copyright 2017 The OPA Authors.  All rights reserved.
// Use of this source code is governed by an Apache2
// license that can be found in the LICENSE file.

package inmem

import (
	"sync"
)

// Sync pools for temporary objects to reduce GC pressure.
// Inspired by v1/ast/syncpools.go pattern.
//
// IMPORTANT: Pools are only used for large transactions where the overhead
// is amortized by reducing heap allocations. Small transactions use
// stack allocation or simple make() calls as they're faster for small sizes.
//
// Pooling strategy:
// - pathIndex: Only used when adaptive index is enabled (>16 updates)
// - toRemove slice: Only used when transaction has >32 updates
// - For smaller transactions, overhead of pool Get/Put exceeds allocation cost

var (
	// pathIndexPool provides recycled maps for transaction path indices.
	// Path index is only used during transaction lifetime and discarded after commit.
	pathIndexPool = sync.Pool{
		New: func() any {
			// Pre-allocate for typical large transaction size
			return make(map[string]int, 32)
		},
	}

	// intSlicePool provides recycled slices for collecting indices to remove.
	// Used in Write() for prefix relationship checking.
	intSlicePool = sync.Pool{
		New: func() any {
			// Pre-allocate small capacity for typical case
			slice := make([]int, 0, 8)
			return &slice
		},
	}
)

// getPathIndex returns a map from the pool for use as a path index.
// Caller must call putPathIndex when done.
func getPathIndex() map[string]int {
	m := pathIndexPool.Get().(map[string]int)
	// Clear any existing entries (defensive - shouldn't be necessary)
	for k := range m {
		delete(m, k)
	}
	return m
}

// putPathIndex returns a path index map to the pool for reuse.
// The map will be cleared before being returned to the pool.
func putPathIndex(m map[string]int) {
	if m == nil {
		return
	}

	// Clear the map before returning to pool
	clear(m)

	pathIndexPool.Put(m)
}

// getIntSlice returns a slice from the pool for collecting indices.
// Caller must call putIntSlice when done.
func getIntSlice() *[]int {
	slicePtr := intSlicePool.Get().(*[]int)
	// Reset slice to zero length but keep capacity
	*slicePtr = (*slicePtr)[:0]
	return slicePtr
}

// putIntSlice returns an int slice to the pool for reuse.
func putIntSlice(slicePtr *[]int) {
	if slicePtr == nil {
		return
	}

	// Reset to zero length before returning
	*slicePtr = (*slicePtr)[:0]

	intSlicePool.Put(slicePtr)
}
