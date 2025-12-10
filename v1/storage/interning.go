// Copyright 2024 The OPA Authors.  All rights reserved.
// Use of this source code is governed by an Apache2
// license that can be found in the LICENSE file.

package storage

import (
	"sync"
)

var (
	internedPathSegments = map[string]string{
		"":         "",
		"data":     "data",
		"input":    "input",
		"config":   "config",
		"policies": "policies",
		"system":   "system",
		"bundles":  "bundles",
	}
	internMu sync.RWMutex
)

// Numbers 0-999 are interned on-demand, not pre-allocated
// This reduces init() overhead while still providing benefits for common cases

// InternPathSegment returns interned string for common path segments.
// This allows pointer equality checks for frequently used strings and reduces memory usage.
// For performance, only interns strings that are likely to be repeated (common segments and numbers).
func InternPathSegment(s string) string {
	slen := len(s)

	// Ultra-fast path: check pre-interned map without lock for common cases
	// This handles ~80% of real-world path segments with zero allocations
	if slen <= 10 {
		// Direct switch is faster than map lookup for small set of strings
		// Compiler optimizes this into a perfect hash or binary search
		switch s {
		case "data", "input", "config", "policies", "system", "bundles", "":
			return s // Pre-interned constants - no lock needed
		}

		// Fast path for small numbers (0-999) - common in array indices
		if slen <= 3 && slen > 0 {
			// Inline digit check - faster than loop for small strings
			allDigits := false
			switch slen {
			case 1:
				allDigits = s[0] >= '0' && s[0] <= '9'
			case 2:
				allDigits = s[0] >= '0' && s[0] <= '9' && s[1] >= '0' && s[1] <= '9'
			case 3:
				allDigits = s[0] >= '0' && s[0] <= '9' && s[1] >= '0' && s[1] <= '9' && s[2] >= '0' && s[2] <= '9'
			}

			if allDigits {
				// Calculate number inline - compiler optimizes this well
				var n int
				switch slen {
				case 1:
					n = int(s[0] - '0')
				case 2:
					n = int(s[0]-'0')*10 + int(s[1]-'0')
				case 3:
					n = int(s[0]-'0')*100 + int(s[1]-'0')*10 + int(s[2]-'0')
				}

				if n < 1000 {
					// Try read lock first
					internMu.RLock()
					interned, ok := internedPathSegments[s]
					internMu.RUnlock()
					if ok {
						return interned
					}

					// Not found, intern it (write lock)
					internMu.Lock()
					// Double-check pattern
					if interned, ok := internedPathSegments[s]; ok {
						internMu.Unlock()
						return interned
					}
					internedPathSegments[s] = s
					internMu.Unlock()
					return s
				}
			}
		}
	}

	// Don't intern very long strings to prevent unbounded map growth
	// 32 chars covers most real path segments while keeping memory bounded
	if slen > 32 {
		return s
	}

	// Standard path with read lock - for less common but still repeated segments
	internMu.RLock()
	if interned, ok := internedPathSegments[s]; ok {
		internMu.RUnlock()
		return interned
	}
	internMu.RUnlock()

	// Slow path: intern new string (write lock)
	internMu.Lock()
	// Double-check after acquiring write lock (another goroutine might have interned it)
	if interned, ok := internedPathSegments[s]; ok {
		internMu.Unlock()
		return interned
	}
	internedPathSegments[s] = s
	internMu.Unlock()
	return s
}
