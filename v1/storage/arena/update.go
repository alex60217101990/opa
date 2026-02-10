// Copyright 2026 The OPA Authors.  All rights reserved.
// Use of this source code is governed by an Apache2
// license that can be found in the LICENSE file.

package arena

import (
	"strconv"

	"github.com/open-policy-agent/opa/v1/storage"
)

// This file implements patch operations for pending updates within transactions.
// Used primarily for merging multiple updates in readWithUpdates().
//
// Optimizations applied:
// - parseArrayIndex: Shared helper with fast path for single-digit indices
// - applyPatchToMap: Fast path for empty maps, pre-calculated capacity
// - insertAt/removeAt: Fast paths for append/prepend operations
//
// NOTE: Apply() creates copies of map/slice structures. For the main commit path,
// updates are applied directly to arena nodes via setValue/removeValue (zero-copy).

// dataUpdate represents a pending update to the storage.
type dataUpdate interface {
	Path() storage.Path
	Value() any
	Remove() bool
	Apply(any) any
}

// arenaUpdate implements dataUpdate for arena storage.
type arenaUpdate struct {
	path   storage.Path
	value  any
	op     storage.PatchOp
	remove bool
}

// Path returns the path of the update.
func (u *arenaUpdate) Path() storage.Path {
	return u.path
}

// Value returns the value of the update.
func (u *arenaUpdate) Value() any {
	return u.value
}

// Remove returns whether this is a remove operation.
func (u *arenaUpdate) Remove() bool {
	return u.remove
}

// Apply applies the update to a base value.
func (u *arenaUpdate) Apply(base any) any {
	if len(u.path) == 0 {
		return u.value
	}

	// For simplicity, we convert to map/slice and apply
	// TODO: Optimize this to work directly with arena nodes
	return applyPatch(base, u.path, u.op, u.value)
}

// applyPatch applies a patch operation to a base value.
func applyPatch(base any, path storage.Path, op storage.PatchOp, value any) any {
	if len(path) == 0 {
		if op == storage.RemoveOp {
			return nil
		}
		return value
	}

	key := path[0]
	rest := path[1:]

	switch b := base.(type) {
	case map[string]any:
		return applyPatchToMap(b, key, rest, op, value)
	case []any:
		return applyPatchToSlice(b, key, rest, op, value)
	default:
		// Can't navigate further
		return base
	}
}

// applyPatchToMap applies a patch to a map.
func applyPatchToMap(m map[string]any, key string, rest storage.Path, op storage.PatchOp, value any) map[string]any {
	// Fast path: if map is empty and we're adding
	if len(m) == 0 && op == storage.AddOp {
		if len(rest) == 0 {
			return map[string]any{key: value}
		}
		return map[string]any{key: applyPatch(map[string]any{}, rest, op, value)}
	}

	// Pre-calculate result size for remove operation
	resultSize := len(m)
	if len(rest) == 0 && op == storage.RemoveOp {
		if _, exists := m[key]; exists {
			resultSize--
		}
	}

	result := make(map[string]any, resultSize)

	// Copy existing keys
	for k, v := range m {
		result[k] = v
	}

	if len(rest) == 0 {
		// This is the target
		switch op {
		case storage.AddOp, storage.ReplaceOp:
			result[key] = value
		case storage.RemoveOp:
			delete(result, key)
		}
	} else {
		// Navigate deeper
		if existing, ok := result[key]; ok {
			result[key] = applyPatch(existing, rest, op, value)
		} else if op == storage.AddOp {
			// Create intermediate object
			result[key] = applyPatch(map[string]any{}, rest, op, value)
		}
	}

	return result
}

// parseArrayIndex parses an array index from a string.
// Returns -1 if the key is invalid.
func parseArrayIndex(key string, arrayLen int) int {
	if key == "-" {
		return arrayLen
	}

	// Fast path for single digit (common case)
	if len(key) == 1 {
		if key[0] >= '0' && key[0] <= '9' {
			return int(key[0] - '0')
		}
		return -1
	}

	// Use strconv for multi-digit numbers (more efficient than manual parsing)
	idx, err := strconv.Atoi(key)
	if err != nil || idx < 0 {
		return -1
	}
	return idx
}

// applyPatchToSlice applies a patch to a slice.
func applyPatchToSlice(s []any, key string, rest storage.Path, op storage.PatchOp, value any) []any {
	// Parse index
	idx := parseArrayIndex(key, len(s))
	if idx < 0 {
		return s
	}

	if len(rest) == 0 {
		// This is the target
		switch op {
		case storage.AddOp:
			return insertAt(s, idx, value)
		case storage.ReplaceOp:
			if idx >= 0 && idx < len(s) {
				result := make([]any, len(s))
				copy(result, s)
				result[idx] = value
				return result
			}
		case storage.RemoveOp:
			if idx >= 0 && idx < len(s) {
				return removeAt(s, idx)
			}
		}
	} else {
		// Navigate deeper
		if idx >= 0 && idx < len(s) {
			result := make([]any, len(s))
			copy(result, s)
			result[idx] = applyPatch(s[idx], rest, op, value)
			return result
		}
	}

	return s
}

// insertAt inserts a value at the given index.
func insertAt(s []any, idx int, value any) []any {
	if idx < 0 {
		idx = 0
	}
	if idx > len(s) {
		idx = len(s)
	}

	// Fast path: append to end
	if idx == len(s) {
		result := make([]any, len(s), len(s)+1)
		copy(result, s)
		return append(result, value)
	}

	// Fast path: prepend to start
	if idx == 0 {
		return append([]any{value}, s...)
	}

	// General case: insert in middle
	result := make([]any, len(s)+1)
	copy(result[:idx], s[:idx])
	result[idx] = value
	copy(result[idx+1:], s[idx:])
	return result
}

// removeAt removes the value at the given index.
func removeAt(s []any, idx int) []any {
	if idx < 0 || idx >= len(s) {
		return s
	}

	// Fast path: remove last element
	if idx == len(s)-1 {
		result := make([]any, len(s)-1)
		copy(result, s[:idx])
		return result
	}

	// Fast path: remove first element
	if idx == 0 {
		result := make([]any, len(s)-1)
		copy(result, s[1:])
		return result
	}

	// General case: remove from middle
	result := make([]any, len(s)-1)
	copy(result[:idx], s[:idx])
	copy(result[idx:], s[idx+1:])
	return result
}
