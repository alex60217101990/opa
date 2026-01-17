// Copyright 2017 The OPA Authors.  All rights reserved.
// Use of this source code is governed by an Apache2
// license that can be found in the LICENSE file.

package inmem

import (
	"encoding/json"
	"slices"
	"strconv"

	"github.com/open-policy-agent/opa/internal/deepcopy"
	"github.com/open-policy-agent/opa/v1/ast"
	"github.com/open-policy-agent/opa/v1/storage"
	"github.com/open-policy-agent/opa/v1/storage/internal/errors"
	"github.com/open-policy-agent/opa/v1/storage/internal/ptr"
)

// transaction implements the low-level read/write operations on the in-memory
// store and contains the state required for pending transactions.
//
// For write transactions, the struct contains a logical set of updates
// performed by write operations in the transaction. Each write operation
// compacts the set such that two updates never overlap:
//
// - If new update path is a prefix of existing update path, existing update is
// removed, new update is added.
//
// - If existing update path is a prefix of new update path, existing update is
// modified.
//
// - Otherwise, new update is added.
//
// Read transactions do not require any special handling and simply passthrough
// to the underlying store. Read transactions do not support upgrade.
type transaction struct {
	db        *store                  // 8 bytes
	updates   []dataUpdate            // 24 bytes (replaced list.List with slice)
	pathIndex map[string]int          // 8 bytes (path.String() -> index for O(1) lookup)
	context   *storage.Context        // 8 bytes
	policies  map[string]policyUpdate // 8 bytes
	xid       uint64                  // 8 bytes
	write     bool                    // 1 byte
	stale     bool                    // 1 byte
	// Total: 72 bytes (with optimal alignment, no padding between bools)
}

type policyUpdate struct {
	value  *lazyPolicy // 8 bytes - compressed policy (nil for removal)
	remove bool        // 1 byte
	// Total: 16 bytes (8 bytes padding after bool)
}

func (txn *transaction) ID() uint64 {
	return txn.xid
}

func (txn *transaction) Write(op storage.PatchOp, path storage.Path, value any) error {
	if !txn.write {
		return &storage.Error{Code: storage.InvalidTransactionErr, Message: "data write during read transaction"}
	}

	if len(path) == 0 {
		return txn.updateRoot(op, value)
	}

	// Adaptive path index: Only enable for larger transactions
	// Threshold chosen based on benchmarks: <16 updates = O(n) iteration faster than map overhead
	const pathIndexThreshold = 16

	if txn.pathIndex == nil && len(txn.updates) >= pathIndexThreshold {
		// Transaction has grown large - initialize index for O(1) lookups
		txn.initPathIndex()
	}

	pathStr := path.String()

	// Fast path: Use O(1) index lookup if available (large transactions)
	if txn.pathIndex != nil {
		if idx, exists := txn.pathIndex[pathStr]; exists {
			update := txn.updates[idx]

			if update.Remove() {
				if op != storage.AddOp {
					return errors.NotFoundErr
				}
			}

			// If the last update has the same path and value, we have nothing to do.
			if txn.db.returnASTValuesOnRead {
				if astValue, ok := update.Value().(ast.Value); ok {
					if equalsValue(value, astValue) {
						return nil
					}
				}
			} else if comparableEquals(update.Value(), value) {
				return nil
			}

			// Remove existing update from slice and index
			txn.removeUpdate(idx)
			// Will add new update below
		}
	} else {
		// Slow path: O(n) iteration for small transactions (<16 updates)
		// For small N, linear scan is faster than map overhead
		for i := 0; i < len(txn.updates); i++ {
			update := txn.updates[i]

			// Check for exact match
			if update.Path().Equal(path) {
				if update.Remove() {
					if op != storage.AddOp {
						return errors.NotFoundErr
					}
				}

				// If the last update has the same path and value, we have nothing to do.
				if txn.db.returnASTValuesOnRead {
					if astValue, ok := update.Value().(ast.Value); ok {
						if equalsValue(value, astValue) {
							return nil
						}
					}
				} else if comparableEquals(update.Value(), value) {
					return nil
				}

				// Remove this update by slicing it out
				txn.updates = slices.Delete(txn.updates, i, i+1)
				break
			}
		}
	}

	// Check for prefix relationships (parent/child masking)
	// This still requires iteration but only for checking prefixes, not exact matches

	// Use pool for toRemove slice only if we have many updates (amortizes pool overhead)
	// For small transactions, stack allocation is faster
	const poolThreshold = 32
	var toRemoveStack [8]int         // stack-allocated for small cases
	var toRemove []int               // slice for collecting indices
	var toRemovePtr *[]int           // pool pointer (only used if > threshold)

	if len(txn.updates) > poolThreshold {
		// Large transaction - use pool to reduce heap allocations
		toRemovePtr = getIntSlice()
		toRemove = *toRemovePtr
		defer putIntSlice(toRemovePtr)
	} else {
		// Small transaction - use stack allocation
		toRemove = toRemoveStack[:0]
	}

	for i := 0; i < len(txn.updates); i++ {
		update := txn.updates[i]

		// Check if new update masks existing update (existing is child of new)
		if update.Path().HasPrefix(path) {
			toRemove = append(toRemove, i)
			continue
		}

		// Check if new update modifies existing update (new is child of existing)
		if path.HasPrefix(update.Path()) {
			if update.Remove() {
				return errors.NotFoundErr
			}
			suffix := path[len(update.Path()):]
			newUpdate, err := txn.db.newUpdate(update.Value(), op, suffix, 0, value)
			if err != nil {
				return err
			}
			update.Set(newUpdate.Apply(update.Value()))
			return nil
		}
	}

	// Remove masked updates in reverse order to maintain indices
	for i := len(toRemove) - 1; i >= 0; i-- {
		txn.removeUpdate(toRemove[i])
	}

	// Create and add new update
	update, err := txn.db.newUpdate(txn.db.data, op, path, 0, value)
	if err != nil {
		return err
	}

	// Prepend to maintain LIFO order (most recent first)
	txn.updates = slices.Insert(txn.updates, 0, update)

	// Update index if it exists (only for large transactions)
	if txn.pathIndex != nil {
		// Increment all existing indices due to prepend
		for k, v := range txn.pathIndex {
			txn.pathIndex[k] = v + 1
		}
		// Add new entry at index 0
		txn.pathIndex[pathStr] = 0
	}

	return nil
}

// initPathIndex initializes the path index by building it from existing updates.
// Called when transaction grows beyond threshold to enable O(1) lookups.
// Uses sync.Pool to reduce allocations for large transactions.
func (txn *transaction) initPathIndex() {
	if txn.pathIndex != nil {
		return // Already initialized
	}

	// Get map from pool to reduce allocations (only for large transactions)
	// For small transactions, pool overhead isn't worth it, but we only call
	// this function when len(updates) >= threshold, so pool is always beneficial here
	txn.pathIndex = getPathIndex()

	// Build index from existing updates
	for i, update := range txn.updates {
		pathStr := update.Path().String()
		txn.pathIndex[pathStr] = i
	}
}

// removeUpdate removes an update at the given index and updates the path index.
func (txn *transaction) removeUpdate(idx int) {
	if idx >= len(txn.updates) {
		return
	}

	// Remove from path index
	pathStr := txn.updates[idx].Path().String()
	delete(txn.pathIndex, pathStr)

	// Remove from slice
	txn.updates = slices.Delete(txn.updates, idx, idx+1)

	// Update remaining indices in path index
	for k, v := range txn.pathIndex {
		if v > idx {
			txn.pathIndex[k] = v - 1
		}
	}
}

func comparableEquals(a, b any) bool {
	switch a := a.(type) {
	case nil:
		return b == nil
	case bool:
		if vb, ok := b.(bool); ok {
			return vb == a
		}
	case string:
		if vs, ok := b.(string); ok {
			return vs == a
		}
	case json.Number:
		if vn, ok := b.(json.Number); ok {
			return vn == a
		}
	}
	return false
}

func (txn *transaction) updateRoot(op storage.PatchOp, value any) error {
	if op == storage.RemoveOp {
		return errors.RootCannotBeRemovedErr
	}

	var update any
	if txn.db.returnASTValuesOnRead {
		valueAST, err := ast.InterfaceToValue(value)
		if err != nil {
			return err
		}
		if _, ok := valueAST.(ast.Object); !ok {
			return errors.RootMustBeObjectErr
		}

		update = &updateAST{
			path:   storage.RootPath,
			remove: false,
			value:  valueAST,
		}
	} else {
		if _, ok := value.(map[string]any); !ok {
			return errors.RootMustBeObjectErr
		}

		update = &updateRaw{
			path:   storage.RootPath,
			remove: false,
			value:  value,
		}
	}

	// Clear all existing updates and add only the root update
	txn.updates = []dataUpdate{update.(dataUpdate)}

	return nil
}

func (txn *transaction) Commit() (result storage.TriggerEvent) {
	result.Context = txn.context

	// Return pathIndex map to pool after commit (no longer needed)
	if txn.pathIndex != nil {
		defer putPathIndex(txn.pathIndex)
		txn.pathIndex = nil
	}

	// Check once if we have triggers - avoid repeated map len checks
	hasTriggers := len(txn.db.triggers) > 0

	if len(txn.updates) > 0 {
		// Only preallocate event slice if we have triggers
		if hasTriggers {
			result.Data = slices.Grow(result.Data, len(txn.updates))
		}

		for _, action := range txn.updates {
			txn.db.data = action.Apply(txn.db.data)

			// Only build event if triggers exist
			if hasTriggers {
				result.Data = append(result.Data, storage.DataEvent{
					Path:    action.Path(),
					Data:    action.Value(),
					Removed: action.Remove(),
				})
			}
		}
	}

	if len(txn.policies) > 0 {
		// Only preallocate if we have triggers
		if hasTriggers {
			result.Policy = slices.Grow(result.Policy, len(txn.policies))
		}

		for id, upd := range txn.policies {
			if upd.remove {
				delete(txn.db.policies, id)
			} else {
				txn.db.policies[id] = upd.value
			}

			// Only decompress and build policy event if triggers exist
			if hasTriggers {
				// Decompress policy data for triggers
				var policyData []byte
				if !upd.remove && upd.value != nil {
					var err error
					policyData, err = upd.value.get()
					if err != nil {
						// Should not happen - data was just compressed successfully
						panic(err)
					}
				}
				result.Policy = append(result.Policy, storage.PolicyEvent{
					ID:      id,
					Data:    policyData,
					Removed: upd.remove,
				})
			}
		}
	}
	return result
}

func pointer(v any, path storage.Path) (any, error) {
	if v, ok := v.(ast.Value); ok {
		return ptr.ValuePtr(v, path)
	}
	return ptr.Ptr(v, path)
}

func deepcpy(v any) any {
	if v, ok := v.(ast.Value); ok {
		var cpy ast.Value

		switch data := v.(type) {
		case ast.Object:
			cpy = data.Copy()
		case *ast.Array:
			cpy = data.Copy()
		}

		return cpy
	}
	return deepcopy.DeepCopy(v)
}

func (txn *transaction) Read(path storage.Path) (any, error) {
	if !txn.write || len(txn.updates) == 0 {
		return pointer(txn.db.data, path)
	}

	var merge []dataUpdate

	for _, upd := range txn.updates {
		if path.HasPrefix(upd.Path()) {
			if upd.Remove() {
				return nil, errors.NotFoundErr
			}
			return pointer(upd.Value(), path[len(upd.Path()):])
		}

		if upd.Path().HasPrefix(path) {
			merge = append(merge, upd)
		}
	}

	data, err := pointer(txn.db.data, path)

	if err != nil {
		return nil, err
	}

	if len(merge) == 0 {
		return data, nil
	}

	cpy := deepcpy(data)

	for _, update := range merge {
		cpy = update.Relative(path).Apply(cpy)
	}

	return cpy, nil
}

func (txn *transaction) ListPolicies() (ids []string) {
	for id := range txn.db.policies {
		if _, ok := txn.policies[id]; !ok {
			ids = append(ids, id)
		}
	}
	for id, update := range txn.policies {
		if !update.remove {
			ids = append(ids, id)
		}
	}
	return ids
}

func (txn *transaction) GetPolicy(id string) ([]byte, error) {
	// Check transaction-local updates first
	if txn.policies != nil {
		if update, ok := txn.policies[id]; ok {
			if update.remove {
				return nil, errors.NewNotFoundErrorf("policy id %q", id)
			}
			// Decompress and return from transaction update
			return update.value.get()
		}
	}
	// Check committed policies
	if lazyPol, ok := txn.db.policies[id]; ok {
		// Lazy decompression happens here - cached for subsequent reads
		return lazyPol.get()
	}
	return nil, errors.NewNotFoundErrorf("policy id %q", id)
}

func (txn *transaction) UpsertPolicy(id string, bs []byte) error {
	// Compress policy data immediately to save memory
	lazyPol := newLazyPolicy(bs)
	return txn.updatePolicy(id, policyUpdate{lazyPol, false})
}

func (txn *transaction) DeletePolicy(id string) error {
	return txn.updatePolicy(id, policyUpdate{nil, true})
}

func (txn *transaction) updatePolicy(id string, update policyUpdate) error {
	if !txn.write {
		return &storage.Error{Code: storage.InvalidTransactionErr, Message: "policy write during read transaction"}
	}

	if txn.policies == nil {
		txn.policies = map[string]policyUpdate{id: update}
	} else {
		txn.policies[id] = update
	}

	return nil
}

type dataUpdate interface {
	Path() storage.Path
	Remove() bool
	Apply(any) any
	Relative(path storage.Path) dataUpdate
	Set(any)
	Value() any
}

// update contains state associated with an update to be applied to the
// in-memory data store.
type updateRaw struct {
	path   storage.Path // 24 bytes (slice header) - data path modified by update
	value  any          // 16 bytes (interface) - value to add/replace at path (ignored if remove is true)
	remove bool         // 1 byte - indicates whether update removes the value at path
	// Total: 48 bytes (24 + 16 + 1 + 7 padding = 48)
}

func equalsValue(a any, v ast.Value) bool {
	if a, ok := a.(ast.Value); ok {
		return a.Compare(v) == 0
	}
	switch a := a.(type) {
	case nil:
		return v == ast.NullValue
	case bool:
		if vb, ok := v.(ast.Boolean); ok {
			return bool(vb) == a
		}
	case string:
		if vs, ok := v.(ast.String); ok {
			return string(vs) == a
		}
	}

	return false
}

func (db *store) newUpdate(data any, op storage.PatchOp, path storage.Path, idx int, value any) (dataUpdate, error) {
	if db.returnASTValuesOnRead {
		astData, err := ast.InterfaceToValue(data)
		if err != nil {
			return nil, err
		}
		astValue, err := ast.InterfaceToValue(value)
		if err != nil {
			return nil, err
		}
		return newUpdateAST(astData, op, path, idx, astValue)
	}
	return newUpdateRaw(data, op, path, idx, value)
}

func newUpdateRaw(data any, op storage.PatchOp, path storage.Path, idx int, value any) (dataUpdate, error) {
	switch data.(type) {
	case nil, bool, json.Number, string:
		return nil, errors.NotFoundErr
	}

	switch data := data.(type) {
	case map[string]any:
		return newUpdateObject(data, op, path, idx, value)

	case []any:
		return newUpdateArray(data, op, path, idx, value)
	}

	return nil, &storage.Error{
		Code:    storage.InternalErr,
		Message: "invalid data value encountered",
	}
}

func newUpdateArray(data []any, op storage.PatchOp, path storage.Path, idx int, value any) (dataUpdate, error) {
	if idx == len(path)-1 {
		if path[idx] == "-" || path[idx] == strconv.Itoa(len(data)) {
			if op != storage.AddOp {
				return nil, errors.NewInvalidPatchError("%v: invalid patch path", path)
			}
			cpy := make([]any, len(data)+1)
			copy(cpy, data)
			cpy[len(data)] = value
			return &updateRaw{path[:len(path)-1], cpy, false}, nil
		}

		pos, err := ptr.ValidateArrayIndex(data, path[idx], path)
		if err != nil {
			return nil, err
		}

		switch op {
		case storage.AddOp:
			cpy := make([]any, len(data)+1)
			copy(cpy[:pos], data[:pos])
			copy(cpy[pos+1:], data[pos:])
			cpy[pos] = value
			return &updateRaw{path[:len(path)-1], cpy, false}, nil

		case storage.RemoveOp:
			cpy := make([]any, len(data)-1)
			copy(cpy[:pos], data[:pos])
			copy(cpy[pos:], data[pos+1:])
			return &updateRaw{path[:len(path)-1], cpy, false}, nil

		default:
			cpy := make([]any, len(data))
			copy(cpy, data)
			cpy[pos] = value
			return &updateRaw{path[:len(path)-1], cpy, false}, nil
		}
	}

	pos, err := ptr.ValidateArrayIndex(data, path[idx], path)
	if err != nil {
		return nil, err
	}

	return newUpdateRaw(data[pos], op, path, idx+1, value)
}

func newUpdateObject(data map[string]any, op storage.PatchOp, path storage.Path, idx int, value any) (dataUpdate, error) {

	if idx == len(path)-1 {
		switch op {
		case storage.ReplaceOp, storage.RemoveOp:
			if _, ok := data[path[idx]]; !ok {
				return nil, errors.NotFoundErr
			}
		}
		return &updateRaw{path, value, op == storage.RemoveOp}, nil
	}

	if data, ok := data[path[idx]]; ok {
		return newUpdateRaw(data, op, path, idx+1, value)
	}

	return nil, errors.NotFoundErr
}

func (u *updateRaw) Remove() bool {
	return u.remove
}

func (u *updateRaw) Path() storage.Path {
	return u.path
}

func (u *updateRaw) Apply(data any) any {
	if len(u.path) == 0 {
		return u.value
	}
	parent, err := ptr.Ptr(data, u.path[:len(u.path)-1])
	if err != nil {
		panic(err)
	}
	key := u.path[len(u.path)-1]
	if u.remove {
		obj := parent.(map[string]any)
		delete(obj, key)
		return data
	}
	switch parent := parent.(type) {
	case map[string]any:
		if parent == nil {
			parent = make(map[string]any, 1)
		}
		parent[key] = u.value
	case []any:
		idx, err := strconv.Atoi(key)
		if err != nil {
			panic(err)
		}
		parent[idx] = u.value
	}
	return data
}

func (u *updateRaw) Set(v any) {
	u.value = v
}

func (u *updateRaw) Value() any {
	return u.value
}

func (u *updateRaw) Relative(path storage.Path) dataUpdate {
	cpy := *u
	cpy.path = cpy.path[len(path):]
	return &cpy
}
