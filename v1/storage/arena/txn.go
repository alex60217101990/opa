// Copyright 2026 The OPA Authors.  All rights reserved.
// Use of this source code is governed by an Apache2
// license that can be found in the LICENSE file.

package arena

import (
	"github.com/open-policy-agent/opa/v1/storage"
	"github.com/open-policy-agent/opa/v1/storage/internal/errors"
)

// transaction represents a transaction in the arena storage.
// Optimized to use preallocated slices instead of maps and lists.
type transaction struct {
	arena    *Arena
	xid      uint64
	rootIdx  int32
	updates  []dataUpdate       // Preallocated slice (8 cap by default)
	policies []txnPolicyUpdate  // Preallocated slice (4 cap by default)
	context  *storage.Context
	write    bool
	stale    bool
}

// txnPolicyUpdate represents a pending policy change.
// Includes ID in the struct to avoid map overhead.
type txnPolicyUpdate struct {
	id     StringHandle // Interned policy ID
	value  []byte
	remove bool
}

// ID implements storage.Transaction.
func (txn *transaction) ID() uint64 {
	return txn.xid
}

// Read reads a value at the given path.
func (txn *transaction) Read(path storage.Path) (any, error) {
	// For write transactions, check pending updates first
	if txn.write && txn.updates != nil {
		return txn.readWithUpdates(path)
	}

	// Read directly from arena
	root := txn.arena.getNode(txn.rootIdx)
	if len(path) == 0 {
		return root.ToInterface(txn.arena), nil
	}

	node, found := root.PathLookup(txn.arena, path)
	if !found {
		return nil, errors.NotFoundErr
	}

	return node.ToInterface(txn.arena), nil
}

// readWithUpdates reads a value considering pending updates.
func (txn *transaction) readWithUpdates(path storage.Path) (any, error) {
	// Check if any update affects this path
	var relevantUpdates []dataUpdate

	for i := range txn.updates {
		upd := txn.updates[i]

		// If update path is a prefix of our path, it contains our data
		if path.HasPrefix(upd.Path()) {
			if upd.Remove() {
				return nil, errors.NotFoundErr
			}
			// Read from the update's value
			suffix := path[len(upd.Path()):]
			if len(suffix) == 0 {
				return upd.Value(), nil
			}
			// Need to navigate into the update's value
			return txn.navigateValue(upd.Value(), suffix)
		}

		// If our path is a prefix of update path, we need to merge
		if upd.Path().HasPrefix(path) {
			relevantUpdates = append(relevantUpdates, upd)
		}
	}

	// Read base value from arena (direct, not through readWithUpdates)
	var baseValue any
	var baseErr error

	root := txn.arena.getNode(txn.rootIdx)
	if len(path) == 0 {
		baseValue = root.ToInterface(txn.arena)
	} else {
		node, found := root.PathLookup(txn.arena, path)
		if !found {
			baseErr = errors.NotFoundErr
		} else {
			baseValue = node.ToInterface(txn.arena)
		}
	}

	// If no base value and no updates, return error
	if baseErr != nil && len(relevantUpdates) == 0 {
		return nil, baseErr
	}

	// If no updates affect this path, return base value
	if len(relevantUpdates) == 0 {
		if baseErr != nil {
			return nil, baseErr
		}
		return baseValue, nil
	}

	// Apply updates to base value
	// For simplicity, convert to map/slice and apply updates
	// TODO: Optimize this to avoid conversions
	if baseErr != nil {
		baseValue = map[string]any{} // Start with empty if no base
	}
	return txn.applyUpdates(baseValue, path, relevantUpdates), nil
}

// navigateValue navigates into a value using a path.
func (txn *transaction) navigateValue(value any, path storage.Path) (any, error) {
	current := value

	for _, key := range path {
		switch v := current.(type) {
		case map[string]any:
			var ok bool
			current, ok = v[key]
			if !ok {
				return nil, errors.NotFoundErr
			}
		case []any:
			// Parse array index using shared helper
			idx := parseArrayIndex(key, len(v))
			if idx < 0 || idx >= len(v) {
				return nil, errors.NotFoundErr
			}
			current = v[idx]
		default:
			return nil, errors.NotFoundErr
		}
	}

	return current, nil
}

// applyUpdates applies pending updates to a base value.
func (txn *transaction) applyUpdates(base any, basePath storage.Path, updates []dataUpdate) any {
	// This is a simplified implementation
	// For production, this would need proper merging logic
	for _, upd := range updates {
		// Apply each update
		base = upd.Apply(base)
	}
	return base
}

// Write performs a write operation.
func (txn *transaction) Write(op storage.PatchOp, path storage.Path, value any) error {
	if !txn.write {
		return &storage.Error{
			Code:    storage.InvalidTransactionErr,
			Message: "cannot write in read-only transaction",
		}
	}

	if len(path) == 0 {
		return txn.updateRoot(op, value)
	}

	// Check for overlapping updates (iterate backwards for efficient removal)
	for i := len(txn.updates) - 1; i >= 0; i-- {
		upd := txn.updates[i]

		// Exact match - replace the update
		if upd.Path().Equal(path) {
			if upd.Remove() && op != storage.AddOp {
				return errors.NotFoundErr
			}
			txn.removeUpdateAt(i)
			break
		}

		// New update masks existing update
		if upd.Path().HasPrefix(path) {
			txn.removeUpdateAt(i)
			continue
		}

		// Existing update contains new update - need to modify it
		if path.HasPrefix(upd.Path()) {
			if upd.Remove() {
				return errors.NotFoundErr
			}
			// Create a nested update
			// For simplicity, we'll just add a new update
			// TODO: Optimize this
			break
		}
	}

	// Create new update
	update := &arenaUpdate{
		path:   path,
		op:     op,
		value:  value,
		remove: op == storage.RemoveOp,
	}

	txn.addUpdate(update)
	return nil
}

// updateRoot updates the root of the storage.
func (txn *transaction) updateRoot(op storage.PatchOp, value any) error {
	if op == storage.RemoveOp {
		return errors.RootCannotBeRemovedErr
	}

	if _, ok := value.(map[string]any); !ok {
		return errors.RootMustBeObjectErr
	}

	update := &arenaUpdate{
		path:   storage.RootPath,
		op:     op,
		value:  value,
		remove: false,
	}

	// Clear all updates and add root update
	txn.clearUpdates()
	txn.addUpdate(update)

	return nil
}

// Commit commits the transaction.
func (txn *transaction) Commit() storage.TriggerEvent {
	event := storage.TriggerEvent{
		Context: txn.context,
	}

	// Apply data updates
	if txn.updates != nil {
		for i := range txn.updates {
			upd := txn.updates[i]

			// Apply update to arena
			err := txn.applyUpdate(upd)
			if err != nil {
				// Log error but continue
				continue
			}

			// Record event for triggers
			if len(txn.arena.triggers) > 0 {
				event.Data = append(event.Data, storage.DataEvent{
					Path:    upd.Path(),
					Data:    upd.Value(),
					Removed: upd.Remove(),
				})
			}
		}
	}

	// Apply policy updates (using simple map - no arena overhead!)
	if len(txn.policies) > 0 {
		txn.arena.policiesMu.Lock()
		for i := range txn.policies {
			upd := txn.policies[i]
			id := GetString(upd.id)

			if upd.remove {
				delete(txn.arena.policies, id)
			} else {
				txn.arena.policies[id] = upd.value
			}

			if len(txn.arena.triggers) > 0 {
				event.Policy = append(event.Policy, storage.PolicyEvent{
					ID:      id,
					Data:    upd.value,
					Removed: upd.remove,
				})
			}
		}
		txn.arena.policiesMu.Unlock()
	}

	return event
}

// applyUpdate applies a data update to the arena.
func (txn *transaction) applyUpdate(upd dataUpdate) error {
	path := upd.Path()
	value := upd.Value()

	if len(path) == 0 {
		// Root update
		if m, ok := value.(map[string]any); ok {
			txn.arena.rootIdx = txn.arena.LoadMap(m)
			return nil
		}
		return errors.RootMustBeObjectErr
	}

	// Handle remove operation
	if upd.Remove() {
		return txn.arena.removeValue(txn.arena.rootIdx, path, 0)
	}

	// For other paths, use setValue (modifies in-place, doesn't change rootIdx)
	_, err := txn.arena.setValue(txn.arena.rootIdx, path, 0, value)
	return err
}

// ListPolicies lists all policy IDs.
func (txn *transaction) ListPolicies() []string {
	// Get base policies from simple map (no arena overhead!)
	txn.arena.policiesMu.RLock()
	baseSize := len(txn.arena.policies)
	baseIDs := make([]string, 0, baseSize)
	for id := range txn.arena.policies {
		baseIDs = append(baseIDs, id)
	}
	txn.arena.policiesMu.RUnlock()

	if len(txn.policies) == 0 {
		return baseIDs
	}

	// Create result with capacity for all possible policies
	result := make([]string, 0, len(baseIDs)+len(txn.policies))
	seen := make(map[string]bool, len(baseIDs)+len(txn.policies))

	// Add base policies that haven't been updated
	for _, id := range baseIDs {
		if _, found := txn.findPolicyUpdate(id); !found {
			result = append(result, id)
			seen[id] = true
		}
	}

	// Add updated policies (not removed)
	for i := range txn.policies {
		upd := txn.policies[i]
		id := GetString(upd.id)
		if !upd.remove && !seen[id] {
			result = append(result, id)
		}
	}

	return result
}

// GetPolicy retrieves a policy by ID.
func (txn *transaction) GetPolicy(id string) ([]byte, error) {
	// Check updates first
	if upd, found := txn.hasPolicyUpdate(id); found {
		if upd.remove {
			return nil, errors.NewNotFoundErrorf("policy %q", id)
		}
		return upd.value, nil
	}

	// Check base policies in simple map
	txn.arena.policiesMu.RLock()
	data, ok := txn.arena.policies[id]
	txn.arena.policiesMu.RUnlock()

	if !ok {
		return nil, errors.NewNotFoundErrorf("policy %q", id)
	}
	return data, nil
}

// UpsertPolicy updates or inserts a policy.
func (txn *transaction) UpsertPolicy(id string, bs []byte) error {
	if !txn.write {
		return &storage.Error{
			Code:    storage.InvalidTransactionErr,
			Message: "cannot write in read-only transaction",
		}
	}

	txn.addPolicyUpdate(id, bs, false)
	return nil
}

// DeletePolicy deletes a policy.
func (txn *transaction) DeletePolicy(id string) error {
	if !txn.write {
		return &storage.Error{
			Code:    storage.InvalidTransactionErr,
			Message: "cannot write in read-only transaction",
		}
	}

	txn.addPolicyUpdate(id, nil, true)
	return nil
}
