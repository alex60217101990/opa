// Copyright 2026 The OPA Authors.  All rights reserved.
// Use of this source code is governed by an Apache2
// license that can be found in the LICENSE file.

package arena

// Helper functions for optimized transaction operations using slices.

// findPolicyUpdate finds a policy update by ID in the slice.
func (txn *transaction) findPolicyUpdate(id string) (int, bool) {
	idHandle := InternString(id)
	for i := range txn.policies {
		if txn.policies[i].id == idHandle {
			return i, true
		}
	}
	return -1, false
}

// addPolicyUpdate adds or updates a policy in the transaction.
func (txn *transaction) addPolicyUpdate(id string, value []byte, remove bool) {
	idx, found := txn.findPolicyUpdate(id)

	if found {
		// Update existing
		txn.policies[idx].value = value
		txn.policies[idx].remove = remove
	} else {
		// Add new (preallocate if first time)
		if txn.policies == nil {
			txn.policies = make([]txnPolicyUpdate, 0, 4)
		}
		txn.policies = append(txn.policies, txnPolicyUpdate{
			id:     InternString(id),
			value:  value,
			remove: remove,
		})
	}
}

// hasPolicyUpdate checks if a policy has a pending update.
func (txn *transaction) hasPolicyUpdate(id string) (txnPolicyUpdate, bool) {
	idx, found := txn.findPolicyUpdate(id)
	if found {
		return txn.policies[idx], true
	}
	return txnPolicyUpdate{}, false
}

// addUpdate adds a data update to the transaction.
// Preallocates updates slice on first use.
func (txn *transaction) addUpdate(upd dataUpdate) {
	if txn.updates == nil {
		txn.updates = make([]dataUpdate, 0, 8)
	}
	txn.updates = append(txn.updates, upd)
}

// removeUpdateAt removes an update at the given index.
func (txn *transaction) removeUpdateAt(idx int) {
	// Remove by shifting elements (maintains order)
	copy(txn.updates[idx:], txn.updates[idx+1:])
	txn.updates = txn.updates[:len(txn.updates)-1]
}

// clearUpdates clears all updates (for root overwrite).
func (txn *transaction) clearUpdates() {
	txn.updates = txn.updates[:0] // Keep capacity
}
