// Copyright 2026 The OPA Authors.  All rights reserved.
// Use of this source code is governed by an Apache2
// license that can be found in the LICENSE file.

package arena

import (
	"github.com/open-policy-agent/opa/v1/storage/internal/errors"
)

// policyNode represents a policy stored in the arena.
// Instead of map[string][]byte, we use a linked list of policy nodes.
type policyNode struct {
	id       StringHandle // Interned policy ID
	data     []byte       // Policy bytes (not in arena, as []byte is already efficient)
	next     int32        // Index of next policy node
	removed  bool         // Tombstone flag
}

// PolicyStore manages policies in the arena using a node chain.
type PolicyStore struct {
	head  int32        // Head of policy chain (-1 if empty)
	nodes []policyNode // Preallocated policy nodes (grows as needed)
	count int          // Number of active policies
}

// NewPolicyStore creates a new policy store with preallocated capacity.
func NewPolicyStore(capacity int) *PolicyStore {
	// Don't allocate slice upfront - saves initial allocation
	// Slice will be allocated on first Upsert
	return &PolicyStore{
		head:  -1,
		nodes: nil, // Lazy allocation
		count: 0,
	}
}

// alloc allocates a policy node by reusing removed nodes or extending the slice.
func (ps *PolicyStore) alloc() int32 {
	// Lazy initialization
	if ps.nodes == nil {
		ps.nodes = make([]policyNode, 0, 8) // Small initial capacity
	}

	// Scan for a removed node to reuse
	for i := range ps.nodes {
		if ps.nodes[i].removed {
			ps.nodes[i].removed = false
			ps.nodes[i].data = nil // Clear old data
			return int32(i)
		}
	}

	// No removed nodes found, extend slice
	idx := int32(len(ps.nodes))
	ps.nodes = append(ps.nodes, policyNode{next: -1})
	return idx
}

// Upsert inserts or updates a policy.
func (ps *PolicyStore) Upsert(id string, data []byte) {
	idHandle := InternString(id)

	// Search for existing policy
	curr := ps.head
	var prev int32 = -1

	for curr != -1 {
		node := &ps.nodes[curr]
		if !node.removed && node.id == idHandle {
			// Update existing
			node.data = data
			return
		}
		prev = curr
		curr = node.next
	}

	// Not found, insert new
	newIdx := ps.alloc()
	newNode := &ps.nodes[newIdx]
	newNode.id = idHandle
	newNode.data = data
	newNode.next = -1
	newNode.removed = false

	if ps.head == -1 {
		ps.head = newIdx
	} else {
		ps.nodes[prev].next = newIdx
	}

	ps.count++
}

// Get retrieves a policy by ID.
func (ps *PolicyStore) Get(id string) ([]byte, error) {
	idHandle := InternString(id)
	curr := ps.head

	for curr != -1 {
		node := &ps.nodes[curr]
		if !node.removed && node.id == idHandle {
			return node.data, nil
		}
		curr = node.next
	}

	return nil, errors.NewNotFoundErrorf("policy %q", id)
}

// Delete marks a policy as removed (tombstone).
// NOTE: We don't modify the chain, just mark as removed.
// The freelist is rebuilt during alloc by scanning for removed nodes.
func (ps *PolicyStore) Delete(id string) error {
	idHandle := InternString(id)
	curr := ps.head

	for curr != -1 {
		node := &ps.nodes[curr]
		if !node.removed && node.id == idHandle {
			node.removed = true
			ps.count--
			return nil
		}
		curr = node.next
	}

	return errors.NewNotFoundErrorf("policy %q", id)
}

// List returns all active policy IDs.
func (ps *PolicyStore) List() []string {
	if ps.count == 0 {
		return nil
	}

	result := make([]string, 0, ps.count)
	curr := ps.head

	for curr != -1 {
		node := &ps.nodes[curr]
		if !node.removed {
			result = append(result, GetString(node.id))
		}
		curr = node.next
	}

	return result
}

// Len returns the number of active policies.
func (ps *PolicyStore) Len() int {
	return ps.count
}

// Copy creates a shallow copy of the policy store for transaction isolation.
func (ps *PolicyStore) Copy() *PolicyStore {
	return &PolicyStore{
		head:  ps.head,
		nodes: append([]policyNode(nil), ps.nodes...), // Copy slice
		count: ps.count,
	}
}
