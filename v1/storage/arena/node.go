// Copyright 2026 The OPA Authors.  All rights reserved.
// Use of this source code is governed by an Apache2
// license that can be found in the LICENSE file.

package arena

import (
	"math"
	"strconv"
)

// ValueType identifies the type of value stored in a Node.
type ValueType uint32

const (
	TypeFree      ValueType = iota // Node is free and can be reused
	TypeInt                         // Integer value
	TypeFloat                       // Float64 value
	TypeBool                        // Boolean value
	TypeString                      // String value (interned)
	TypeObject                      // Object (map) - value points to head of child chain
	TypeArray                       // Array (slice) - value points to head of element chain
	TypeNull                        // Null value
	TypeTombstone                   // Marked for deletion
)

const (
	// SegmentSize defines how many nodes fit in one segment.
	// 16KB = 1024 * 16 / 32 bytes per node = 512 nodes per segment
	SegmentSize = 512

	// MaxSegments limits total arena size to ~64MB of nodes (512 * 4096 * 32 bytes)
	// Increased from 1024 to 4096 to support benchmarks and large datasets
	// This allows ~2M nodes (524,288 → 2,097,152)
	MaxSegments = 4096
)

// Node represents a single storage unit in the arena.
// Size: 32 bytes (cache-line friendly on most architectures)
//
// Memory layout:
//   - key: 8 bytes (unique.Handle for string interning)
//   - vStr: 8 bytes (unique.Handle for string values)
//   - vRaw: 8 bytes (raw numeric data or child index)
//   - next: 4 bytes (index of next sibling in chain)
//   - vType: 4 bytes (value type discriminator)
type Node struct {
	key   StringHandle // Interned key (for objects) or empty (for arrays)
	vStr  StringHandle // Interned string value (only for TypeString)
	vRaw  uint64                // Raw data: int, float bits, or child node index
	next  int32                 // Index of next sibling node (-1 = no next)
	vType ValueType             // Type of value stored
}

// Key returns the key as a string (for object nodes).
func (n *Node) Key() string {
	return GetString(n.key)
}

// Type returns the value type.
func (n *Node) Type() ValueType {
	return n.vType
}

// Next returns the index of the next sibling node.
func (n *Node) Next() int32 {
	return n.next
}

// SetNext sets the next sibling index.
func (n *Node) SetNext(idx int32) {
	n.next = idx
}

// AsInt returns the value as an integer.
func (n *Node) AsInt() int {
	return int(n.vRaw)
}

// AsFloat returns the value as a float64.
func (n *Node) AsFloat() float64 {
	return math.Float64frombits(n.vRaw)
}

// AsBool returns the value as a boolean.
func (n *Node) AsBool() bool {
	return n.vRaw == 1
}

// AsString returns the value as a string.
func (n *Node) AsString() string {
	return GetString(n.vStr)
}

// AsChildIndex returns the child node index (for objects/arrays).
func (n *Node) AsChildIndex() int32 {
	return int32(n.vRaw)
}

// SetInt sets the node value to an integer.
func (n *Node) SetInt(v int) {
	n.vType = TypeInt
	n.vRaw = uint64(v)
	n.vStr = EmptyHandle()
}

// SetFloat sets the node value to a float64.
func (n *Node) SetFloat(v float64) {
	n.vType = TypeFloat
	n.vRaw = math.Float64bits(v)
	n.vStr = EmptyHandle()
}

// SetBool sets the node value to a boolean.
func (n *Node) SetBool(v bool) {
	n.vType = TypeBool
	if v {
		n.vRaw = 1
	} else {
		n.vRaw = 0
	}
	n.vStr = EmptyHandle()
}

// SetString sets the node value to a string (interned).
func (n *Node) SetString(v string) {
	n.vType = TypeString
	n.vStr = InternString(v)
	n.vRaw = 0
}

// SetNull sets the node value to null.
func (n *Node) SetNull() {
	n.vType = TypeNull
	n.vRaw = 0
	n.vStr = EmptyHandle()
}

// SetObject sets the node to an object with the given child index.
func (n *Node) SetObject(childIdx int32) {
	n.vType = TypeObject
	n.vRaw = uint64(childIdx)
	n.vStr = EmptyHandle()
}

// SetArray sets the node to an array with the given child index.
func (n *Node) SetArray(childIdx int32) {
	n.vType = TypeArray
	n.vRaw = uint64(childIdx)
	n.vStr = EmptyHandle()
}

// SetKey sets the node's key (for object entries).
func (n *Node) SetKey(key string) {
	n.key = InternString(key)
}

// Reset clears the node for reuse.
func (n *Node) Reset() {
	n.key = EmptyHandle()
	n.vStr = EmptyHandle()
	n.vRaw = 0
	n.next = -1
	n.vType = TypeFree
}

// MarkTombstone marks the node for deletion.
func (n *Node) MarkTombstone() {
	n.vType = TypeTombstone
}

// IsFree returns true if the node is free.
func (n *Node) IsFree() bool {
	return n.vType == TypeFree
}

// IsTombstone returns true if the node is marked for deletion.
func (n *Node) IsTombstone() bool {
	return n.vType == TypeTombstone
}

// ToInterface converts the node value to a Go interface{}.
func (n *Node) ToInterface(a *Arena) any {
	switch n.vType {
	case TypeInt:
		return n.AsInt()
	case TypeFloat:
		return n.AsFloat()
	case TypeBool:
		return n.AsBool()
	case TypeString:
		return n.AsString()
	case TypeObject:
		return n.objectToMap(a)
	case TypeArray:
		return n.arrayToSlice(a)
	case TypeNull:
		return nil
	default:
		return nil
	}
}

// objectToMap converts an object node chain to map[string]any.
func (n *Node) objectToMap(a *Arena) map[string]any {
	// Preallocate with capacity hint to reduce map reallocations
	// Most objects have < 32 fields (user records, config objects, etc.)
	// This reduces bucket reallocation: 0→8→16→32 becomes just 32
	result := make(map[string]any, 32)
	idx := n.AsChildIndex()

	for idx != -1 {
		child := a.getNode(idx)
		if child.vType != TypeTombstone {
			result[child.Key()] = child.ToInterface(a)
		}
		idx = child.next
	}

	return result
}

// arrayToSlice converts an array node chain to []any.
func (n *Node) arrayToSlice(a *Arena) []any {
	// Single-pass optimization: use reasonable initial capacity
	// This avoids double iteration while minimizing reallocations
	// Most arrays have < 32 elements, so this is a good trade-off
	result := make([]any, 0, 32)
	idx := n.AsChildIndex()

	for idx != -1 {
		child := a.getNode(idx)
		if child.vType != TypeTombstone {
			result = append(result, child.ToInterface(a))
		}
		idx = child.next
	}

	return result
}

// PathLookup performs a path lookup starting from this node.
// Returns the target node and true if found, nil and false otherwise.
func (n *Node) PathLookup(a *Arena, path []string) (*Node, bool) {
	if len(path) == 0 {
		return n, true
	}

	switch n.vType {
	case TypeObject:
		return n.objectLookup(a, path)
	case TypeArray:
		return n.arrayLookup(a, path)
	default:
		return nil, false
	}
}

// objectLookup performs lookup in an object node.
func (n *Node) objectLookup(a *Arena, path []string) (*Node, bool) {
	keyHandle := InternString(path[0])
	idx := n.AsChildIndex()

	for idx != -1 {
		child := a.getNode(idx)
		if child.vType != TypeTombstone && child.key == keyHandle {
			if len(path) == 1 {
				return child, true
			}
			return child.PathLookup(a, path[1:])
		}
		idx = child.next
	}

	return nil, false
}

// arrayLookup performs lookup in an array node.
func (n *Node) arrayLookup(a *Arena, path []string) (*Node, bool) {
	pos, err := strconv.Atoi(path[0])
	if err != nil || pos < 0 {
		return nil, false
	}

	idx := n.AsChildIndex()
	current := 0

	for idx != -1 {
		child := a.getNode(idx)
		if child.vType != TypeTombstone {
			if current == pos {
				if len(path) == 1 {
					return child, true
				}
				return child.PathLookup(a, path[1:])
			}
			current++
		}
		idx = child.next
	}

	return nil, false
}
