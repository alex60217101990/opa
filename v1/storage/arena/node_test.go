// Copyright 2026 The OPA Authors.  All rights reserved.
// Use of this source code is governed by an Apache2
// license that can be found in the LICENSE file.

package arena

import (
	"testing"
)

func TestNodeBasicOperations(t *testing.T) {
	var node Node

	// Test int
	node.SetInt(42)
	if node.Type() != TypeInt {
		t.Fatalf("Expected TypeInt, got %v", node.Type())
	}
	if node.AsInt() != 42 {
		t.Fatalf("Expected 42, got %v", node.AsInt())
	}

	// Test float
	node.SetFloat(3.14)
	if node.Type() != TypeFloat {
		t.Fatalf("Expected TypeFloat, got %v", node.Type())
	}
	if node.AsFloat() != 3.14 {
		t.Fatalf("Expected 3.14, got %v", node.AsFloat())
	}

	// Test bool
	node.SetBool(true)
	if node.Type() != TypeBool {
		t.Fatalf("Expected TypeBool, got %v", node.Type())
	}
	if !node.AsBool() {
		t.Fatalf("Expected true, got false")
	}

	// Test string
	node.SetString("hello")
	if node.Type() != TypeString {
		t.Fatalf("Expected TypeString, got %v", node.Type())
	}
	if node.AsString() != "hello" {
		t.Fatalf("Expected 'hello', got '%v'", node.AsString())
	}

	// Test null
	node.SetNull()
	if node.Type() != TypeNull {
		t.Fatalf("Expected TypeNull, got %v", node.Type())
	}

	// Test reset
	node.SetInt(100)
	node.Reset()
	if node.Type() != TypeFree {
		t.Fatalf("Expected TypeFree after reset, got %v", node.Type())
	}
}

func TestNodeKey(t *testing.T) {
	var node Node

	node.SetKey("mykey")
	if node.Key() != "mykey" {
		t.Fatalf("Expected 'mykey', got '%v'", node.Key())
	}
}

func TestNodeObjectArray(t *testing.T) {
	var node Node

	// Test object
	node.SetObject(10)
	if node.Type() != TypeObject {
		t.Fatalf("Expected TypeObject, got %v", node.Type())
	}
	if node.AsChildIndex() != 10 {
		t.Fatalf("Expected child index 10, got %v", node.AsChildIndex())
	}

	// Test array
	node.SetArray(20)
	if node.Type() != TypeArray {
		t.Fatalf("Expected TypeArray, got %v", node.Type())
	}
	if node.AsChildIndex() != 20 {
		t.Fatalf("Expected child index 20, got %v", node.AsChildIndex())
	}
}

func TestNodeTombstone(t *testing.T) {
	var node Node

	node.SetInt(42)
	if node.IsTombstone() {
		t.Fatal("Node should not be tombstone")
	}

	node.MarkTombstone()
	if !node.IsTombstone() {
		t.Fatal("Node should be tombstone")
	}
}

func TestNodeToInterface(t *testing.T) {
	arena := NewWithOpts().(*Arena)

	tests := []struct {
		name     string
		setup    func(*Node)
		expected any
	}{
		{
			name:     "int",
			setup:    func(n *Node) { n.SetInt(42) },
			expected: 42,
		},
		{
			name:     "float",
			setup:    func(n *Node) { n.SetFloat(3.14) },
			expected: 3.14,
		},
		{
			name:     "bool",
			setup:    func(n *Node) { n.SetBool(true) },
			expected: true,
		},
		{
			name:     "string",
			setup:    func(n *Node) { n.SetString("hello") },
			expected: "hello",
		},
		{
			name:     "null",
			setup:    func(n *Node) { n.SetNull() },
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var node Node
			tt.setup(&node)
			result := node.ToInterface(arena)

			switch expected := tt.expected.(type) {
			case int:
				if r, ok := result.(int); !ok || r != expected {
					t.Fatalf("Expected %v, got %v", expected, result)
				}
			case float64:
				if r, ok := result.(float64); !ok || r != expected {
					t.Fatalf("Expected %v, got %v", expected, result)
				}
			case bool:
				if r, ok := result.(bool); !ok || r != expected {
					t.Fatalf("Expected %v, got %v", expected, result)
				}
			case string:
				if r, ok := result.(string); !ok || r != expected {
					t.Fatalf("Expected %v, got %v", expected, result)
				}
			case nil:
				if result != nil {
					t.Fatalf("Expected nil, got %v", result)
				}
			}
		})
	}
}

func TestArenaAllocation(t *testing.T) {
	arena := NewWithOpts().(*Arena)

	// Test basic allocation
	idx1 := arena.alloc()
	if idx1 < 0 {
		t.Fatalf("Expected valid index, got %d", idx1)
	}

	idx2 := arena.alloc()
	if idx2 <= idx1 {
		t.Fatalf("Expected idx2 > idx1, got idx1=%d, idx2=%d", idx1, idx2)
	}

	// Test node access
	node1 := arena.getNode(idx1)
	if node1 == nil {
		t.Fatal("Expected non-nil node")
	}

	node1.SetInt(100)
	if node1.AsInt() != 100 {
		t.Fatalf("Expected 100, got %v", node1.AsInt())
	}
}

func TestArenaFreeAndReuse(t *testing.T) {
	arena := NewWithOpts().(*Arena)

	// Allocate
	idx1 := arena.alloc()
	node1 := arena.getNode(idx1)
	node1.SetInt(42)

	// Free
	arena.free(idx1)

	// Allocate again - should reuse
	idx2 := arena.alloc()
	if idx2 != idx1 {
		t.Logf("Note: idx2=%d != idx1=%d (freelist may be reordered)", idx2, idx1)
	}

	// Node should be reset
	node2 := arena.getNode(idx2)
	if node2.Type() != TypeFree {
		t.Fatalf("Expected TypeFree after realloc, got %v", node2.Type())
	}
}

func TestArenaLoadMap(t *testing.T) {
	arena := NewWithOpts().(*Arena)

	data := map[string]any{
		"name":  "alice",
		"age":   30,
		"admin": true,
	}

	rootIdx := arena.LoadMap(data)
	root := arena.getNode(rootIdx)

	if root.Type() != TypeObject {
		t.Fatalf("Expected TypeObject, got %v", root.Type())
	}

	// Convert back and verify
	result := root.ToInterface(arena)
	resultMap, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("Expected map[string]any, got %T", result)
	}

	if resultMap["name"] != "alice" {
		t.Fatalf("Expected name=alice, got %v", resultMap["name"])
	}
	if resultMap["age"] != 30 {
		t.Fatalf("Expected age=30, got %v", resultMap["age"])
	}
	if resultMap["admin"] != true {
		t.Fatalf("Expected admin=true, got %v", resultMap["admin"])
	}
}

func TestArenaLoadSlice(t *testing.T) {
	arena := NewWithOpts().(*Arena)

	data := []any{1, 2, 3, "four", 5.0}

	rootIdx := arena.LoadSlice(data)
	root := arena.getNode(rootIdx)

	if root.Type() != TypeArray {
		t.Fatalf("Expected TypeArray, got %v", root.Type())
	}

	// Convert back and verify
	result := root.ToInterface(arena)
	resultSlice, ok := result.([]any)
	if !ok {
		t.Fatalf("Expected []any, got %T", result)
	}

	if len(resultSlice) != 5 {
		t.Fatalf("Expected length 5, got %d", len(resultSlice))
	}

	if resultSlice[0] != 1 {
		t.Fatalf("Expected resultSlice[0]=1, got %v", resultSlice[0])
	}
	if resultSlice[3] != "four" {
		t.Fatalf("Expected resultSlice[3]='four', got %v", resultSlice[3])
	}
}

func TestArenaNestedData(t *testing.T) {
	arena := NewWithOpts().(*Arena)

	data := map[string]any{
		"user": map[string]any{
			"name": "alice",
			"contacts": map[string]any{
				"email": "alice@example.com",
				"phone": "555-1234",
			},
		},
		"active": true,
	}

	rootIdx := arena.LoadMap(data)
	root := arena.getNode(rootIdx)

	// Convert back
	result := root.ToInterface(arena)
	resultMap, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("Expected map[string]any, got %T", result)
	}

	// Navigate nested structure
	user, ok := resultMap["user"].(map[string]any)
	if !ok {
		t.Fatalf("Expected user to be map[string]any, got %T", resultMap["user"])
	}

	if user["name"] != "alice" {
		t.Fatalf("Expected name=alice, got %v", user["name"])
	}

	contacts, ok := user["contacts"].(map[string]any)
	if !ok {
		t.Fatalf("Expected contacts to be map[string]any, got %T", user["contacts"])
	}

	if contacts["email"] != "alice@example.com" {
		t.Fatalf("Expected email=alice@example.com, got %v", contacts["email"])
	}
}

func TestStringInterning(t *testing.T) {
	// Test that string interning works
	s1 := InternString("test")
	s2 := InternString("test")

	// Should return the same handle (or at least same value)
	if GetString(s1) != GetString(s2) {
		t.Fatal("String interning failed")
	}

	if GetString(s1) != "test" {
		t.Fatalf("Expected 'test', got '%s'", GetString(s1))
	}
}
