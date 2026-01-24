// Copyright 2026 The OPA Authors.  All rights reserved.
// Use of this source code is governed by an Apache2
// license that can be found in the LICENSE file.

package ast

import (
	"fmt"
	"testing"
)

// TestInterfaceToValue_Correctness verifies that optimizations don't break logic
func TestInterfaceToValue_Correctness(t *testing.T) {
	tests := []struct {
		name     string
		input    any
		validate func(*testing.T, Value)
	}{
		{
			name:  "empty slice",
			input: []any{},
			validate: func(t *testing.T, v Value) {
				arr, ok := v.(*Array)
				if !ok {
					t.Fatalf("expected *Array, got %T", v)
				}
				if arr.Len() != 0 {
					t.Errorf("expected length 0, got %d", arr.Len())
				}
				if !arr.IsGround() {
					t.Error("empty array should be ground")
				}
			},
		},
		{
			name:  "empty string slice",
			input: []string{},
			validate: func(t *testing.T, v Value) {
				arr, ok := v.(*Array)
				if !ok {
					t.Fatalf("expected *Array, got %T", v)
				}
				if arr.Len() != 0 {
					t.Errorf("expected length 0, got %d", arr.Len())
				}
			},
		},
		{
			name:  "single element slice",
			input: []any{42},
			validate: func(t *testing.T, v Value) {
				arr, ok := v.(*Array)
				if !ok {
					t.Fatalf("expected *Array, got %T", v)
				}
				if arr.Len() != 1 {
					t.Fatalf("expected length 1, got %d", arr.Len())
				}
				elem := arr.Elem(0)
				num, ok := elem.Value.(Number)
				if !ok {
					t.Fatalf("expected Number, got %T", elem.Value)
				}
				if num.String() != "42" {
					t.Errorf("expected 42, got %s", num.String())
				}
			},
		},
		{
			name:  "mixed type slice",
			input: []any{1, "test", true, 3.14},
			validate: func(t *testing.T, v Value) {
				arr, ok := v.(*Array)
				if !ok {
					t.Fatalf("expected *Array, got %T", v)
				}
				if arr.Len() != 4 {
					t.Fatalf("expected length 4, got %d", arr.Len())
				}
				// Check each element type
				if _, ok := arr.Elem(0).Value.(Number); !ok {
					t.Error("element 0 should be Number")
				}
				if _, ok := arr.Elem(1).Value.(String); !ok {
					t.Error("element 1 should be String")
				}
				if _, ok := arr.Elem(2).Value.(Boolean); !ok {
					t.Error("element 2 should be Boolean")
				}
				if _, ok := arr.Elem(3).Value.(Number); !ok {
					t.Error("element 3 should be Number")
				}
			},
		},
		{
			name:  "string slice",
			input: []string{"a", "b", "c"},
			validate: func(t *testing.T, v Value) {
				arr, ok := v.(*Array)
				if !ok {
					t.Fatalf("expected *Array, got %T", v)
				}
				if arr.Len() != 3 {
					t.Fatalf("expected length 3, got %d", arr.Len())
				}
				expected := []string{"a", "b", "c"}
				for i, exp := range expected {
					str, ok := arr.Elem(i).Value.(String)
					if !ok {
						t.Fatalf("element %d: expected String, got %T", i, arr.Elem(i).Value)
					}
					if string(str) != exp {
						t.Errorf("element %d: expected %q, got %q", i, exp, string(str))
					}
				}
			},
		},
		{
			name:  "empty map",
			input: map[string]any{},
			validate: func(t *testing.T, v Value) {
				obj, ok := v.(*object)
				if !ok {
					t.Fatalf("expected *object, got %T", v)
				}
				if obj.Len() != 0 {
					t.Errorf("expected length 0, got %d", obj.Len())
				}
				if !obj.IsGround() {
					t.Error("empty object should be ground")
				}
			},
		},
		{
			name:  "small map (below threshold)",
			input: map[string]any{"a": 1, "b": 2, "c": 3},
			validate: func(t *testing.T, v Value) {
				obj, ok := v.(*object)
				if !ok {
					t.Fatalf("expected *object, got %T", v)
				}
				if obj.Len() != 3 {
					t.Fatalf("expected length 3, got %d", obj.Len())
				}
				// Verify all keys exist
				for _, key := range []string{"a", "b", "c"} {
					val := obj.Get(StringTerm(key))
					if val == nil {
						t.Errorf("key %q not found", key)
					}
				}
			},
		},
		{
			name: "large map (above threshold)",
			input: func() map[string]any {
				m := make(map[string]any, 20)
				for i := range 20 {
					m[fmt.Sprintf("key_%d", i)] = i
				}
				return m
			}(),
			validate: func(t *testing.T, v Value) {
				obj, ok := v.(*object)
				if !ok {
					t.Fatalf("expected *object, got %T", v)
				}
				if obj.Len() != 20 {
					t.Fatalf("expected length 20, got %d", obj.Len())
				}
				// Verify all keys exist and have correct values
				for i := range 20 {
					key := fmt.Sprintf("key_%d", i)
					val := obj.Get(StringTerm(key))
					if val == nil {
						t.Errorf("key %q not found", key)
						continue
					}
					num, ok := val.Value.(Number)
					if !ok {
						t.Errorf("key %q: expected Number, got %T", key, val.Value)
						continue
					}
					if num.String() != fmt.Sprintf("%d", i) {
						t.Errorf("key %q: expected %d, got %s", key, i, num.String())
					}
				}
			},
		},
		{
			name:  "typed map string",
			input: map[string]string{"x": "foo", "y": "bar"},
			validate: func(t *testing.T, v Value) {
				obj, ok := v.(*object)
				if !ok {
					t.Fatalf("expected *object, got %T", v)
				}
				if obj.Len() != 2 {
					t.Fatalf("expected length 2, got %d", obj.Len())
				}
				xVal := obj.Get(StringTerm("x"))
				if xVal == nil {
					t.Fatal("key 'x' not found")
				}
				if str, ok := xVal.Value.(String); !ok || string(str) != "foo" {
					t.Errorf("expected 'foo', got %v", xVal.Value)
				}
			},
		},
		{
			name:  "typed map int",
			input: map[string]int{"a": 10, "b": 20},
			validate: func(t *testing.T, v Value) {
				obj, ok := v.(*object)
				if !ok {
					t.Fatalf("expected *object, got %T", v)
				}
				if obj.Len() != 2 {
					t.Fatalf("expected length 2, got %d", obj.Len())
				}
			},
		},
		{
			name:  "typed map bool",
			input: map[string]bool{"t": true, "f": false},
			validate: func(t *testing.T, v Value) {
				obj, ok := v.(*object)
				if !ok {
					t.Fatalf("expected *object, got %T", v)
				}
				if obj.Len() != 2 {
					t.Fatalf("expected length 2, got %d", obj.Len())
				}
				tVal := obj.Get(StringTerm("t"))
				if tVal == nil {
					t.Fatal("key 't' not found")
				}
				if b, ok := tVal.Value.(Boolean); !ok || !bool(b) {
					t.Errorf("expected true, got %v", tVal.Value)
				}
			},
		},
		{
			name: "nested structures",
			input: map[string]any{
				"array": []any{1, 2, 3},
				"map": map[string]any{
					"nested": "value",
				},
			},
			validate: func(t *testing.T, v Value) {
				obj, ok := v.(*object)
				if !ok {
					t.Fatalf("expected *object, got %T", v)
				}
				if obj.Len() != 2 {
					t.Fatalf("expected length 2, got %d", obj.Len())
				}
				// Check array
				arrVal := obj.Get(StringTerm("array"))
				if arrVal == nil {
					t.Fatal("key 'array' not found")
				}
				arr, ok := arrVal.Value.(*Array)
				if !ok {
					t.Fatalf("expected *Array, got %T", arrVal.Value)
				}
				if arr.Len() != 3 {
					t.Errorf("array: expected length 3, got %d", arr.Len())
				}
				// Check nested map
				mapVal := obj.Get(StringTerm("map"))
				if mapVal == nil {
					t.Fatal("key 'map' not found")
				}
				nestedObj, ok := mapVal.Value.(*object)
				if !ok {
					t.Fatalf("expected *object, got %T", mapVal.Value)
				}
				nestedVal := nestedObj.Get(StringTerm("nested"))
				if nestedVal == nil {
					t.Fatal("nested key 'nested' not found")
				}
			},
		},
		{
			name: "slice with nested maps",
			input: []any{
				map[string]any{"id": 1, "name": "Alice"},
				map[string]any{"id": 2, "name": "Bob"},
			},
			validate: func(t *testing.T, v Value) {
				arr, ok := v.(*Array)
				if !ok {
					t.Fatalf("expected *Array, got %T", v)
				}
				if arr.Len() != 2 {
					t.Fatalf("expected length 2, got %d", arr.Len())
				}
				// Check first element
				obj0, ok := arr.Elem(0).Value.(*object)
				if !ok {
					t.Fatalf("element 0: expected *object, got %T", arr.Elem(0).Value)
				}
				if obj0.Len() != 2 {
					t.Errorf("element 0: expected length 2, got %d", obj0.Len())
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := InterfaceToValue(tt.input)
			if err != nil {
				t.Fatalf("InterfaceToValue failed: %v", err)
			}
			if result == nil {
				t.Fatal("InterfaceToValue returned nil")
			}
			tt.validate(t, result)
		})
	}
}

// TestInterfaceToValue_HashConsistency verifies that hashes are computed correctly
func TestInterfaceToValue_HashConsistency(t *testing.T) {
	tests := []struct {
		name   string
		input1 any
		input2 any
		equal  bool
	}{
		{
			name:   "identical slices",
			input1: []any{1, 2, 3},
			input2: []any{1, 2, 3},
			equal:  true,
		},
		{
			name:   "different slices",
			input1: []any{1, 2, 3},
			input2: []any{1, 2, 4},
			equal:  false,
		},
		{
			name:   "identical string slices",
			input1: []string{"a", "b", "c"},
			input2: []string{"a", "b", "c"},
			equal:  true,
		},
		{
			name:   "identical maps",
			input1: map[string]any{"x": 1, "y": 2},
			input2: map[string]any{"x": 1, "y": 2},
			equal:  true,
		},
		{
			name:   "different maps",
			input1: map[string]any{"x": 1, "y": 2},
			input2: map[string]any{"x": 1, "y": 3},
			equal:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v1, err := InterfaceToValue(tt.input1)
			if err != nil {
				t.Fatalf("InterfaceToValue(input1) failed: %v", err)
			}
			v2, err := InterfaceToValue(tt.input2)
			if err != nil {
				t.Fatalf("InterfaceToValue(input2) failed: %v", err)
			}

			hash1 := v1.Hash()
			hash2 := v2.Hash()

			if tt.equal {
				if hash1 != hash2 {
					t.Errorf("expected equal hashes, got %d != %d", hash1, hash2)
				}
				if v1.Compare(v2) != 0 {
					t.Error("values should be equal")
				}
			} else {
				// Different values might have same hash (collision), but should not be equal
				if v1.Compare(v2) == 0 {
					t.Error("values should not be equal")
				}
			}
		})
	}
}

// TestInterfaceToValue_GroundFlag verifies that ground flag is computed correctly
func TestInterfaceToValue_GroundFlag(t *testing.T) {
	tests := []struct {
		name    string
		input   any
		isGround bool
	}{
		{
			name:     "empty slice",
			input:    []any{},
			isGround: true,
		},
		{
			name:     "ground slice",
			input:    []any{1, "test", true},
			isGround: true,
		},
		{
			name:     "string slice always ground",
			input:    []string{"a", "b"},
			isGround: true,
		},
		{
			name:     "empty map",
			input:    map[string]any{},
			isGround: true,
		},
		{
			name:     "ground map",
			input:    map[string]any{"x": 1, "y": "test"},
			isGround: true,
		},
		{
			name:     "typed string map",
			input:    map[string]string{"a": "b"},
			isGround: true,
		},
		{
			name:     "typed int map",
			input:    map[string]int{"a": 1},
			isGround: true,
		},
		{
			name:     "typed bool map",
			input:    map[string]bool{"a": true},
			isGround: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := InterfaceToValue(tt.input)
			if err != nil {
				t.Fatalf("InterfaceToValue failed: %v", err)
			}
			if result.IsGround() != tt.isGround {
				t.Errorf("expected IsGround() = %v, got %v", tt.isGround, result.IsGround())
			}
		})
	}
}
