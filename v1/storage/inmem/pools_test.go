// Copyright 2017 The OPA Authors.  All rights reserved.
// Use of this source code is governed by an Apache2
// license that can be found in the LICENSE file.

package inmem

import (
	"testing"
)

// TestPathIndexPool verifies pathIndex pool behavior.
func TestPathIndexPool(t *testing.T) {
	t.Run("get_returns_empty_map", func(t *testing.T) {
		m := getPathIndex()
		if m == nil {
			t.Fatal("getPathIndex returned nil")
		}
		if len(m) != 0 {
			t.Errorf("expected empty map, got %d entries", len(m))
		}
	})

	t.Run("put_clears_map", func(t *testing.T) {
		m := getPathIndex()
		m["test1"] = 1
		m["test2"] = 2

		putPathIndex(m)

		// Get it again - should be cleared
		m2 := getPathIndex()
		if len(m2) != 0 {
			t.Errorf("expected cleared map from pool, got %d entries", len(m2))
		}
	})

	t.Run("put_nil_is_safe", func(t *testing.T) {
		// Should not panic
		putPathIndex(nil)
	})

	t.Run("concurrent_usage", func(t *testing.T) {
		// Simulate multiple transactions using pool concurrently
		done := make(chan bool, 10)
		for range 10 {
			go func() {
				m := getPathIndex()
				m["key"] = 1
				putPathIndex(m)
				done <- true
			}()
		}
		for range 10 {
			<-done
		}
	})
}

// TestIntSlicePool verifies intSlice pool behavior.
func TestIntSlicePool(t *testing.T) {
	t.Run("get_returns_empty_slice", func(t *testing.T) {
		s := getIntSlice()
		if s == nil {
			t.Fatal("getIntSlice returned nil")
		}
		if len(*s) != 0 {
			t.Errorf("expected empty slice, got length %d", len(*s))
		}
		if cap(*s) == 0 {
			t.Error("expected non-zero capacity")
		}
	})

	t.Run("put_resets_length", func(t *testing.T) {
		s := getIntSlice()
		*s = append(*s, 1, 2, 3)

		putIntSlice(s)

		// Get it again - should be empty but preserve capacity
		s2 := getIntSlice()
		if len(*s2) != 0 {
			t.Errorf("expected empty slice from pool, got length %d", len(*s2))
		}
	})

	t.Run("put_nil_is_safe", func(t *testing.T) {
		// Should not panic
		putIntSlice(nil)
	})

	t.Run("concurrent_usage", func(t *testing.T) {
		// Simulate multiple Write() calls using pool concurrently
		done := make(chan bool, 10)
		for range 10 {
			go func() {
				s := getIntSlice()
				*s = append(*s, 1, 2, 3)
				putIntSlice(s)
				done <- true
			}()
		}
		for range 10 {
			<-done
		}
	})
}

// TestPoolIntegration verifies pools work correctly in transaction workflow.
func TestPoolIntegration(t *testing.T) {
	t.Run("pathIndex_lifecycle", func(t *testing.T) {
		// Simulate transaction creating large pathIndex
		m := getPathIndex()

		// Populate like initPathIndex() does
		for i := range 100 {
			m[string(rune(i))] = i
		}

		if len(m) != 100 {
			t.Errorf("expected 100 entries, got %d", len(m))
		}

		// Return to pool (like Commit() does)
		putPathIndex(m)

		// Next transaction should get cleared map
		m2 := getPathIndex()
		if len(m2) != 0 {
			t.Errorf("pool not cleared: expected 0 entries, got %d", len(m2))
		}

		putPathIndex(m2)
	})

	t.Run("intSlice_lifecycle", func(t *testing.T) {
		// Simulate Write() collecting indices to remove
		s := getIntSlice()

		// Append indices like prefix checking does
		*s = append(*s, 10, 20, 30)

		if len(*s) != 3 {
			t.Errorf("expected 3 elements, got %d", len(*s))
		}

		// Return to pool (deferred in Write())
		putIntSlice(s)

		// Next Write() should get empty slice
		s2 := getIntSlice()
		if len(*s2) != 0 {
			t.Errorf("pool not reset: expected length 0, got %d", len(*s2))
		}

		putIntSlice(s2)
	})
}
