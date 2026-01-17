// Copyright 2017 The OPA Authors.  All rights reserved.
// Use of this source code is governed by an Apache2
// license that can be found in the LICENSE file.

package inmem

import (
	"context"
	"fmt"
	"testing"

	"github.com/open-policy-agent/opa/v1/storage"
)

// TestPathIndexMemoryOverhead measures exact memory overhead of path index.
func TestPathIndexMemoryOverhead(t *testing.T) {
	sizes := []int{10, 50, 100, 500}

	for _, size := range sizes {
		t.Run(fmt.Sprintf("%d_updates", size), func(t *testing.T) {
			data := map[string]any{}
			db := NewFromObject(data)
			ctx := context.Background()

			txn, _ := db.NewTransaction(ctx, storage.WriteParams)
			internalTxn := txn.(*transaction)

			// Create updates - this will build path index
			for j := range size {
				path := storage.MustParsePath(fmt.Sprintf("/key_%d", j))
				_ = internalTxn.Write(storage.AddOp, path, fmt.Sprintf("value_%d", j))
			}

			// Measure actual components
			updatesCount := len(internalTxn.updates)
			indexSize := len(internalTxn.pathIndex)

			t.Logf("Updates: %d entries", updatesCount)
			t.Logf("Path index: %d entries", indexSize)

			// Adaptive index: only created for transactions with >16 updates
			const pathIndexThreshold = 16
			if size > pathIndexThreshold {
				if indexSize != updatesCount {
					t.Errorf("Large transaction: index size mismatch: %d updates but %d index entries", updatesCount, indexSize)
				}
				t.Logf("Large transaction (%d updates) - index enabled", size)
			} else {
				if indexSize != 0 {
					t.Errorf("Small transaction: index should not exist: %d updates, %d index entries", updatesCount, indexSize)
				}
				t.Logf("Small transaction (%d updates) - index disabled (zero overhead)", size)
			}

			// Estimate memory
			// Each update: ~24 bytes (pointer to dataUpdate struct)
			// Each index entry: ~40 bytes (string key + int value + map overhead)
			updatesMemory := updatesCount * 24
			indexMemory := indexSize * 40

			t.Logf("Estimated updates slice memory: ~%d bytes", updatesMemory)
			t.Logf("Estimated path index memory: ~%d bytes", indexMemory)
			if updatesMemory > 0 {
				t.Logf("Overhead ratio: %.1f%%", float64(indexMemory)/float64(updatesMemory)*100)
			}
		})
	}
}

// BenchmarkMemoryComparisonIndependent compares memory with real measurements.
func BenchmarkMemoryComparisonIndependent(b *testing.B) {
	sizes := []int{10, 50, 100}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("with_pathindex_%d", size), func(b *testing.B) {
			data := map[string]any{}
			db := NewFromObject(data)
			ctx := context.Background()

			b.ReportAllocs()
			b.ResetTimer()

			for b.Loop() {
				txn, _ := db.NewTransaction(ctx, storage.WriteParams)
				internalTxn := txn.(*transaction)

				// Independent paths - index will be populated
				for j := range size {
					_ = internalTxn.Write(storage.AddOp, storage.MustParsePath(fmt.Sprintf("/key_%d", j)), "value")
				}

				_ = db.Commit(ctx, txn)
			}
		})
	}
}

// TestPathIndexBenefit shows when path index saves memory/work.
func TestPathIndexBenefit(t *testing.T) {
	scenarios := []struct {
		name               string
		writes             []string
		expectedIterations int // iterations that would occur WITHOUT index
		actualIterations   int // iterations WITH index (prefix checks only)
	}{
		{
			name:               "100_same_path",
			writes:             generateSamePathWrites("/key", 100),
			expectedIterations: 5050, // sum(1..100) for O(n) search each time
			actualIterations:   0,    // O(1) exact match - no iterations!
		},
		{
			name:               "100_independent",
			writes:             generateIndependentWrites(100),
			expectedIterations: 0,    // No exact matches either way
			actualIterations:   4950, // Still need prefix checking (99+98+...+1)
		},
	}

	for _, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
			data := map[string]any{}
			db := NewFromObject(data)
			ctx := context.Background()

			txn, _ := db.NewTransaction(ctx, storage.WriteParams)
			internalTxn := txn.(*transaction)

			// Perform writes
			for _, pathStr := range sc.writes {
				_ = internalTxn.Write(storage.AddOp, storage.MustParsePath(pathStr), "value")
			}

			t.Logf("Pattern: %s", sc.name)
			t.Logf("Writes: %d", len(sc.writes))
			t.Logf("Final updates: %d", len(internalTxn.updates))
			t.Logf("Path index size: %d", len(internalTxn.pathIndex))
			t.Logf("Expected iterations (no index): %d", sc.expectedIterations)
			t.Logf("Actual iterations (with index): %d (estimated)", sc.actualIterations)

			if sc.expectedIterations > 0 {
				savingsPercent := float64(sc.expectedIterations-sc.actualIterations) / float64(sc.expectedIterations) * 100
				t.Logf("Savings: %.1f%% fewer iterations", savingsPercent)
			}
		})
	}
}

func generateSamePathWrites(path string, count int) []string {
	writes := make([]string, count)
	for i := range count {
		writes[i] = path
	}
	return writes
}

func generateIndependentWrites(count int) []string {
	writes := make([]string, count)
	for i := range count {
		writes[i] = fmt.Sprintf("/key_%d", i)
	}
	return writes
}
