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

// BenchmarkWritePathIndex benchmarks Write() performance with path index optimization.
// This measures the improvement from O(n) to O(1) exact path lookups.
func BenchmarkWritePathIndex(b *testing.B) {
	patterns := []struct {
		name        string
		numWrites   int
		pathPattern string // "same", "independent", "nested"
	}{
		// Same path overwrites - benefits most from O(1) lookup
		{"same_path_10_writes", 10, "same"},
		{"same_path_50_writes", 50, "same"},
		{"same_path_100_writes", 100, "same"},
		{"same_path_500_writes", 500, "same"},
		{"same_path_1000_writes", 1000, "same"},

		// Independent paths - still has prefix checking overhead
		{"independent_10_writes", 10, "independent"},
		{"independent_50_writes", 50, "independent"},
		{"independent_100_writes", 100, "independent"},
		{"independent_500_writes", 500, "independent"},

		// Nested paths - mix of exact and prefix operations
		{"nested_10_writes", 10, "nested"},
		{"nested_50_writes", 50, "nested"},
		{"nested_100_writes", 100, "nested"},
	}

	for _, pattern := range patterns {
		b.Run(pattern.name, func(b *testing.B) {
			data := map[string]any{}
			db := NewFromObject(data)
			ctx := context.Background()

			b.ReportAllocs()
			b.ResetTimer()

			for b.Loop() {
				txn, _ := db.NewTransaction(ctx, storage.WriteParams)
				internalTxn := txn.(*transaction)

				switch pattern.pathPattern {
				case "same":
					// All writes to same path - O(1) lookup every time
					path := storage.MustParsePath("/key")
					for j := range pattern.numWrites {
						_ = internalTxn.Write(storage.AddOp, path, fmt.Sprintf("value_%d", j))
					}

				case "independent":
					// All writes to different paths - O(1) lookup (no match) + prefix checking
					for j := range pattern.numWrites {
						path := storage.MustParsePath(fmt.Sprintf("/key_%d", j))
						_ = internalTxn.Write(storage.AddOp, path, fmt.Sprintf("value_%d", j))
					}

				case "nested":
					// Mix of parent and child writes
					_ = internalTxn.Write(storage.AddOp, storage.MustParsePath("/root"), map[string]any{})
					for j := range pattern.numWrites {
						path := storage.MustParsePath(fmt.Sprintf("/root/key_%d", j))
						_ = internalTxn.Write(storage.AddOp, path, fmt.Sprintf("value_%d", j))
					}
				}

				_ = db.Commit(ctx, txn)
			}
		})
	}
}

// BenchmarkWriteScaling measures how Write() performance scales with transaction size.
func BenchmarkWriteScaling(b *testing.B) {
	sizes := []int{10, 50, 100, 500, 1000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("%d_updates_then_overwrite", size), func(b *testing.B) {
			data := map[string]any{}
			db := NewFromObject(data)
			ctx := context.Background()

			b.ReportAllocs()
			b.ResetTimer()

			for b.Loop() {
				txn, _ := db.NewTransaction(ctx, storage.WriteParams)
				internalTxn := txn.(*transaction)

				// Create many independent updates
				for j := range size {
					path := storage.MustParsePath(fmt.Sprintf("/key_%d", j))
					_ = internalTxn.Write(storage.AddOp, path, fmt.Sprintf("initial_%d", j))
				}

				// Measure overwriting an existing path - should be O(1) with index
				path := storage.MustParsePath(fmt.Sprintf("/key_%d", size/2))
				_ = internalTxn.Write(storage.AddOp, path, "overwritten")

				_ = db.Commit(ctx, txn)
			}
		})
	}
}

// BenchmarkLargeTransactionWrite simulates writing large maps as individual key writes.
// This is the real-world scenario that motivated the optimization.
func BenchmarkLargeTransactionWrite(b *testing.B) {
	mapSizes := []int{100, 500, 1000, 5000}

	for _, size := range mapSizes {
		b.Run(fmt.Sprintf("%d_element_map_individual_writes", size), func(b *testing.B) {
			data := map[string]any{}
			db := NewFromObject(data)
			ctx := context.Background()

			b.ReportAllocs()
			b.ResetTimer()

			for b.Loop() {
				txn, _ := db.NewTransaction(ctx, storage.WriteParams)
				internalTxn := txn.(*transaction)

				// Write base object
				basePath := storage.MustParsePath("/data")
				_ = internalTxn.Write(storage.AddOp, basePath, map[string]any{})

				// Write each map element as individual update
				for j := range size {
					keyPath := storage.MustParsePath(fmt.Sprintf("/data/key_%d", j))
					_ = internalTxn.Write(storage.AddOp, keyPath, map[string]any{
						"id":    j,
						"value": fmt.Sprintf("item_%d", j),
					})
				}

				_ = db.Commit(ctx, txn)
			}
		})
	}
}

// BenchmarkPathIndexMemory measures memory overhead of path index.
func BenchmarkPathIndexMemory(b *testing.B) {
	sizes := []int{10, 50, 100, 500}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("%d_updates", size), func(b *testing.B) {
			data := map[string]any{}
			db := NewFromObject(data)
			ctx := context.Background()

			b.ReportAllocs()
			b.ResetTimer()

			for b.Loop() {
				txn, _ := db.NewTransaction(ctx, storage.WriteParams)
				internalTxn := txn.(*transaction)

				// Create updates with path index
				for j := range size {
					path := storage.MustParsePath(fmt.Sprintf("/key_%d", j))
					_ = internalTxn.Write(storage.AddOp, path, fmt.Sprintf("value_%d", j))
				}

				_ = db.Commit(ctx, txn)
			}
		})
	}
}
