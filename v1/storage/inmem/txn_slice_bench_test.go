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

// BenchmarkTransactionWrites measures the performance of write operations
// with the slice-based updates implementation.
func BenchmarkTransactionWrites(b *testing.B) {
	benchmarks := []struct {
		name       string
		numUpdates int
	}{
		{"1_update", 1},
		{"5_updates", 5},
		{"10_updates", 10},
		{"20_updates", 20},
		{"50_updates", 50},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			data := map[string]any{}
			db := NewFromObject(data)
			ctx := context.Background()

			b.ResetTimer()
			b.ReportAllocs()

			for b.Loop() {
				txn, err := db.NewTransaction(ctx, storage.WriteParams)
				if err != nil {
					b.Fatal(err)
				}

				internalTxn := txn.(*transaction)

				// Perform writes
				for j := range bm.numUpdates {
					path := storage.MustParsePath(fmt.Sprintf("/key_%d", j))
					if err := internalTxn.Write(storage.AddOp, path, fmt.Sprintf("value_%d", j)); err != nil {
						b.Fatal(err)
					}
				}

				// Commit the transaction
				if err := db.Commit(ctx, txn); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkTransactionUpdateMasking measures the performance of update masking
// (when newer updates override older ones).
func BenchmarkTransactionUpdateMasking(b *testing.B) {
	benchmarks := []struct {
		name          string
		numOverwrites int
	}{
		{"1_overwrite", 1},
		{"5_overwrites", 5},
		{"10_overwrites", 10},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			data := map[string]any{}
			db := NewFromObject(data)
			ctx := context.Background()

			b.ResetTimer()
			b.ReportAllocs()

			for b.Loop() {
				txn, err := db.NewTransaction(ctx, storage.WriteParams)
				if err != nil {
					b.Fatal(err)
				}

				internalTxn := txn.(*transaction)
				path := storage.MustParsePath("/key")

				// Write to the same path multiple times (tests masking logic)
				for j := range bm.numOverwrites {
					if err := internalTxn.Write(storage.AddOp, path, fmt.Sprintf("value_%d", j)); err != nil {
						b.Fatal(err)
					}
				}

				// Should only have 1 update due to masking
				if len(internalTxn.updates) != 1 {
					b.Fatalf("expected 1 update, got %d", len(internalTxn.updates))
				}

				if err := db.Commit(ctx, txn); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkTransactionReads measures read performance from transactions with pending updates.
func BenchmarkTransactionReads(b *testing.B) {
	benchmarks := []struct {
		name       string
		numUpdates int
	}{
		{"no_updates", 0},
		{"5_updates", 5},
		{"10_updates", 10},
		{"20_updates", 20},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			data := map[string]any{}
			db := NewFromObject(data)
			ctx := context.Background()

			// Setup: create initial data
			for i := range 100 {
				path := storage.MustParsePath(fmt.Sprintf("/key_%d", i))
				if err := storage.WriteOne(ctx, db, storage.AddOp, path, fmt.Sprintf("value_%d", i)); err != nil {
					b.Fatal(err)
				}
			}

			b.ResetTimer()
			b.ReportAllocs()

			for b.Loop() {
				txn, err := db.NewTransaction(ctx, storage.WriteParams)
				if err != nil {
					b.Fatal(err)
				}

				internalTxn := txn.(*transaction)

				// Add some pending updates
				for j := range bm.numUpdates {
					path := storage.MustParsePath(fmt.Sprintf("/updated_key_%d", j))
					if err := internalTxn.Write(storage.AddOp, path, fmt.Sprintf("new_value_%d", j)); err != nil {
						b.Fatal(err)
					}
				}

				// Read from existing data
				if _, err := internalTxn.Read(storage.MustParsePath("/key_50")); err != nil {
					b.Fatal(err)
				}

				// Read from pending updates
				if bm.numUpdates > 0 {
					if _, err := internalTxn.Read(storage.MustParsePath("/updated_key_0")); err != nil {
						b.Fatal(err)
					}
				}

				db.Abort(ctx, txn)
			}
		})
	}
}

// BenchmarkTransactionCommit measures commit performance with various numbers of updates.
func BenchmarkTransactionCommit(b *testing.B) {
	benchmarks := []struct {
		name       string
		numUpdates int
	}{
		{"1_update", 1},
		{"10_updates", 10},
		{"50_updates", 50},
		{"100_updates", 100},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			data := map[string]any{}
			db := NewFromObject(data)
			ctx := context.Background()

			b.ResetTimer()
			b.ReportAllocs()

			for i := range b.N {
				b.StopTimer()
				txn, err := db.NewTransaction(ctx, storage.WriteParams)
				if err != nil {
					b.Fatal(err)
				}

				internalTxn := txn.(*transaction)

				// Prepare updates
				for j := range bm.numUpdates {
					path := storage.MustParsePath(fmt.Sprintf("/key_%d_%d", i, j))
					if err := internalTxn.Write(storage.AddOp, path, fmt.Sprintf("value_%d", j)); err != nil {
						b.Fatal(err)
					}
				}

				b.StartTimer()
				// Measure only commit time
				if err := db.Commit(ctx, txn); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
