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

// BenchmarkCommitWithoutTriggers measures commit performance when no triggers registered.
// This is the common case - most transactions don't have triggers.
func BenchmarkCommitWithoutTriggers(b *testing.B) {
	sizes := []int{10, 100, 500}

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

				// Write updates
				for j := range size {
					path := storage.MustParsePath(fmt.Sprintf("/key_%d", j))
					_ = internalTxn.Write(storage.AddOp, path, fmt.Sprintf("value_%d", j))
				}

				// Commit without triggers - should skip event building
				_ = db.Commit(ctx, txn)
			}
		})
	}
}

// BenchmarkCommitWithTriggers measures commit performance when triggers are registered.
func BenchmarkCommitWithTriggers(b *testing.B) {
	sizes := []int{10, 100, 500}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("%d_updates_with_trigger", size), func(b *testing.B) {
			data := map[string]any{}
			db := NewFromObject(data)
			ctx := context.Background()

			// Register a no-op trigger
			setupTxn, _ := db.NewTransaction(ctx, storage.WriteParams)
			_, _ = db.Register(ctx, setupTxn, storage.TriggerConfig{
				OnCommit: func(context.Context, storage.Transaction, storage.TriggerEvent) {},
			})
			db.Abort(ctx, setupTxn)

			b.ReportAllocs()
			b.ResetTimer()

			for b.Loop() {
				txn, _ := db.NewTransaction(ctx, storage.WriteParams)
				internalTxn := txn.(*transaction)

				// Write updates
				for j := range size {
					path := storage.MustParsePath(fmt.Sprintf("/key_%d", j))
					_ = internalTxn.Write(storage.AddOp, path, fmt.Sprintf("value_%d", j))
				}

				// Commit with trigger - must build events
				_ = db.Commit(ctx, txn)
			}
		})
	}
}

// BenchmarkTriggerEventBuilding isolates the cost of building trigger events.
func BenchmarkTriggerEventBuilding(b *testing.B) {
	b.Run("no_triggers_no_event_building", func(b *testing.B) {
		data := map[string]any{}
		db := NewFromObject(data)
		ctx := context.Background()

		b.ReportAllocs()
		b.ResetTimer()

		for b.Loop() {
			txn, _ := db.NewTransaction(ctx, storage.WriteParams)
			internalTxn := txn.(*transaction)

			for j := range 100 {
				path := storage.MustParsePath(fmt.Sprintf("/key_%d", j))
				_ = internalTxn.Write(storage.AddOp, path, j)
			}

			// hasTriggers = false, should skip DataEvent allocation
			_ = db.Commit(ctx, txn)
		}
	})

	b.Run("with_triggers_event_building", func(b *testing.B) {
		data := map[string]any{}
		db := NewFromObject(data)
		ctx := context.Background()

		// Register trigger to enable event building
		setupTxn, _ := db.NewTransaction(ctx, storage.WriteParams)
		_, _ = db.Register(ctx, setupTxn, storage.TriggerConfig{
			OnCommit: func(context.Context, storage.Transaction, storage.TriggerEvent) {},
		})
		db.Abort(ctx, setupTxn)

		b.ReportAllocs()
		b.ResetTimer()

		for b.Loop() {
			txn, _ := db.NewTransaction(ctx, storage.WriteParams)
			internalTxn := txn.(*transaction)

			for j := range 100 {
				path := storage.MustParsePath(fmt.Sprintf("/key_%d", j))
				_ = internalTxn.Write(storage.AddOp, path, j)
			}

			// hasTriggers = true, must build DataEvents
			_ = db.Commit(ctx, txn)
		}
	})
}

// BenchmarkPolicyTriggerDecompression measures policy decompression overhead for triggers.
func BenchmarkPolicyTriggerDecompression(b *testing.B) {
	// Generate test policy data
	policyData := []byte(`package test

default allow = false

allow {
	input.user == "admin"
	input.action == "read"
}

allow {
	input.user == "user"
	input.resource.owner == input.user
}`)

	b.Run("without_triggers_no_decompression", func(b *testing.B) {
		data := map[string]any{}
		db := NewFromObject(data)
		ctx := context.Background()

		b.ReportAllocs()
		b.ResetTimer()

		for b.Loop() {
			txn, _ := db.NewTransaction(ctx, storage.WriteParams)
			underlying := txn.(*transaction)

			// Upsert policy - will compress
			_ = underlying.UpsertPolicy("test_policy", policyData)

			// Commit without triggers - should skip decompression
			_ = db.Commit(ctx, txn)
		}
	})

	b.Run("with_triggers_decompression", func(b *testing.B) {
		data := map[string]any{}
		db := NewFromObject(data)
		ctx := context.Background()

		// Register trigger
		setupTxn, _ := db.NewTransaction(ctx, storage.WriteParams)
		_, _ = db.Register(ctx, setupTxn, storage.TriggerConfig{
			OnCommit: func(context.Context, storage.Transaction, storage.TriggerEvent) {},
		})
		db.Abort(ctx, setupTxn)

		b.ReportAllocs()
		b.ResetTimer()

		for b.Loop() {
			txn, _ := db.NewTransaction(ctx, storage.WriteParams)
			underlying := txn.(*transaction)

			// Upsert policy - will compress
			_ = underlying.UpsertPolicy("test_policy", policyData)

			// Commit with triggers - must decompress for event
			_ = db.Commit(ctx, txn)
		}
	})
}
