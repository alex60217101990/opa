// Copyright 2026 The OPA Authors.  All rights reserved.
// Use of this source code is governed by an Apache2
// license that can be found in the LICENSE file.

package arena

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/open-policy-agent/opa/v1/storage"
	"github.com/open-policy-agent/opa/v1/storage/inmem"
)

func BenchmarkArenaWrite(b *testing.B) {
	ctx := context.Background()
	store := New()

	data := generateTestData(100)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		txn, _ := store.NewTransaction(ctx, storage.WriteParams)
		_ = store.Write(ctx, txn, storage.AddOp, storage.MustParsePath("/data"), data)
		_ = store.Commit(ctx, txn)
	}
}

func BenchmarkInmemWrite(b *testing.B) {
	ctx := context.Background()
	store := inmem.New()

	data := generateTestData(100)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		txn, _ := store.NewTransaction(ctx, storage.WriteParams)
		_ = store.Write(ctx, txn, storage.AddOp, storage.MustParsePath("/data"), data)
		_ = store.Commit(ctx, txn)
	}
}

func BenchmarkArenaRead(b *testing.B) {
	ctx := context.Background()
	store := New()

	// Setup
	txn, _ := store.NewTransaction(ctx, storage.WriteParams)
	data := generateTestData(100)
	_ = store.Write(ctx, txn, storage.AddOp, storage.MustParsePath("/data"), data)
	_ = store.Commit(ctx, txn)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		txn, _ := store.NewTransaction(ctx)
		_, _ = store.Read(ctx, txn, storage.MustParsePath("/data/users/0/name"))
		store.Abort(ctx, txn)
	}
}

func BenchmarkInmemRead(b *testing.B) {
	ctx := context.Background()
	store := inmem.New()

	// Setup
	txn, _ := store.NewTransaction(ctx, storage.WriteParams)
	data := generateTestData(100)
	_ = store.Write(ctx, txn, storage.AddOp, storage.MustParsePath("/data"), data)
	_ = store.Commit(ctx, txn)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		txn, _ := store.NewTransaction(ctx)
		_, _ = store.Read(ctx, txn, storage.MustParsePath("/data/users/0/name"))
		store.Abort(ctx, txn)
	}
}

func BenchmarkArenaDeepRead(b *testing.B) {
	ctx := context.Background()
	store := New()

	// Setup deep nested structure
	txn, _ := store.NewTransaction(ctx, storage.WriteParams)
	data := generateNestedData(10)
	_ = store.Write(ctx, txn, storage.AddOp, storage.MustParsePath("/data"), data)
	_ = store.Commit(ctx, txn)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		txn, _ := store.NewTransaction(ctx)
		path := storage.MustParsePath("/data/level0/level1/level2/level3/level4/value")
		_, _ = store.Read(ctx, txn, path)
		store.Abort(ctx, txn)
	}
}

func BenchmarkInmemDeepRead(b *testing.B) {
	ctx := context.Background()
	store := inmem.New()

	// Setup deep nested structure
	txn, _ := store.NewTransaction(ctx, storage.WriteParams)
	data := generateNestedData(10)
	_ = store.Write(ctx, txn, storage.AddOp, storage.MustParsePath("/data"), data)
	_ = store.Commit(ctx, txn)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		txn, _ := store.NewTransaction(ctx)
		path := storage.MustParsePath("/data/level0/level1/level2/level3/level4/value")
		_, _ = store.Read(ctx, txn, path)
		store.Abort(ctx, txn)
	}
}

func BenchmarkArenaUpdate(b *testing.B) {
	ctx := context.Background()
	store := New()

	// Setup
	txn, _ := store.NewTransaction(ctx, storage.WriteParams)
	data := map[string]any{"counter": 0}
	_ = store.Write(ctx, txn, storage.AddOp, storage.MustParsePath("/data"), data)
	_ = store.Commit(ctx, txn)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		txn, _ := store.NewTransaction(ctx, storage.WriteParams)
		_ = store.Write(ctx, txn, storage.ReplaceOp, storage.MustParsePath("/data/counter"), i)
		_ = store.Commit(ctx, txn)
	}
}

func BenchmarkInmemUpdate(b *testing.B) {
	ctx := context.Background()
	store := inmem.New()

	// Setup
	txn, _ := store.NewTransaction(ctx, storage.WriteParams)
	data := map[string]any{"counter": 0}
	_ = store.Write(ctx, txn, storage.AddOp, storage.MustParsePath("/data"), data)
	_ = store.Commit(ctx, txn)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		txn, _ := store.NewTransaction(ctx, storage.WriteParams)
		_ = store.Write(ctx, txn, storage.ReplaceOp, storage.MustParsePath("/data/counter"), i)
		_ = store.Commit(ctx, txn)
	}
}

func BenchmarkArenaScavenger(b *testing.B) {
	ctx := context.Background()
	store := NewWithOpts(WithScavenger(100 * time.Millisecond)).(*Arena)
	defer store.StopScavenger()

	data := generateTestData(50)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		txn, _ := store.NewTransaction(ctx, storage.WriteParams)
		_ = store.Write(ctx, txn, storage.AddOp, storage.MustParsePath("/data"), data)
		_ = store.Commit(ctx, txn)

		// Update to create tombstones
		txn, _ = store.NewTransaction(ctx, storage.WriteParams)
		_ = store.Write(ctx, txn, storage.RemoveOp, storage.MustParsePath("/data/users/0"), nil)
		_ = store.Commit(ctx, txn)
	}
}

func BenchmarkArenaMemoryReuse(b *testing.B) {
	store := New().(*Arena)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Allocate
		idx := store.alloc()

		// Use the node
		node := store.getNode(idx)
		node.SetInt(42)

		// Free it
		store.free(idx)
	}
}

// Benchmark node operations
func BenchmarkNodeSetInt(b *testing.B) {
	var node Node

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		node.SetInt(i)
	}
}

func BenchmarkNodeSetString(b *testing.B) {
	var node Node

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		node.SetString("test")
	}
}

func BenchmarkNodeAsInt(b *testing.B) {
	var node Node
	node.SetInt(42)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = node.AsInt()
	}
}

func BenchmarkNodeAsString(b *testing.B) {
	var node Node
	node.SetString("test")

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = node.AsString()
	}
}

// Helper functions

func generateTestData(numUsers int) map[string]any {
	users := make([]any, numUsers)
	for i := 0; i < numUsers; i++ {
		users[i] = map[string]any{
			"name":  fmt.Sprintf("user%d", i),
			"age":   20 + i%50,
			"email": fmt.Sprintf("user%d@example.com", i),
			"tags":  []any{"tag1", "tag2", "tag3"},
		}
	}

	return map[string]any{
		"users": users,
		"config": map[string]any{
			"enabled": true,
			"timeout": 5000,
			"retries": 3,
		},
	}
}

func generateNestedData(depth int) map[string]any {
	if depth == 0 {
		return map[string]any{"value": 42}
	}

	return map[string]any{
		fmt.Sprintf("level%d", depth-1): generateNestedData(depth - 1),
	}
}
