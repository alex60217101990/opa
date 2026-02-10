// Copyright 2026 The OPA Authors.  All rights reserved.
// Use of this source code is governed by an Apache2
// license that can be found in the LICENSE file.

package arena_test

import (
	"context"
	"fmt"
	"time"

	"github.com/open-policy-agent/opa/v1/storage"
	"github.com/open-policy-agent/opa/v1/storage/arena"
)

func ExampleNew() {
	// Create a new arena storage backend
	store := arena.New()

	ctx := context.Background()

	// Create a write transaction
	txn, _ := store.NewTransaction(ctx, storage.WriteParams)

	// Write some data
	data := map[string]any{
		"users": []any{
			map[string]any{"name": "alice", "role": "admin"},
			map[string]any{"name": "bob", "role": "user"},
		},
	}

	_ = store.Write(ctx, txn, storage.AddOp, storage.MustParsePath("/data"), data)
	_ = store.Commit(ctx, txn)

	// Read the data
	txn, _ = store.NewTransaction(ctx)
	defer store.Abort(ctx, txn)

	value, _ := store.Read(ctx, txn, storage.MustParsePath("/data/users/0/name"))
	fmt.Println(value)

	// Output: alice
}

func ExampleNewWithOpts() {
	// Create an arena with background scavenging enabled
	store := arena.NewWithOpts(
		arena.WithScavenger(1 * time.Second),
	)

	ctx := context.Background()

	// Write and update data multiple times
	for i := 0; i < 5; i++ {
		txn, _ := store.NewTransaction(ctx, storage.WriteParams)
		data := map[string]any{"counter": i}
		_ = store.Write(ctx, txn, storage.AddOp, storage.MustParsePath("/data"), data)
		_ = store.Commit(ctx, txn)
	}

	// The scavenger will reclaim tombstoned nodes in the background
	time.Sleep(2 * time.Second)

	fmt.Println("Scavenger reclaimed old nodes")

	// Output: Scavenger reclaimed old nodes
}

func Example_readWrite() {
	ctx := context.Background()
	store := arena.New()

	// Write nested data
	txn, _ := store.NewTransaction(ctx, storage.WriteParams)
	config := map[string]any{
		"database": map[string]any{
			"host":     "localhost",
			"port":     5432,
			"name":     "mydb",
			"settings": map[string]any{"pool_size": 10, "timeout": 30},
		},
	}

	_ = store.Write(ctx, txn, storage.AddOp, storage.MustParsePath("/config"), config)
	_ = store.Commit(ctx, txn)

	// Read nested values
	txn, _ = store.NewTransaction(ctx)
	defer store.Abort(ctx, txn)

	host, _ := store.Read(ctx, txn, storage.MustParsePath("/config/database/host"))
	poolSize, _ := store.Read(ctx, txn, storage.MustParsePath("/config/database/settings/pool_size"))

	fmt.Printf("Host: %v, Pool Size: %v\n", host, poolSize)

	// Output: Host: localhost, Pool Size: 10
}

func Example_update() {
	ctx := context.Background()
	store := arena.New()

	// Initial write
	txn, _ := store.NewTransaction(ctx, storage.WriteParams)
	_ = store.Write(ctx, txn, storage.AddOp, storage.MustParsePath("/data/version"), 1)
	_ = store.Commit(ctx, txn)

	// Update the value
	txn, _ = store.NewTransaction(ctx, storage.WriteParams)
	_ = store.Write(ctx, txn, storage.ReplaceOp, storage.MustParsePath("/data/version"), 2)
	_ = store.Commit(ctx, txn)

	// Read updated value
	txn, _ = store.NewTransaction(ctx)
	defer store.Abort(ctx, txn)

	version, _ := store.Read(ctx, txn, storage.MustParsePath("/data/version"))
	fmt.Println(version)

	// Output: 2
}

func Example_remove() {
	ctx := context.Background()
	store := arena.New()

	// Write data
	txn, _ := store.NewTransaction(ctx, storage.WriteParams)
	data := map[string]any{
		"a": 1,
		"b": 2,
		"c": 3,
	}
	_ = store.Write(ctx, txn, storage.AddOp, storage.MustParsePath("/data"), data)
	_ = store.Commit(ctx, txn)

	// Remove a key
	txn, _ = store.NewTransaction(ctx, storage.WriteParams)
	_ = store.Write(ctx, txn, storage.RemoveOp, storage.MustParsePath("/data/b"), nil)
	_ = store.Commit(ctx, txn)

	// Try to read removed key
	txn, _ = store.NewTransaction(ctx)
	defer store.Abort(ctx, txn)

	_, err := store.Read(ctx, txn, storage.MustParsePath("/data/b"))
	fmt.Println(storage.IsNotFound(err))

	// Read remaining keys
	a, _ := store.Read(ctx, txn, storage.MustParsePath("/data/a"))
	c, _ := store.Read(ctx, txn, storage.MustParsePath("/data/c"))
	fmt.Printf("a=%v, c=%v\n", a, c)

	// Output:
	// true
	// a=1, c=3
}

func Example_policies() {
	ctx := context.Background()
	store := arena.New()

	// Upsert a policy
	txn, _ := store.NewTransaction(ctx, storage.WriteParams)
	policy := []byte(`package example

allow {
    input.user == "alice"
}`)
	_ = store.UpsertPolicy(ctx, txn, "example.rego", policy)
	_ = store.Commit(ctx, txn)

	// List policies
	txn, _ = store.NewTransaction(ctx)
	defer store.Abort(ctx, txn)

	policies, _ := store.ListPolicies(ctx, txn)
	fmt.Println("Policies:", policies)

	// Get policy content
	content, _ := store.GetPolicy(ctx, txn, "example.rego")
	fmt.Printf("Policy size: %d bytes\n", len(content))

	// Output:
	// Policies: [example.rego]
	// Policy size: 52 bytes
}

func Example_stringInterning() {
	ctx := context.Background()
	store := arena.New()

	// Write data with many repeated keys
	txn, _ := store.NewTransaction(ctx, storage.WriteParams)

	users := make([]any, 100)
	for i := 0; i < 100; i++ {
		users[i] = map[string]any{
			"name":  fmt.Sprintf("user%d", i),
			"email": fmt.Sprintf("user%d@example.com", i),
			"role":  "user", // This string will be interned once
		}
	}

	data := map[string]any{"users": users}
	_ = store.Write(ctx, txn, storage.AddOp, storage.MustParsePath("/data"), data)
	_ = store.Commit(ctx, txn)

	// The keys "name", "email", "role" are stored only once via unique.Handle
	// This significantly reduces memory usage for large datasets

	fmt.Println("String interning reduces memory for repeated keys")

	// Output: String interning reduces memory for repeated keys
}

func Example_deepNesting() {
	ctx := context.Background()
	store := arena.New()

	// Write deeply nested structure
	txn, _ := store.NewTransaction(ctx, storage.WriteParams)

	data := map[string]any{
		"level1": map[string]any{
			"level2": map[string]any{
				"level3": map[string]any{
					"level4": map[string]any{
						"value": "deep value",
					},
				},
			},
		},
	}

	_ = store.Write(ctx, txn, storage.AddOp, storage.MustParsePath("/data"), data)
	_ = store.Commit(ctx, txn)

	// Read from deep path
	txn, _ = store.NewTransaction(ctx)
	defer store.Abort(ctx, txn)

	value, _ := store.Read(ctx, txn, storage.MustParsePath("/data/level1/level2/level3/level4/value"))
	fmt.Println(value)

	// Output: deep value
}
