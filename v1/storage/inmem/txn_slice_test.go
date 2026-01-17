// Copyright 2017 The OPA Authors.  All rights reserved.
// Use of this source code is governed by an Apache2
// license that can be found in the LICENSE file.

package inmem

import (
	"context"
	"testing"

	"github.com/open-policy-agent/opa/v1/storage"
)

// TestSliceUpdateMasking verifies that the slice-based update implementation
// correctly handles update masking (where newer updates override or modify older ones).
func TestSliceUpdateMasking(t *testing.T) {
	tests := []struct {
		name     string
		setup    func(*transaction) error
		validate func(*testing.T, *transaction)
	}{
		{
			name: "exact path masking - newer update replaces older",
			setup: func(txn *transaction) error {
				// First write
				if err := txn.Write(storage.AddOp, storage.MustParsePath("/x"), "value1"); err != nil {
					return err
				}
				// Second write to same path should mask the first (use AddOp since in transaction)
				return txn.Write(storage.AddOp, storage.MustParsePath("/x"), "value2")
			},
			validate: func(t *testing.T, txn *transaction) {
				// Should only have one update (second one)
				if len(txn.updates) != 1 {
					t.Errorf("expected 1 update, got %d", len(txn.updates))
				}
				// Read should return the latest value
				val, err := txn.Read(storage.MustParsePath("/x"))
				if err != nil {
					t.Fatalf("read failed: %v", err)
				}
				if val != "value2" {
					t.Errorf("expected 'value2', got %v", val)
				}
			},
		},
		{
			name: "prefix path masking - newer update at parent path removes child updates",
			setup: func(txn *transaction) error {
				// First create parent object
				if err := txn.Write(storage.AddOp, storage.MustParsePath("/x"), map[string]any{"y": map[string]any{"z": "value1"}}); err != nil {
					return err
				}
				// Write to parent path should mask the nested update (use AddOp)
				return txn.Write(storage.AddOp, storage.MustParsePath("/x"), map[string]any{"a": "value2"})
			},
			validate: func(t *testing.T, txn *transaction) {
				// Should only have one update (last one)
				if len(txn.updates) != 1 {
					t.Errorf("expected 1 update, got %d", len(txn.updates))
				}
				// Read parent should return new value
				val, err := txn.Read(storage.MustParsePath("/x"))
				if err != nil {
					t.Fatalf("read failed: %v", err)
				}
				obj, ok := val.(map[string]any)
				if !ok {
					t.Fatalf("expected map[string]any, got %T", val)
				}
				if obj["a"] != "value2" {
					t.Errorf("expected 'value2', got %v", obj["a"])
				}
			},
		},
		{
			name: "child path modification - newer update modifies parent update",
			setup: func(txn *transaction) error {
				// Write parent object
				if err := txn.Write(storage.AddOp, storage.MustParsePath("/x"), map[string]any{"a": "value1"}); err != nil {
					return err
				}
				// Write to child path should modify the parent update
				return txn.Write(storage.AddOp, storage.MustParsePath("/x/b"), "value2")
			},
			validate: func(t *testing.T, txn *transaction) {
				// Should still have one update (modified parent)
				if len(txn.updates) != 1 {
					t.Errorf("expected 1 update, got %d", len(txn.updates))
				}
				// Read should return merged value
				val, err := txn.Read(storage.MustParsePath("/x"))
				if err != nil {
					t.Fatalf("read failed: %v", err)
				}
				obj, ok := val.(map[string]any)
				if !ok {
					t.Fatalf("expected map[string]any, got %T", val)
				}
				if obj["a"] != "value1" || obj["b"] != "value2" {
					t.Errorf("expected both a and b, got %v", obj)
				}
			},
		},
		{
			name: "multiple independent updates - no masking",
			setup: func(txn *transaction) error {
				if err := txn.Write(storage.AddOp, storage.MustParsePath("/x"), "value1"); err != nil {
					return err
				}
				if err := txn.Write(storage.AddOp, storage.MustParsePath("/y"), "value2"); err != nil {
					return err
				}
				return txn.Write(storage.AddOp, storage.MustParsePath("/z"), "value3")
			},
			validate: func(t *testing.T, txn *transaction) {
				// Should have three updates
				if len(txn.updates) != 3 {
					t.Errorf("expected 3 updates, got %d", len(txn.updates))
				}
			},
		},
		{
			name: "LIFO order verification - most recent update is first",
			setup: func(txn *transaction) error {
				if err := txn.Write(storage.AddOp, storage.MustParsePath("/x"), "value1"); err != nil {
					return err
				}
				if err := txn.Write(storage.AddOp, storage.MustParsePath("/y"), "value2"); err != nil {
					return err
				}
				return txn.Write(storage.AddOp, storage.MustParsePath("/z"), "value3")
			},
			validate: func(t *testing.T, txn *transaction) {
				// Verify LIFO order: most recent first
				if len(txn.updates) != 3 {
					t.Fatalf("expected 3 updates, got %d", len(txn.updates))
				}
				// Most recent update should be first
				if !txn.updates[0].Path().Equal(storage.MustParsePath("/z")) {
					t.Errorf("expected first update to be /z, got %v", txn.updates[0].Path())
				}
				if !txn.updates[2].Path().Equal(storage.MustParsePath("/x")) {
					t.Errorf("expected last update to be /x, got %v", txn.updates[2].Path())
				}
			},
		},
		{
			name: "update value optimization - same value write is no-op",
			setup: func(txn *transaction) error {
				// Add value
				if err := txn.Write(storage.AddOp, storage.MustParsePath("/x"), "value1"); err != nil {
					return err
				}
				// Write same value again - should be optimized out
				return txn.Write(storage.AddOp, storage.MustParsePath("/x"), "value1")
			},
			validate: func(t *testing.T, txn *transaction) {
				// Should still have one update (same value optimization)
				if len(txn.updates) != 1 {
					t.Errorf("expected 1 update, got %d", len(txn.updates))
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Initialize with empty data
			data := map[string]any{}
			db := NewFromObject(data)
			ctx := context.Background()

			txn, err := db.NewTransaction(ctx, storage.WriteParams)
			if err != nil {
				t.Fatalf("failed to create transaction: %v", err)
			}

			if err := tt.setup(txn.(*transaction)); err != nil {
				t.Fatalf("setup failed: %v", err)
			}

			tt.validate(t, txn.(*transaction))
		})
	}
}

// TestSliceUpdateCommit verifies that updates are applied correctly when committed.
func TestSliceUpdateCommit(t *testing.T) {
	data := map[string]any{}
	db := NewFromObject(data)
	ctx := context.Background()

	// Make multiple updates
	updates := []struct {
		path  string
		value any
	}{
		{"/x", "value1"},
		{"/y", "value2"},
		{"/z", map[string]any{"f": "value3"}},
	}

	for _, upd := range updates {
		if err := storage.WriteOne(ctx, db, storage.AddOp, storage.MustParsePath(upd.path), upd.value); err != nil {
			t.Fatalf("write failed: %v", err)
		}
	}

	// Verify values in new read transaction
	for _, upd := range updates {
		val, err := storage.ReadOne(ctx, db, storage.MustParsePath(upd.path))
		if err != nil {
			t.Errorf("read %s failed: %v", upd.path, err)
			continue
		}

		switch expected := upd.value.(type) {
		case string:
			if val != expected {
				t.Errorf("expected %v, got %v", expected, val)
			}
		case map[string]any:
			valMap, ok := val.(map[string]any)
			if !ok {
				t.Errorf("expected map[string]any, got %T", val)
				continue
			}
			for k, v := range expected {
				if valMap[k] != v {
					t.Errorf("expected %v, got %v", v, valMap[k])
				}
			}
		}
	}
}

// TestSliceUpdateReadIsolation verifies that Read sees updates from the same transaction.
func TestSliceUpdateReadIsolation(t *testing.T) {
	data := map[string]any{}
	db := NewFromObject(data)
	ctx := context.Background()

	txn, err := db.NewTransaction(ctx, storage.WriteParams)
	if err != nil {
		t.Fatalf("failed to create transaction: %v", err)
	}
	internalTxn := txn.(*transaction)

	// Write value
	path := storage.MustParsePath("/x")
	if err := internalTxn.Write(storage.AddOp, path, "value1"); err != nil {
		t.Fatalf("write failed: %v", err)
	}

	// Read should see the update
	val, err := internalTxn.Read(path)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}
	if val != "value1" {
		t.Errorf("expected 'value1', got %v", val)
	}

	// Update value (use AddOp since within transaction)
	if err := internalTxn.Write(storage.AddOp, path, "value2"); err != nil {
		t.Fatalf("write failed: %v", err)
	}

	// Read should see the new value
	val, err = internalTxn.Read(path)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}
	if val != "value2" {
		t.Errorf("expected 'value2', got %v", val)
	}
}
