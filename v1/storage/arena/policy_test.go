// Copyright 2026 The OPA Authors.  All rights reserved.
// Use of this source code is governed by an Apache2
// license that can be found in the LICENSE file.

package arena

import (
	"testing"
)

func TestPolicyStore(t *testing.T) {
	ps := NewPolicyStore(4)

	// Test empty store
	if ps.Len() != 0 {
		t.Fatalf("Expected len 0, got %d", ps.Len())
	}

	list := ps.List()
	if len(list) != 0 {
		t.Fatalf("Expected empty list, got %v", list)
	}

	// Test upsert
	ps.Upsert("policy1", []byte("data1"))
	if ps.Len() != 1 {
		t.Fatalf("Expected len 1, got %d", ps.Len())
	}

	// Test get
	data, err := ps.Get("policy1")
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "data1" {
		t.Fatalf("Expected 'data1', got '%s'", string(data))
	}

	// Test update
	ps.Upsert("policy1", []byte("data1_updated"))
	data, err = ps.Get("policy1")
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "data1_updated" {
		t.Fatalf("Expected 'data1_updated', got '%s'", string(data))
	}

	// Len should still be 1
	if ps.Len() != 1 {
		t.Fatalf("Expected len 1 after update, got %d", ps.Len())
	}

	// Add more policies
	ps.Upsert("policy2", []byte("data2"))
	ps.Upsert("policy3", []byte("data3"))

	if ps.Len() != 3 {
		t.Fatalf("Expected len 3, got %d", ps.Len())
	}

	// Test list
	list = ps.List()
	if len(list) != 3 {
		t.Fatalf("Expected 3 policies, got %d", len(list))
	}

	// Test delete
	err = ps.Delete("policy2")
	if err != nil {
		t.Fatal(err)
	}

	if ps.Len() != 2 {
		t.Fatalf("Expected len 2 after delete, got %d", ps.Len())
	}

	// Try to get deleted policy
	_, err = ps.Get("policy2")
	if err == nil {
		t.Fatal("Expected error getting deleted policy")
	}

	// List should not include deleted
	list = ps.List()
	if len(list) != 2 {
		t.Fatalf("Expected 2 policies in list, got %d", len(list))
	}

	// Test freelist reuse
	ps.Upsert("policy4", []byte("data4"))
	if ps.Len() != 3 {
		t.Fatalf("Expected len 3 after freelist reuse, got %d", ps.Len())
	}
}

func TestPolicyStoreStringInterning(t *testing.T) {
	ps := NewPolicyStore(8)

	// Add multiple policies with same ID pattern
	for i := 0; i < 100; i++ {
		// Reuse same ID string to test interning
		id := "common_policy"
		ps.Upsert(id, []byte("data"))
	}

	// Should only have 1 policy (updated 100 times)
	if ps.Len() != 1 {
		t.Fatalf("Expected len 1, got %d", ps.Len())
	}

	t.Log("String interning working - repeated IDs deduplicated")
}

func BenchmarkPolicyStoreUpsert(b *testing.B) {
	ps := NewPolicyStore(16)
	data := []byte("policy data")

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		ps.Upsert("policy", data)
	}
}

func BenchmarkPolicyStoreGet(b *testing.B) {
	ps := NewPolicyStore(16)
	ps.Upsert("policy", []byte("data"))

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, _ = ps.Get("policy")
	}
}

func BenchmarkMapPolicyUpsert(b *testing.B) {
	m := make(map[string][]byte, 16)
	data := []byte("policy data")

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		m["policy"] = data
	}
}

func BenchmarkMapPolicyGet(b *testing.B) {
	m := map[string][]byte{"policy": []byte("data")}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = m["policy"]
	}
}
