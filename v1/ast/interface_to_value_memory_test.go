// Copyright 2026 The OPA Authors.  All rights reserved.
// Use of this source code is governed by an Apache2
// license that can be found in the LICENSE file.

package ast

import (
	"fmt"
	"runtime"
	"testing"
)

// TestInterfaceToValue_MemoryProfile creates detailed memory profiles
func TestInterfaceToValue_MemoryProfile(t *testing.T) {
	// Create test data
	data := make(map[string]any, 1000)
	for i := range 1000 {
		data[fmt.Sprintf("key_%d", i)] = map[string]any{
			"id":    i,
			"name":  fmt.Sprintf("Name %d", i),
			"value": i * 100,
			"tags":  []any{"tag1", "tag2", "tag3"},
		}
	}

	// Force GC before measurement
	runtime.GC()

	var m1, m2 runtime.MemStats
	runtime.ReadMemStats(&m1)

	// Run conversion
	for range 100 {
		_, err := InterfaceToValue(data)
		if err != nil {
			t.Fatal(err)
		}
	}

	runtime.ReadMemStats(&m2)

	// Report memory usage
	t.Logf("Total Alloc: %d bytes", m2.TotalAlloc-m1.TotalAlloc)
	t.Logf("Mallocs: %d", m2.Mallocs-m1.Mallocs)
	t.Logf("Frees: %d", m2.Frees-m1.Frees)
	t.Logf("HeapAlloc: %d bytes", m2.HeapAlloc-m1.HeapAlloc)
}

// BenchmarkInterfaceToValue_MemProfile benchmarks with memory profiling
func BenchmarkInterfaceToValue_MemProfile(b *testing.B) {
	data := make(map[string]any, 100)
	for i := range 100 {
		data[fmt.Sprintf("key_%d", i)] = map[string]any{
			"id":   i,
			"name": fmt.Sprintf("Name %d", i),
		}
	}

	b.ReportAllocs()
	

	for b.Loop() {
		_, err := InterfaceToValue(data)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkInterfaceToValue_AllocationSites shows where allocations happen
func BenchmarkInterfaceToValue_AllocationSites(b *testing.B) {
	sizes := []int{10, 50, 100}

	for _, size := range sizes {
		data := make(map[string]any, size)
		for i := range size {
			data[fmt.Sprintf("key_%d", i)] = map[string]any{
				"field1": i,
				"field2": fmt.Sprintf("value_%d", i),
			}
		}

		b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				_, err := InterfaceToValue(data)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
