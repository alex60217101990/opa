// Copyright 2026 The OPA Authors.  All rights reserved.
// Use of this source code is governed by an Apache2
// license that can be found in the LICENSE file.

package ast

import (
	"fmt"
	"testing"
)

// BenchmarkInterfaceToValue_StringCacheBenefit demonstrates the benefit of string caching
// for realistic scenarios with repeated keys (like JSON API responses)
func BenchmarkInterfaceToValue_StringCacheBenefit(b *testing.B) {
	// Realistic API response scenario: array of objects with same field names
	b.Run("repeated_keys_size_200", func(b *testing.B) {
		// 200 objects with 7 repeated keys = 1,400 string conversions
		// Without cache: 1,400 String allocations
		// With cache (threshold=100): 7 String allocations + cache overhead
		data := make([]any, 200)
		for i := range 200 {
			data[i] = map[string]any{
				"id":         i,
				"name":       fmt.Sprintf("User %d", i),
				"email":      fmt.Sprintf("user%d@example.com", i),
				"status":     "active",
				"role":       "user",
				"created_at": "2024-01-01",
				"updated_at": "2024-01-27",
			}
		}

		b.ReportAllocs()
		b.ResetTimer()

		for b.Loop() {
			_, err := InterfaceToValue(data)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("repeated_keys_size_500", func(b *testing.B) {
		// 500 objects with 7 repeated keys = 3,500 string conversions
		// Without cache: 3,500 String allocations
		// With cache: 7 String allocations + cache overhead
		data := make([]any, 500)
		for i := range 500 {
			data[i] = map[string]any{
				"id":         i,
				"name":       fmt.Sprintf("User %d", i),
				"email":      fmt.Sprintf("user%d@example.com", i),
				"status":     "active",
				"role":       "user",
				"created_at": "2024-01-01",
				"updated_at": "2024-01-27",
			}
		}

		b.ReportAllocs()
		b.ResetTimer()

		for b.Loop() {
			_, err := InterfaceToValue(data)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("repeated_keys_size_1000", func(b *testing.B) {
		// 1000 objects with 7 repeated keys = 7,000 string conversions
		// Without cache: 7,000 String allocations
		// With cache: 7 String allocations + cache overhead
		data := make([]any, 1000)
		for i := range 1000 {
			data[i] = map[string]any{
				"id":         i,
				"name":       fmt.Sprintf("User %d", i),
				"email":      fmt.Sprintf("user%d@example.com", i),
				"status":     "active",
				"role":       "user",
				"created_at": "2024-01-01",
				"updated_at": "2024-01-27",
			}
		}

		b.ReportAllocs()
		b.ResetTimer()

		for b.Loop() {
			_, err := InterfaceToValue(data)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}
