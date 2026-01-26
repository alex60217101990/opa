package ast

import (
	"fmt"
	"testing"
)

// BenchmarkInterfaceToValue_StringCache tests the effectiveness of local string caching
// for arrays of objects with repeated keys
func BenchmarkInterfaceToValue_StringCache(b *testing.B) {
	// Scenario: Array of objects with common keys (typical JSON API response)
	// Keys repeat 100 times, cache should deduplicate them

	sizes := []int{10, 50, 100, 200}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("array_of_maps/size_%d", size), func(b *testing.B) {
			// Create array of maps with repeated keys
			data := make([]any, size)
			for i := 0; i < size; i++ {
				data[i] = map[string]any{
					"id":         i,
					"name":       "user_" + string(rune('A'+i%26)),
					"email":      "user@example.com",
					"status":     "active",
					"created_at": "2024-01-01",
					"updated_at": "2024-01-02",
					"role":       "user",
					"verified":   true,
				}
			}

			b.ResetTimer()
			b.ReportAllocs()

			for b.Loop() {
				_, err := InterfaceToValue(data)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}

	// Nested scenario: Object with arrays (common in real APIs)
	b.Run("nested_structure", func(b *testing.B) {
		data := map[string]any{
			"users": []any{
				map[string]any{"id": 1, "name": "Alice", "email": "alice@example.com"},
				map[string]any{"id": 2, "name": "Bob", "email": "bob@example.com"},
				map[string]any{"id": 3, "name": "Charlie", "email": "charlie@example.com"},
			},
			"posts": []any{
				map[string]any{"id": 1, "title": "Post 1", "author_id": 1},
				map[string]any{"id": 2, "title": "Post 2", "author_id": 2},
				map[string]any{"id": 3, "title": "Post 3", "author_id": 1},
			},
			"metadata": map[string]any{
				"total":    3,
				"page":     1,
				"per_page": 10,
			},
		}

		b.ResetTimer()
		b.ReportAllocs()

		for b.Loop() {
			_, err := InterfaceToValue(data)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	// Small objects - cache should NOT activate (no overhead)
	b.Run("small_objects_no_cache", func(b *testing.B) {
		data := []any{
			map[string]any{"id": 1, "name": "Alice"},
			map[string]any{"id": 2, "name": "Bob"},
		}

		b.ResetTimer()
		b.ReportAllocs()

		for b.Loop() {
			_, err := InterfaceToValue(data)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkInterfaceToValue_StringCacheHitRate measures cache effectiveness
func BenchmarkInterfaceToValue_StringCacheHitRate(b *testing.B) {
	b.Run("high_key_reuse", func(b *testing.B) {
		// 100 objects with same 5 keys = 500 total keys, 495 should be cached
		data := make([]any, 100)
		for i := 0; i < 100; i++ {
			data[i] = map[string]any{
				"id":     i,
				"name":   "user",
				"email":  "test@example.com",
				"status": "active",
				"role":   "user",
			}
		}

		b.ResetTimer()
		b.ReportAllocs()

		for b.Loop() {
			_, err := InterfaceToValue(data)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("no_key_reuse", func(b *testing.B) {
		// Every object has unique keys - cache should not help much
		data := make([]any, 100)
		for i := range 100 {
			data[i] = map[string]any{
				"unique_key_" + string(rune('A'+i%26)) + "_1": i,
				"unique_key_" + string(rune('A'+i%26)) + "_2": "value",
			}
		}

		b.ResetTimer()
		b.ReportAllocs()

		for b.Loop() {
			_, err := InterfaceToValue(data)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}
