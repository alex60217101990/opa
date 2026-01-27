// Temporary benchmark for baseline comparison
package ast

import (
	"fmt"
	"testing"
)

func BenchmarkInterfaceToValue_Baseline(b *testing.B) {
	// Helper to generate realistic user data
	names := []string{"Alice", "Bob", "Charlie", "Dave", "Eve", "Frank", "Grace", "Henry"}
	statuses := []string{"active", "pending", "inactive"}
	roles := []string{"user", "admin", "moderator"}

	b.Run("array_of_maps_size_40", func(b *testing.B) {
		data := make([]any, 40)
		for i := range 40 {
			data[i] = map[string]any{
				"id":         i,
				"name":       names[i%len(names)],
				"email":      fmt.Sprintf("user%d@example.com", i),
				"status":     statuses[i%len(statuses)],
				"role":       roles[i%len(roles)],
				"created_at": fmt.Sprintf("2024-01-%02d", (i%28)+1),
				"updated_at": fmt.Sprintf("2024-02-%02d", (i%28)+1),
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

	b.Run("array_of_maps_size_50", func(b *testing.B) {
		data := make([]any, 50)
		for i := range 50 {
			data[i] = map[string]any{
				"id":         i,
				"name":       names[i%len(names)],
				"email":      fmt.Sprintf("user%d@example.com", i),
				"status":     statuses[i%len(statuses)],
				"role":       roles[i%len(roles)],
				"created_at": fmt.Sprintf("2024-01-%02d", (i%28)+1),
				"updated_at": fmt.Sprintf("2024-02-%02d", (i%28)+1),
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

	b.Run("array_of_maps_size_100", func(b *testing.B) {
		data := make([]any, 100)
		for i := range 100 {
			data[i] = map[string]any{
				"id":         i,
				"name":       names[i%len(names)],
				"email":      fmt.Sprintf("user%d@example.com", i),
				"status":     statuses[i%len(statuses)],
				"role":       roles[i%len(roles)],
				"created_at": fmt.Sprintf("2024-01-%02d", (i%28)+1),
				"updated_at": fmt.Sprintf("2024-02-%02d", (i%28)+1),
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

	b.Run("array_of_maps_size_200", func(b *testing.B) {
		data := make([]any, 200)
		for i := range 200 {
			data[i] = map[string]any{
				"id":         i,
				"name":       names[i%len(names)],
				"email":      fmt.Sprintf("user%d@example.com", i),
				"status":     statuses[i%len(statuses)],
				"role":       roles[i%len(roles)],
				"created_at": fmt.Sprintf("2024-01-%02d", (i%28)+1),
				"updated_at": fmt.Sprintf("2024-02-%02d", (i%28)+1),
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

	b.Run("unique_strings_50", func(b *testing.B) {
		data := make([]any, 50)
		for i := range 50 {
			data[i] = map[string]any{
				"unique_key_" + string(rune('A'+i%26)) + "_1": i,
				"unique_key_" + string(rune('A'+i%26)) + "_2": "value",
				"unique_key_" + string(rune('A'+i%26)) + "_3": true,
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

	b.Run("small_array_5", func(b *testing.B) {
		data := []any{
			map[string]any{"id": 1, "name": "Alice"},
			map[string]any{"id": 2, "name": "Bob"},
			map[string]any{"id": 3, "name": "Charlie"},
			map[string]any{"id": 4, "name": "Dave"},
			map[string]any{"id": 5, "name": "Eve"},
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
