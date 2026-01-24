// Copyright 2026 The OPA Authors.  All rights reserved.
// Use of this source code is governed by an Apache2
// license that can be found in the LICENSE file.

package ast

import (
	"fmt"
	"testing"
)

// BenchmarkInterfaceToValue_Slices benchmarks slice conversions with batch allocation
func BenchmarkInterfaceToValue_Slices(b *testing.B) {
	sizes := []int{10, 50, 100, 500}

	for _, size := range sizes {
		// []any with mixed types
		b.Run(fmt.Sprintf("any/size_%d", size), func(b *testing.B) {
			data := make([]any, size)
			for i := range size {
				switch i % 4 {
				case 0:
					data[i] = i
				case 1:
					data[i] = fmt.Sprintf("value_%d", i)
				case 2:
					data[i] = i%2 == 0
				default:
					data[i] = float64(i) * 1.5
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

		// []string
		b.Run(fmt.Sprintf("string/size_%d", size), func(b *testing.B) {
			data := make([]string, size)
			for i := range size {
				data[i] = fmt.Sprintf("value_%d", i)
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

		// []any with nested maps
		b.Run(fmt.Sprintf("nested/size_%d", size), func(b *testing.B) {
			data := make([]any, size)
			for i := range size {
				data[i] = map[string]any{
					"id":    i,
					"name":  fmt.Sprintf("Name %d", i),
					"value": i * 100,
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
}

// BenchmarkInterfaceToValue_SliceSmall benchmarks small slices (edge cases)
func BenchmarkInterfaceToValue_SliceSmall(b *testing.B) {
	sizes := []int{1, 2, 3, 5}

	for _, size := range sizes {
		data := make([]any, size)
		for i := range size {
			data[i] = i
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
