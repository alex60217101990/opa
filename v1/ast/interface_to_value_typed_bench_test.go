// Copyright 2026 The OPA Authors.  All rights reserved.
// Use of this source code is governed by an Apache2
// license that can be found in the LICENSE file.

package ast

import (
	"fmt"
	"testing"
)

// BenchmarkInterfaceToValue_TypedMaps benchmarks typed map conversions
func BenchmarkInterfaceToValue_TypedMaps(b *testing.B) {
	sizes := []int{10, 50, 100}

	for _, size := range sizes {
		// map[string]string
		b.Run(fmt.Sprintf("string/size_%d", size), func(b *testing.B) {
			data := make(map[string]string, size)
			for i := range size {
				data[fmt.Sprintf("key_%d", i)] = fmt.Sprintf("value_%d", i)
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

		// map[string]int
		b.Run(fmt.Sprintf("int/size_%d", size), func(b *testing.B) {
			data := make(map[string]int, size)
			for i := range size {
				data[fmt.Sprintf("key_%d", i)] = i * 100
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

		// map[string]bool
		b.Run(fmt.Sprintf("bool/size_%d", size), func(b *testing.B) {
			data := make(map[string]bool, size)
			for i := range size {
				data[fmt.Sprintf("key_%d", i)] = i%2 == 0
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
