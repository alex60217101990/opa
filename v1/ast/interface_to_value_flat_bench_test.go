// Copyright 2026 The OPA Authors.  All rights reserved.
// Use of this source code is governed by an Apache2
// license that can be found in the LICENSE file.

package ast

import (
	"fmt"
	"testing"
)

// BenchmarkInterfaceToValue_FlatMaps benchmarks with flat data (no nested maps)
// to accurately measure threshold performance without recursive effects
func BenchmarkInterfaceToValue_FlatMaps(b *testing.B) {
	sizes := []int{5, 10, 15, 20, 30, 50, 100}

	for _, size := range sizes {
		data := make(map[string]any, size)
		for i := range size {
			// Use simple values to avoid nested InterfaceToValue calls
			switch i % 3 {
			case 0:
				data[fmt.Sprintf("key_%d", i)] = i
			case 1:
				data[fmt.Sprintf("key_%d", i)] = fmt.Sprintf("value_%d", i)
			default:
				data[fmt.Sprintf("key_%d", i)] = i%2 == 0
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
