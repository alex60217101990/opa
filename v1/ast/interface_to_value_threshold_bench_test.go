// Copyright 2026 The OPA Authors.  All rights reserved.
// Use of this source code is governed by an Apache2
// license that can be found in the LICENSE file.

package ast

import (
	"fmt"
	"testing"
)

// BenchmarkInterfaceToValue_ThresholdFinding finds optimal threshold for hybrid approach
func BenchmarkInterfaceToValue_ThresholdFinding(b *testing.B) {
	// Test various sizes to find crossover point
	sizes := []int{5, 10, 15, 20, 25, 30, 35, 40, 45, 50}

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
