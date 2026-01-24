// Copyright 2026 The OPA Authors.  All rights reserved.
// Use of this source code is governed by an Apache2
// license that can be found in the LICENSE file.

package ast

import (
	"fmt"
	"testing"

	"github.com/open-policy-agent/opa/v1/util"
)

// BenchmarkArrayCreation_Old simulates old NewArray approach with variadic
func BenchmarkArrayCreation_Old(b *testing.B) {
	sizes := []int{10, 50, 100, 500}

	for _, size := range sizes {
		terms := util.NewPtrSlice[Term](size)
		for i := range size {
			terms[i].Value = String(fmt.Sprintf("value_%d", i))
		}

		b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				_ = NewArray(terms...) // Variadic call
			}
		})
	}
}

// BenchmarkArrayCreation_New simulates new direct approach
func BenchmarkArrayCreation_New(b *testing.B) {
	sizes := []int{10, 50, 100, 500}

	for _, size := range sizes {
		terms := util.NewPtrSlice[Term](size)
		for i := range size {
			terms[i].Value = String(fmt.Sprintf("value_%d", i))
		}

		b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				// Direct creation without variadic
				hs := make([]int, len(terms))
				for i, e := range terms {
					hs[i] = e.Value.Hash()
				}
				arr := &Array{elems: terms, hashs: hs, ground: true}
				arr.rehash()
				_ = arr
			}
		})
	}
}
