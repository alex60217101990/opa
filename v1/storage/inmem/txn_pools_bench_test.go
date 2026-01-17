// Copyright 2017 The OPA Authors.  All rights reserved.
// Use of this source code is governed by an Apache2
// license that can be found in the LICENSE file.

package inmem

import (
	"context"
	"fmt"
	"testing"

	"github.com/open-policy-agent/opa/v1/storage"
)

// BenchmarkPoolingImpact measures the impact of sync.Pool usage
// on allocation count and memory usage for large transactions.
func BenchmarkPoolingImpact(b *testing.B) {
	sizes := []int{100, 500, 1000, 5000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("pooled_%d_independent_writes", size), func(b *testing.B) {
			data := map[string]any{}
			db := NewFromObject(data)
			ctx := context.Background()

			b.ReportAllocs()
			b.ResetTimer()

			for b.Loop() {
				txn, _ := db.NewTransaction(ctx, storage.WriteParams)
				internalTxn := txn.(*transaction)

				// Independent paths - pathIndex will be used from pool
				for j := range size {
					path := storage.MustParsePath(fmt.Sprintf("/key_%d", j))
					_ = internalTxn.Write(storage.AddOp, path, fmt.Sprintf("value_%d", j))
				}

				_ = db.Commit(ctx, txn)
			}
		})
	}
}

// BenchmarkPoolRecycling verifies that pools are actually recycling objects.
func BenchmarkPoolRecycling(b *testing.B) {
	b.Run("pathIndex_get_put", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()

		for b.Loop() {
			m := getPathIndex()
			m["test1"] = 1
			m["test2"] = 2
			putPathIndex(m)
		}
	})

	b.Run("intSlice_get_put", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()

		for b.Loop() {
			s := getIntSlice()
			*s = append(*s, 1, 2, 3)
			putIntSlice(s)
		}
	})
}
