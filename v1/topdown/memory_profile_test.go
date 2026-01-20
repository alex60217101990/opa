// Copyright 2026 The OPA Authors. All rights reserved.
// Use of this source code is governed by an Apache2
// license that can be found in the LICENSE file.

package topdown

import (
	"context"
	"fmt"
	"testing"

	"github.com/open-policy-agent/opa/v1/ast"
	"github.com/open-policy-agent/opa/v1/storage"
	inmem "github.com/open-policy-agent/opa/v1/storage/inmem/test"
)

// BenchmarkMemoryBaseline - baseline memory profiling для понимания bottleneck
func BenchmarkMemoryBaseline(b *testing.B) {
	sizes := []int{1000, 5000, 10000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
			ctx := context.Background()

			// Создаем большую вложенную структуру данных
			data := generateLargeDataset(size)
			store := inmem.NewFromObject(map[string]any{"users": data})

			// Реальный Rego запрос с comprehension
			module := `
			package test

			# Фильтрация пользователей
			active_users := [user |
				user := data.users[_]
				user.profile.active == true
			]

			# Подсчет активных
			count_active := count([1 |
				user := data.users[_]
				user.profile.active == true
			])

			# Доступ к вложенным полям
			premium_count := count([1 |
				user := data.users[_]
				user.profile.settings.subscription.tier == "premium"
			])
			`

			query := ast.MustParseBody("data.test")
			compiler := ast.MustCompileModules(map[string]string{
				"test.rego": module,
			})

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				err := storage.Txn(ctx, store, storage.TransactionParams{}, func(txn storage.Transaction) error {
					q := NewQuery(query).
						WithCompiler(compiler).
						WithStore(store).
						WithTransaction(txn)

					_, err := q.Run(ctx)
					return err
				})

				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func generateLargeDataset(count int) map[string]any {
	data := make(map[string]any, count)

	for i := 0; i < count; i++ {
		userKey := fmt.Sprintf("user_%d", i)
		data[userKey] = map[string]any{
			"id":    i,
			"name":  fmt.Sprintf("User %d", i),
			"email": fmt.Sprintf("user%d@example.com", i),
			"profile": map[string]any{
				"active": i%3 == 0,
				"age":    20 + (i % 50),
				"tags":   []any{"tag1", "tag2", "tag3"},
				"settings": map[string]any{
					"theme":    "dark",
					"language": "en",
					"subscription": map[string]any{
						"tier":    []string{"free", "basic", "premium"}[i%3],
						"expires": "2027-01-01",
					},
				},
			},
			"metadata": map[string]any{
				"created": "2026-01-01",
				"updated": "2026-01-20",
			},
		}
	}

	return data
}
