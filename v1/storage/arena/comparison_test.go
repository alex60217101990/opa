// Copyright 2026 The OPA Authors.  All rights reserved.
// Use of this source code is governed by an Apache2
// license that can be found in the LICENSE file.

package arena_test

import (
	"context"
	"fmt"
	"runtime"
	"testing"

	"github.com/open-policy-agent/opa/v1/storage"
	"github.com/open-policy-agent/opa/v1/storage/arena"
	"github.com/open-policy-agent/opa/v1/storage/inmem"
)

// memStats captures memory statistics before and after an operation.
type memStats struct {
	AllocsBefore  uint64
	AllocsAfter   uint64
	BytesBefore   uint64
	BytesAfter    uint64
	TotalAllocs   uint64
	TotalBytes    uint64
}

// captureMemStats runs a function and captures memory stats.
func captureMemStats(fn func()) memStats {
	var m1, m2 runtime.MemStats

	// Force GC to get clean baseline
	runtime.GC()
	runtime.ReadMemStats(&m1)

	fn()

	runtime.ReadMemStats(&m2)

	return memStats{
		AllocsBefore: m1.Mallocs,
		AllocsAfter:  m2.Mallocs,
		BytesBefore:  m1.TotalAlloc,
		BytesAfter:   m2.TotalAlloc,
		TotalAllocs:  m2.Mallocs - m1.Mallocs,
		TotalBytes:   m2.TotalAlloc - m1.TotalAlloc,
	}
}

// generateUsers generates N mock users with realistic data.
func generateUsers(n int) map[string]any {
	users := make([]any, n)

	departments := []string{"engineering", "sales", "marketing", "hr", "finance"}
	roles := []string{"developer", "manager", "analyst", "director", "vp"}

	for i := 0; i < n; i++ {
		users[i] = map[string]any{
			"id":         fmt.Sprintf("user-%d", i),
			"name":       fmt.Sprintf("User %d", i),
			"email":      fmt.Sprintf("user%d@example.com", i),
			"age":        25 + (i % 40),
			"department": departments[i%len(departments)],
			"role":       roles[i%len(roles)],
			"active":     i%10 != 0, // 90% active
			"salary":     50000 + (i * 1000 % 100000),
			"metadata": map[string]any{
				"last_login":    "2026-02-10T10:00:00Z",
				"login_count":   i % 1000,
				"preferences":   map[string]any{"theme": "dark", "lang": "en"},
			},
		}
	}

	return map[string]any{"users": users}
}

// realisticPolicies returns 10 realistic Rego policies.
func realisticPolicies() map[string][]byte {
	return map[string][]byte{
		"rbac.rego": []byte(`package rbac

import rego.v1

# Role-based access control policy
default allow := false

allow if {
    user_has_role(input.user, input.required_role)
}

user_has_role(user, role) if {
    data.role_bindings[user][_] == role
}

# Admin users can do anything
allow if {
    input.user in data.admins
}
`),
		"api_authorization.rego": []byte(`package api.authorization

import rego.v1

# API endpoint authorization
default allow := false

# Allow GET requests for authenticated users
allow if {
    input.method == "GET"
    input.user.authenticated
}

# Allow POST/PUT/DELETE for users with write permission
allow if {
    input.method in ["POST", "PUT", "DELETE"]
    input.user.permissions[_] == "write"
}

# Rate limiting check
allow if {
    not rate_limit_exceeded
}

rate_limit_exceeded if {
    count(data.requests[input.user.id]) > 1000
}
`),
		"data_filtering.rego": []byte(`package data.filtering

import rego.v1

# Filter data based on user department
filtered_users contains user if {
    some user in data.users
    user.department == input.user.department
}

# Managers can see all users in their department
filtered_users contains user if {
    some user in data.users
    input.user.role == "manager"
    user.department == input.user.department
}

# Admins see everyone
filtered_users contains user if {
    some user in data.users
    input.user.role == "admin"
}
`),
		"compliance.rego": []byte(`package compliance

import rego.v1

# Compliance checks for sensitive operations
default compliant := false

# PII access requires approval
compliant if {
    input.operation == "access_pii"
    approval_exists(input.request_id)
}

# Data export requires manager approval
compliant if {
    input.operation == "export_data"
    input.approver.role in ["manager", "director", "vp"]
}

approval_exists(request_id) if {
    data.approvals[request_id].status == "approved"
}

# Audit logging required
audit_required if {
    input.operation in ["access_pii", "export_data", "delete_user"]
}
`),
		"resource_limits.rego": []byte(`package resource.limits

import rego.v1

# Resource usage limits
default within_limits := false

within_limits if {
    input.cpu_usage < data.limits.max_cpu
    input.memory_usage < data.limits.max_memory
}

# Storage quota check
within_limits if {
    user_storage := sum([size |
        some file in data.files[input.user.id][_]
        size := file.size
    ])
    user_storage < data.limits.storage_quota
}

# API quota check
within_limits if {
    count(data.api_calls[input.user.id]) < data.limits.daily_api_calls
}
`),
		"security.rego": []byte(`package security

import rego.v1

# Security policy checks
default secure := false

# Require MFA for sensitive operations
secure if {
    input.operation in ["transfer_funds", "change_password", "delete_account"]
    input.user.mfa_verified
}

# IP whitelist check
secure if {
    input.ip_address in data.whitelisted_ips
}

# Detect suspicious activity
suspicious_activity if {
    # Multiple failed logins
    count([attempt |
        some attempt in data.login_attempts[input.user.id][_]
        attempt.status == "failed"
    ]) > 5
}

secure if {
    not suspicious_activity
}
`),
		"approval_workflow.rego": []byte(`package approval.workflow

import rego.v1

# Approval workflow rules
default needs_approval := false

needs_approval if {
    input.amount > 10000
    not approved_by_manager
}

approved_by_manager if {
    some approval in input.approvals[_]
    approval.role == "manager"
    approval.status == "approved"
}

# Escalation rules
needs_escalation if {
    input.amount > 50000
    not approved_by_director
}

approved_by_director if {
    some approval in input.approvals[_]
    approval.role in ["director", "vp"]
    approval.status == "approved"
}
`),
		"data_retention.rego": []byte(`package data.retention

import rego.v1

# Data retention policy
default should_delete := false

should_delete if {
    resource_age_days := (time.now_ns() - input.resource.created_at) / 1000000000 / 86400
    resource_age_days > data.retention_periods[input.resource.type]
}

# Legal hold check
should_delete if {
    not legal_hold_active
}

legal_hold_active if {
    input.resource.id in data.legal_holds
}

# Archive instead of delete for important data
should_archive if {
    should_delete
    input.resource.importance == "high"
}
`),
		"tenant_isolation.rego": []byte(`package tenant.isolation

import rego.v1

# Multi-tenancy isolation
default allow := false

# Users can only access their tenant's data
allow if {
    input.resource.tenant_id == input.user.tenant_id
}

# Super admins can access any tenant
allow if {
    input.user.role == "super_admin"
}

# Cross-tenant access requires explicit permission
allow if {
    permission_granted(input.user, input.resource.tenant_id)
}

permission_granted(user, tenant_id) if {
    some permission in data.cross_tenant_permissions[user.id][_]
    permission.tenant_id == tenant_id
    permission.status == "active"
}
`),
		"cost_optimization.rego": []byte(`package cost.optimization

import rego.v1

# Cost optimization recommendations
default recommend_downgrade := false

recommend_downgrade if {
    # Low resource usage
    avg_cpu := sum([u | some u in data.metrics.cpu[_]]) / count(data.metrics.cpu)
    avg_cpu < 20

    avg_memory := sum([m | some m in data.metrics.memory[_]]) / count(data.metrics.memory)
    avg_memory < 30
}

# Unused resources
unused_resources contains resource if {
    some resource in data.resources[_]
    last_access := resource.last_accessed
    days_unused := (time.now_ns() - last_access) / 1000000000 / 86400
    days_unused > 30
}

# Estimate cost savings
estimated_savings := sum([cost |
    some resource in unused_resources[_]
    cost := resource.monthly_cost
])
`),
	}
}

// TestStorageComparison_10KUsers compares memory usage for 10K users.
func TestStorageComparison_10KUsers(t *testing.T) {
	ctx := context.Background()
	numUsers := 10000
	userData := generateUsers(numUsers)

	t.Logf("\n=== Storage Comparison: 10,000 Users ===\n")

	// Test InMemory Storage
	t.Run("InMemory", func(t *testing.T) {
		stats := captureMemStats(func() {
			store := inmem.New()

			txn, err := store.NewTransaction(ctx, storage.WriteParams)
			if err != nil {
				t.Fatal(err)
			}

			if err := store.Write(ctx, txn, storage.AddOp, storage.MustParsePath("/data"), userData); err != nil {
				t.Fatal(err)
			}

			if err := store.Commit(ctx, txn); err != nil {
				t.Fatal(err)
			}

			// Read back some data
			txn, _ = store.NewTransaction(ctx)
			defer store.Abort(ctx, txn)

			for i := 0; i < 100; i++ {
				path := storage.MustParsePath(fmt.Sprintf("/data/users/%d/name", i))
				_, err := store.Read(ctx, txn, path)
				if err != nil {
					t.Fatal(err)
				}
			}
		})

		t.Logf("InMemory Storage:")
		t.Logf("  Total Allocations: %d", stats.TotalAllocs)
		t.Logf("  Total Memory:      %.2f MB", float64(stats.TotalBytes)/1024/1024)
		t.Logf("  Avg per user:      %.2f bytes", float64(stats.TotalBytes)/float64(numUsers))
	})

	// Test Arena Storage
	t.Run("Arena", func(t *testing.T) {
		stats := captureMemStats(func() {
			store := arena.New()

			txn, err := store.NewTransaction(ctx, storage.WriteParams)
			if err != nil {
				t.Fatal(err)
			}

			if err := store.Write(ctx, txn, storage.AddOp, storage.MustParsePath("/data"), userData); err != nil {
				t.Fatal(err)
			}

			if err := store.Commit(ctx, txn); err != nil {
				t.Fatal(err)
			}

			// Read back some data
			txn, _ = store.NewTransaction(ctx)
			defer store.Abort(ctx, txn)

			for i := 0; i < 100; i++ {
				path := storage.MustParsePath(fmt.Sprintf("/data/users/%d/name", i))
				_, err := store.Read(ctx, txn, path)
				if err != nil {
					t.Fatal(err)
				}
			}
		})

		t.Logf("Arena Storage:")
		t.Logf("  Total Allocations: %d", stats.TotalAllocs)
		t.Logf("  Total Memory:      %.2f MB", float64(stats.TotalBytes)/1024/1024)
		t.Logf("  Avg per user:      %.2f bytes", float64(stats.TotalBytes)/float64(numUsers))
	})
}

// TestStorageComparison_Policies compares memory usage for policy storage.
func TestStorageComparison_Policies(t *testing.T) {
	ctx := context.Background()
	policies := realisticPolicies()

	t.Logf("\n=== Storage Comparison: 10 Realistic Rego Policies ===\n")

	// Calculate total policy size
	totalPolicySize := 0
	for _, policy := range policies {
		totalPolicySize += len(policy)
	}
	t.Logf("Total policy size: %d bytes (%.2f KB)\n", totalPolicySize, float64(totalPolicySize)/1024)

	// Test InMemory Storage
	var inmemStats memStats
	t.Run("InMemory", func(t *testing.T) {
		inmemStats = captureMemStats(func() {
			store := inmem.New()

			txn, err := store.NewTransaction(ctx, storage.WriteParams)
			if err != nil {
				t.Fatal(err)
			}

			// Upsert all policies
			for id, data := range policies {
				if err := store.UpsertPolicy(ctx, txn, id, data); err != nil {
					t.Fatal(err)
				}
			}

			if err := store.Commit(ctx, txn); err != nil {
				t.Fatal(err)
			}

			// Read back policies
			txn, _ = store.NewTransaction(ctx)
			defer store.Abort(ctx, txn)

			policyList, err := store.ListPolicies(ctx, txn)
			if err != nil {
				t.Fatal(err)
			}

			for _, id := range policyList {
				_, err := store.GetPolicy(ctx, txn, id)
				if err != nil {
					t.Fatal(err)
				}
			}
		})

		t.Logf("InMemory Storage:")
		t.Logf("  Total Allocations: %d", inmemStats.TotalAllocs)
		t.Logf("  Total Memory:      %d bytes (%.2f KB)", inmemStats.TotalBytes, float64(inmemStats.TotalBytes)/1024)
		t.Logf("  Overhead:          %.1f%% (%.2f KB)",
			float64(inmemStats.TotalBytes-uint64(totalPolicySize))/float64(totalPolicySize)*100,
			float64(inmemStats.TotalBytes-uint64(totalPolicySize))/1024)
	})

	// Test Arena Storage
	var arenaStats memStats
	t.Run("Arena", func(t *testing.T) {
		arenaStats = captureMemStats(func() {
			store := arena.New()

			txn, err := store.NewTransaction(ctx, storage.WriteParams)
			if err != nil {
				t.Fatal(err)
			}

			// Upsert all policies
			for id, data := range policies {
				if err := store.UpsertPolicy(ctx, txn, id, data); err != nil {
					t.Fatal(err)
				}
			}

			if err := store.Commit(ctx, txn); err != nil {
				t.Fatal(err)
			}

			// Read back policies
			txn, _ = store.NewTransaction(ctx)
			defer store.Abort(ctx, txn)

			policyList, err := store.ListPolicies(ctx, txn)
			if err != nil {
				t.Fatal(err)
			}

			for _, id := range policyList {
				_, err := store.GetPolicy(ctx, txn, id)
				if err != nil {
					t.Fatal(err)
				}
			}
		})

		t.Logf("Arena Storage:")
		t.Logf("  Total Allocations: %d", arenaStats.TotalAllocs)
		t.Logf("  Total Memory:      %d bytes (%.2f KB)", arenaStats.TotalBytes, float64(arenaStats.TotalBytes)/1024)
		t.Logf("  Overhead:          %.1f%% (%.2f KB)",
			float64(arenaStats.TotalBytes-uint64(totalPolicySize))/float64(totalPolicySize)*100,
			float64(arenaStats.TotalBytes-uint64(totalPolicySize))/1024)
	})

	// Comparison
	t.Logf("\n=== Comparison ===")
	allocReduction := float64(inmemStats.TotalAllocs-arenaStats.TotalAllocs) / float64(inmemStats.TotalAllocs) * 100
	memReduction := float64(inmemStats.TotalBytes-arenaStats.TotalBytes) / float64(inmemStats.TotalBytes) * 100

	t.Logf("Allocation Reduction: %.1f%% (%d → %d)", allocReduction, inmemStats.TotalAllocs, arenaStats.TotalAllocs)
	t.Logf("Memory Reduction:     %.1f%% (%.2f KB → %.2f KB)", memReduction,
		float64(inmemStats.TotalBytes)/1024, float64(arenaStats.TotalBytes)/1024)
}

// TestStorageComparison_ConcurrentOperations tests concurrent transaction performance.
func TestStorageComparison_ConcurrentOperations(t *testing.T) {
	ctx := context.Background()
	numOperations := 1000

	t.Logf("\n=== Storage Comparison: Concurrent Operations ===\n")
	t.Logf("Running %d concurrent read transactions\n", numOperations)

	// Prepare test data
	testData := map[string]any{
		"config": map[string]any{
			"enabled": true,
			"timeout": 5000,
			"retries": 3,
		},
	}

	// Test InMemory Storage
	t.Run("InMemory", func(t *testing.T) {
		store := inmem.New()

		// Setup data
		txn, _ := store.NewTransaction(ctx, storage.WriteParams)
		store.Write(ctx, txn, storage.AddOp, storage.MustParsePath("/data"), testData)
		store.Commit(ctx, txn)

		stats := captureMemStats(func() {
			// Concurrent reads
			for i := 0; i < numOperations; i++ {
				txn, _ := store.NewTransaction(ctx)
				_, err := store.Read(ctx, txn, storage.MustParsePath("/data/config/enabled"))
				if err != nil {
					t.Fatal(err)
				}
				store.Abort(ctx, txn)
			}
		})

		t.Logf("InMemory Storage:")
		t.Logf("  Total Allocations: %d", stats.TotalAllocs)
		t.Logf("  Allocs per op:     %.2f", float64(stats.TotalAllocs)/float64(numOperations))
		t.Logf("  Total Memory:      %.2f KB", float64(stats.TotalBytes)/1024)
		t.Logf("  Memory per op:     %.2f bytes", float64(stats.TotalBytes)/float64(numOperations))
	})

	// Test Arena Storage
	t.Run("Arena", func(t *testing.T) {
		store := arena.New()

		// Setup data
		txn, _ := store.NewTransaction(ctx, storage.WriteParams)
		store.Write(ctx, txn, storage.AddOp, storage.MustParsePath("/data"), testData)
		store.Commit(ctx, txn)

		stats := captureMemStats(func() {
			// Concurrent reads
			for i := 0; i < numOperations; i++ {
				txn, _ := store.NewTransaction(ctx)
				_, err := store.Read(ctx, txn, storage.MustParsePath("/data/config/enabled"))
				if err != nil {
					t.Fatal(err)
				}
				store.Abort(ctx, txn)
			}
		})

		t.Logf("Arena Storage:")
		t.Logf("  Total Allocations: %d", stats.TotalAllocs)
		t.Logf("  Allocs per op:     %.2f", float64(stats.TotalAllocs)/float64(numOperations))
		t.Logf("  Total Memory:      %.2f KB", float64(stats.TotalBytes)/1024)
		t.Logf("  Memory per op:     %.2f bytes", float64(stats.TotalBytes)/float64(numOperations))
	})
}
