// Copyright 2016 The OPA Authors.  All rights reserved.
// Use of this source code is governed by an Apache2
// license that can be found in the LICENSE file.

package ast

import (
	"fmt"
	"testing"
)

// BenchmarkValueEqual benchmarks the ValueEqual function with realistic scenarios
// that mirror actual OPA policy evaluation patterns.
func BenchmarkValueEqual(b *testing.B) {
	// Prepare test values that represent realistic OPA data

	// Simple scalar values (most common in policies)
	nullVal := NullValue
	boolTrue := Boolean(true)
	boolFalse := Boolean(false)
	str1 := String("users")
	str2 := String("roles")
	strLong := String("this_is_a_longer_string_representing_a_resource_path")
	num1 := Number("42")
	num2 := Number("100")
	numFloat := Number("3.14159")
	var1 := Var("x")
	var2 := Var("y")
	varWild := Var("_")

	// Complex values (less common but still important)
	arr1 := NewArray(StringTerm("a"), StringTerm("b"), StringTerm("c"))
	arr2 := NewArray(StringTerm("a"), StringTerm("b"), StringTerm("c"))
	arr3 := NewArray(StringTerm("x"), StringTerm("y"), StringTerm("z"))

	ref1 := Ref{StringTerm("data"), StringTerm("users"), NumberTerm("0")}
	ref2 := Ref{StringTerm("data"), StringTerm("users"), NumberTerm("0")}
	ref3 := Ref{StringTerm("data"), StringTerm("roles"), NumberTerm("1")}

	obj1 := NewObject(
		Item(StringTerm("name"), StringTerm("alice")),
		Item(StringTerm("role"), StringTerm("admin")),
	)
	obj2 := NewObject(
		Item(StringTerm("name"), StringTerm("alice")),
		Item(StringTerm("role"), StringTerm("admin")),
	)
	obj3 := NewObject(
		Item(StringTerm("name"), StringTerm("bob")),
		Item(StringTerm("role"), StringTerm("user")),
	)

	// Test cases representing realistic comparison patterns in OPA
	testCases := []struct {
		name string
		a    Value
		b    Value
	}{
		// Null comparisons (common in existence checks)
		{"Null_Equal", nullVal, nullVal},
		{"Null_NotEqual", nullVal, boolTrue},

		// Boolean comparisons (extremely common in conditionals)
		{"Boolean_Equal_True", boolTrue, boolTrue},
		{"Boolean_Equal_False", boolFalse, boolFalse},
		{"Boolean_NotEqual", boolTrue, boolFalse},
		{"Boolean_DifferentType", boolTrue, str1},

		// String comparisons (very common - resource names, user IDs, etc.)
		{"String_Equal_Short", str1, String("users")},
		{"String_NotEqual_Short", str1, str2},
		{"String_Equal_Long", strLong, String("this_is_a_longer_string_representing_a_resource_path")},
		{"String_DifferentType", str1, num1},

		// Number comparisons (common in numeric constraints)
		{"Number_Equal_Int", num1, Number("42")},
		{"Number_NotEqual_Int", num1, num2},
		{"Number_Equal_Float", numFloat, Number("3.14159")},
		{"Number_DifferentType", num1, str1},

		// Var comparisons (common in unification)
		{"Var_Equal", var1, Var("x")},
		{"Var_NotEqual", var1, var2},
		{"Var_Wildcard", varWild, Var("_")},
		{"Var_DifferentType", var1, str1},

		// Array comparisons (moderate frequency)
		{"Array_Equal", arr1, arr2},
		{"Array_NotEqual", arr1, arr3},
		{"Array_DifferentType", arr1, str1},

		// Ref comparisons (common in rule heads and bodies)
		{"Ref_Equal", ref1, ref2},
		{"Ref_NotEqual", ref1, ref3},
		{"Ref_DifferentType", ref1, str1},

		// Object comparisons (moderate frequency - JSON documents)
		{"Object_Equal", obj1, obj2},
		{"Object_NotEqual", obj1, obj3},
		{"Object_DifferentType", obj1, str1},
	}

	// Run benchmarks for each case
	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				_ = ValueEqual(tc.a, tc.b)
			}
		})
	}
}

// BenchmarkValueEqualMixed simulates a mixed workload typical in policy evaluation
// where different types are compared in sequence (e.g., during rule evaluation)
func BenchmarkValueEqualMixed(b *testing.B) {
	values := []Value{
		NullValue,
		Boolean(true),
		Boolean(false),
		String("user"),
		String("admin"),
		Number("42"),
		Number("100"),
		Var("x"),
		Var("y"),
		NewArray(StringTerm("a"), StringTerm("b")),
		Ref{StringTerm("data"), StringTerm("users")},
		NewObject(Item(StringTerm("name"), StringTerm("alice"))),
	}

	b.ReportAllocs()
	b.ResetTimer()

	n := len(values)
	for i := range b.N {
		// Compare each value with the next one in a circular fashion
		// This simulates iterating through policy rules
		a := values[i%n]
		b := values[(i+1)%n]
		_ = ValueEqual(a, b)
	}
}

// BenchmarkValueEqualHotPath focuses on the most common types in real policies
// Based on typical OPA usage: strings, numbers, booleans are 80%+ of comparisons
func BenchmarkValueEqualHotPath(b *testing.B) {
	testCases := []struct {
		name   string
		values []Value
	}{
		{
			name: "Strings_MostCommon",
			values: []Value{
				String("users"),
				String("users"),
				String("admin"),
				String("read"),
				String("write"),
				String("resource:123"),
			},
		},
		{
			name: "Numbers_Common",
			values: []Value{
				Number("0"),
				Number("1"),
				Number("42"),
				Number("100"),
				Number("3.14"),
			},
		},
		{
			name: "Booleans_Common",
			values: []Value{
				Boolean(true),
				Boolean(false),
				Boolean(true),
				Boolean(true),
			},
		},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			n := len(tc.values)
			for i := range b.N {
				a := tc.values[i%n]
				b := tc.values[(i+1)%n]
				_ = ValueEqual(a, b)
			}
		})
	}
}

// BenchmarkValueEqualTypeCheck benchmarks the specific overhead of type checking
// which is what we're optimizing by eliminating the double-switch
func BenchmarkValueEqualTypeCheck(b *testing.B) {
	// Create pairs of same-type values to isolate type-check overhead
	testCases := []struct {
		name string
		a, b Value
	}{
		{"Null", NullValue, NullValue},
		{"Boolean", Boolean(true), Boolean(true)},
		{"String", String("test"), String("test")},
		{"Number", Number("42"), Number("42")},
		{"Var", Var("x"), Var("x")},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				_ = ValueEqual(tc.a, tc.b)
			}
		})
	}
}

// BenchmarkValueEqualLargeArray tests array comparison performance
// to ensure our optimization doesn't negatively impact complex types
func BenchmarkValueEqualLargeArray(b *testing.B) {
	sizes := []int{10, 100, 1000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("Array_%d_elements", size), func(b *testing.B) {
			// Create two identical arrays
			terms := make([]*Term, size)
			for i := range size {
				terms[i] = IntNumberTerm(i)
			}
			arr1 := NewArray(terms...)
			arr2 := NewArray(terms...)

			b.ReportAllocs()
			b.ResetTimer()

			for b.Loop() {
				_ = ValueEqual(arr1, arr2)
			}
		})
	}
}
