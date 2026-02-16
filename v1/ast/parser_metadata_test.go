// Copyright 2026 The OPA Authors.  All rights reserved.
// Use of this source code is governed by an Apache2
// license that can be found in the LICENSE file.

package ast

import (
	"testing"
)

func TestParserMetadata_PrintCalls(t *testing.T) {
	tests := []struct {
		name             string
		input            string
		expectPrintCalls bool
		expectCount      int
	}{
		{
			name: "module with print call",
			input: `package test
allow if {
	print("hello")
	input.user == "admin"
}`,
			expectPrintCalls: true,
			expectCount:      1,
		},
		{
			name: "module with multiple print calls",
			input: `package test
allow if {
	print("first")
	print("second")
	input.user == "admin"
}`,
			expectPrintCalls: true,
			expectCount:      2,
		},
		{
			name: "module without print calls",
			input: `package test
allow if {
	input.user == "admin"
}`,
			expectPrintCalls: false,
			expectCount:      0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := ParserOptions{
				CollectMetadata: true,
			}

			mod, err := ParseModuleWithOpts("test.rego", tt.input, opts)
			if err != nil {
				t.Fatalf("Failed to parse module: %v", err)
			}

			if mod.Metadata == nil {
				t.Fatal("Expected metadata to be collected, got nil")
			}

			if mod.Metadata.HasPrintCalls() != tt.expectPrintCalls {
				t.Errorf("Expected HasPrintCalls=%v, got %v",
					tt.expectPrintCalls, mod.Metadata.HasPrintCalls())
			}

			if mod.Metadata.PrintCallCount() != tt.expectCount {
				t.Errorf("Expected PrintCallCount=%d, got %d",
					tt.expectCount, mod.Metadata.PrintCallCount())
			}
		})
	}
}

func TestParserMetadata_TemplateStrings(t *testing.T) {
	tests := []struct {
		name                  string
		input                 string
		expectTemplateStrings bool
		expectCount           int
	}{
		{
			name: "module with template string",
			input: `package test

message := "world"
greeting := $"Hello {message}"
`,
			expectTemplateStrings: true,
			expectCount:           1,
		},
		{
			name: "module without template strings",
			input: `package test

message := "Hello world"
`,
			expectTemplateStrings: false,
			expectCount:           0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := ParserOptions{
				CollectMetadata: true,
			}

			mod, err := ParseModuleWithOpts("test.rego", tt.input, opts)
			if err != nil {
				t.Fatalf("Failed to parse module: %v", err)
			}

			if mod.Metadata == nil {
				t.Fatal("Expected metadata to be collected, got nil")
			}

			if mod.Metadata.HasTemplateStrings() != tt.expectTemplateStrings {
				t.Errorf("Expected HasTemplateStrings=%v, got %v",
					tt.expectTemplateStrings, mod.Metadata.HasTemplateStrings())
			}

			if mod.Metadata.TemplateStringCount() != tt.expectCount {
				t.Errorf("Expected TemplateStringCount=%d, got %d",
					tt.expectCount, mod.Metadata.TemplateStringCount())
			}
		})
	}
}

func TestParserMetadata_FunctionRefs(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		expectCount int
		expectRefs  []string
	}{
		{
			name: "module with function calls",
			input: `package test
allow if {
	count(input.users) > 0
	startswith(input.path, "/api")
}`,
			// Note: Also captures operators like 'gt' (>), so we expect 3 refs total
			expectCount: 3,
			expectRefs:  []string{"count", "startswith", "gt"},
		},
		{
			name: "module without function calls",
			input: `package test
allow if {
	input.user == "admin"
}`,
			// Note: The '==' operator is captured as 'equal', so we expect 1 ref
			expectCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := ParserOptions{
				CollectMetadata: true,
			}

			mod, err := ParseModuleWithOpts("test.rego", tt.input, opts)
			if err != nil {
				t.Fatalf("Failed to parse module: %v", err)
			}

			if mod.Metadata == nil {
				t.Fatal("Expected metadata to be collected, got nil")
			}

			if mod.Metadata.FunctionRefCount() != tt.expectCount {
				t.Errorf("Expected %d function refs, got %d",
					tt.expectCount, mod.Metadata.FunctionRefCount())
			}

			// Verify specific refs if provided
			if len(tt.expectRefs) > 0 {
				refMap := make(map[string]bool)
				for _, ref := range mod.Metadata.FunctionRefs() {
					refMap[ref.String()] = true
				}

				for _, expected := range tt.expectRefs {
					if !refMap[expected] {
						t.Errorf("Expected to find function ref %q, but it was not collected", expected)
					}
				}
			}
		})
	}
}

func TestParserMetadata_BuiltinTracking(t *testing.T) {
	input := `package test
allow if {
	count(input.users) > 10
	print("checking users")
	startswith(input.path, "/api")
}`

	opts := ParserOptions{
		CollectMetadata: true,
	}

	mod, err := ParseModuleWithOpts("test.rego", input, opts)
	if err != nil {
		t.Fatalf("Failed to parse module: %v", err)
	}

	if mod.Metadata == nil {
		t.Fatal("Expected metadata to be collected, got nil")
	}

	// Check that builtins are tracked
	expectedBuiltins := []string{"count", "print", "startswith"}
	for _, builtin := range expectedBuiltins {
		if !mod.Metadata.HasBuiltin(builtin) {
			t.Errorf("Expected builtin %q to be tracked, but it was not", builtin)
		}
	}

	// Verify non-builtin is not tracked
	if mod.Metadata.HasBuiltin("nonexistent") {
		t.Error("Did not expect non-existent builtin to be tracked")
	}
}

func TestParserMetadata_WithoutCollection(t *testing.T) {
	input := `package test
allow if {
	print("hello")
	input.user == "admin"
}`

	// Parse WITHOUT metadata collection (default behavior)
	opts := ParserOptions{
		CollectMetadata: false,
	}

	mod, err := ParseModuleWithOpts("test.rego", input, opts)
	if err != nil {
		t.Fatalf("Failed to parse module: %v", err)
	}

	// Metadata should be nil when not collected
	if mod.Metadata != nil {
		t.Error("Expected metadata to be nil when CollectMetadata=false")
	}
}

func TestParserMetadata_BackwardCompatibility(t *testing.T) {
	input := `package test
allow if {
	print("hello")
	input.user == "admin"
}`

	// Parse with default options (no metadata collection)
	mod, err := ParseModule("test.rego", input)
	if err != nil {
		t.Fatalf("Failed to parse module: %v", err)
	}

	// Metadata should be nil by default (backward compat)
	if mod.Metadata != nil {
		t.Error("Expected metadata to be nil by default for backward compatibility")
	}
}
