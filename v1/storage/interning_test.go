// Copyright 2024 The OPA Authors.  All rights reserved.
// Use of this source code is governed by an Apache2
// license that can be found in the LICENSE file.

package storage

import (
	"strconv"
	"testing"
	"unsafe"
)

func TestInternPathSegment(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		wantSame bool
	}{
		{
			name:     "common segment 'data'",
			input:    "data",
			wantSame: true,
		},
		{
			name:     "common segment 'input'",
			input:    "input",
			wantSame: true,
		},
		{
			name:     "numeric segment '0'",
			input:    "0",
			wantSame: true,
		},
		{
			name:     "numeric segment '999'",
			input:    "999",
			wantSame: true,
		},
		{
			name:     "numeric segment '1000' (not pre-interned but will be interned)",
			input:    "1000",
			wantSame: true,
		},
		{
			name:     "custom segment",
			input:    "custom_segment",
			wantSame: true,
		},
		{
			name:     "long segment over 64 chars (returned as-is, not added to map but Go may intern literals)",
			input:    "this_is_a_very_long_segment_that_exceeds_sixty_four_characters_in_length_and_should_not_be_interned",
			wantSame: true, // Go compiler may intern string literals, so pointer equality is possible
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s1 := InternPathSegment(tt.input)
			s2 := InternPathSegment(tt.input)

			// Check value equality
			if s1 != s2 {
				t.Errorf("Expected values to be equal: %q != %q", s1, s2)
			}

			// Check pointer equality
			if tt.wantSame {
				if unsafe.StringData(s1) != unsafe.StringData(s2) {
					t.Errorf("Expected interned strings to share pointer for %q", tt.input)
				}
			} else {
				if unsafe.StringData(s1) == unsafe.StringData(s2) {
					t.Errorf("Expected non-interned strings to have different pointers for %q", tt.input)
				}
			}
		})
	}
}

func TestInternPathSegmentNumbers(t *testing.T) {
	// Test that numbers 0-999 are pre-interned
	for i := range 1000 {
		s := strconv.Itoa(i)
		s1 := InternPathSegment(s)
		s2 := InternPathSegment(s)

		if s1 != s2 {
			t.Errorf("Expected number %d to have equal values", i)
		}

		if unsafe.StringData(s1) != unsafe.StringData(s2) {
			t.Errorf("Expected number %d to be interned", i)
		}
	}
}

func TestPathStringWithEscape(t *testing.T) {
	tests := []struct {
		path Path
		want string
	}{
		{Path{}, "/"},
		{Path{"foo"}, "/foo"},
		{Path{"hello world"}, "/hello%20world"},
		{Path{"foo/bar"}, "/foo%2Fbar"},
		{Path{"data", "users", "0"}, "/data/users/0"},
		{Path{"a", "b c", "d"}, "/a/b%20c/d"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			got := tt.path.String()
			if got != tt.want {
				t.Errorf("Path.String() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestParsePathEscaped(t *testing.T) {
	tests := []struct {
		input string
		want  Path
		ok    bool
	}{
		{"/", Path{}, true},
		{"/foo", Path{"foo"}, true},
		{"/hello%20world", Path{"hello world"}, true},
		{"/foo%2Fbar", Path{"foo/bar"}, true},
		{"/data/users/0", Path{"data", "users", "0"}, true},
		{"/a/b%20c/d", Path{"a", "b c", "d"}, true},
		{"invalid", nil, false},
		{"", nil, false},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, ok := ParsePathEscaped(tt.input)
			if ok != tt.ok {
				t.Errorf("ParsePathEscaped(%q) ok = %v, want %v", tt.input, ok, tt.ok)
				return
			}
			if !ok {
				return
			}
			if !got.Equal(tt.want) {
				t.Errorf("ParsePathEscaped(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

func TestPathEqualWithInterning(t *testing.T) {
	// Create paths with interned segments
	p1 := Path{InternPathSegment("data"), InternPathSegment("users"), InternPathSegment("0")}
	p2 := Path{InternPathSegment("data"), InternPathSegment("users"), InternPathSegment("0")}

	if !p1.Equal(p2) {
		t.Error("Expected interned paths to be equal")
	}

	// Verify pointer equality for interned strings
	for i := range p1 {
		if unsafe.StringData(p1[i]) != unsafe.StringData(p2[i]) {
			t.Errorf("Expected segment %d to share pointer", i)
		}
	}
}

func BenchmarkInternPathSegment(b *testing.B) {
	segments := []string{"data", "input", "config", "0", "1", "999", "custom"}

	b.Run("PreInterned", func(b *testing.B) {
		for b.Loop() {
			for _, seg := range segments {
				_ = InternPathSegment(seg)
			}
		}
	})

	b.Run("NewSegment", func(b *testing.B) {
		for i := 0; b.Loop(); i++ {
			_ = InternPathSegment(strconv.Itoa(i))
		}
	})

	b.Run("LongSegment", func(b *testing.B) {
		long := "this_is_a_very_long_segment_that_exceeds_sixty_four_characters"
		for b.Loop() {
			_ = InternPathSegment(long)
		}
	})
}

func BenchmarkPathStringWithInterning(b *testing.B) {
	paths := []Path{
		{"data"},
		{"data", "users", "0"},
		{"hello world", "foo/bar"},
		{"a", "b", "c", "d", "e"},
	}

	for _, p := range paths {
		b.Run(p.String(), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				_ = p.String()
			}
		})
	}
}

func BenchmarkParsePathEscapedWithInterning(b *testing.B) {
	paths := []string{
		"/data",
		"/data/users/0",
		"/hello%20world/foo%2Fbar",
		"/a/b/c/d/e",
	}

	for _, path := range paths {
		b.Run(path, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if _, ok := ParsePathEscaped(path); !ok {
					b.Fatalf("failed to parse escaped path: %s", path)
				}
			}
		})
	}
}

func BenchmarkPathEqualWithInterning(b *testing.B) {
	p1 := Path{InternPathSegment("data"), InternPathSegment("users"), InternPathSegment("0")}
	p2 := Path{InternPathSegment("data"), InternPathSegment("users"), InternPathSegment("0")}

	
	b.ReportAllocs()
	for b.Loop() {
		_ = p1.Equal(p2)
	}
}
