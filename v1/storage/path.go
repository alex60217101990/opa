// Copyright 2016 The OPA Authors.  All rights reserved.
// Use of this source code is governed by an Apache2
// license that can be found in the LICENSE file.

package storage

import (
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"unsafe"

	"github.com/open-policy-agent/opa/v1/ast"
)

// RootPath refers to the root document in storage.
var RootPath = Path{}

// Path refers to a document in storage.
type Path []string

// ParsePath returns a new path for the given str.
func ParsePath(str string) (path Path, ok bool) {
	if len(str) == 0 || str[0] != '/' {
		return nil, false
	}
	if len(str) == 1 {
		return Path{}, true
	}

	segments := strings.Split(str[1:], "/")
	path = make(Path, len(segments))

	// Intern each segment
	for i, seg := range segments {
		path[i] = InternPathSegment(seg)
	}

	return path, true
}

func hexToInt(c byte) (int, bool) {
	switch {
	case '0' <= c && c <= '9':
		return int(c - '0'), true
	case 'a' <= c && c <= 'f':
		return int(c - 'a' + 10), true
	case 'A' <= c && c <= 'F':
		return int(c - 'A' + 10), true
	}
	return 0, false
}

func unescapePathSegment(s string) (string, error) {
	// Fast path: no escaping
	if !strings.ContainsRune(s, '%') {
		return InternPathSegment(s), nil
	}

	// Slow path: has escaping
	sb := sbPool.Get()
	defer sbPool.Put(sb)
	sb.Grow(len(s))

	for i := 0; i < len(s); i++ {
		if s[i] == '%' {
			if i+2 >= len(s) {
				return "", fmt.Errorf("invalid escape sequence")
			}

			h1, ok1 := hexToInt(s[i+1])
			h2, ok2 := hexToInt(s[i+2])
			if !ok1 || !ok2 {
				return "", fmt.Errorf("invalid escape sequence")
			}

			sb.WriteByte(byte(h1<<4 | h2))
			i += 2
		} else {
			sb.WriteByte(s[i])
		}
	}

	result := sb.String()
	return InternPathSegment(result), nil
}

// ParsePathEscaped returns a new path for the given escaped str.
func ParsePathEscaped(str string) (path Path, ok bool) {
	if path, ok = ParsePath(str); !ok {
		return nil, false
	}

	for i := range path {
		unescaped, err := unescapePathSegment(path[i])
		if err != nil {
			return nil, false
		}
		path[i] = unescaped
	}

	return path, true
}

// NewPathForRef returns a new path for the given ref.
func NewPathForRef(ref ast.Ref) (path Path, err error) {
	if len(ref) == 0 {
		return nil, errors.New("empty reference (indicates error in caller)")
	}

	if len(ref) == 1 {
		return Path{}, nil
	}

	path = make(Path, 0, len(ref)-1)

	for _, term := range ref[1:] {
		switch v := term.Value.(type) {
		case ast.String:
			// Intern directly
			path = append(path, InternPathSegment(string(v)))
		case ast.Number:
			// Fast path for small integers
			if i64, ok := v.Int64(); ok && i64 >= 0 && i64 < 1000 {
				path = append(path, InternPathSegment(strconv.FormatInt(i64, 10)))
			} else {
				path = append(path, InternPathSegment(v.String()))
			}
		case ast.Boolean, ast.Null:
			return nil, &Error{
				Code:    NotFoundErr,
				Message: fmt.Sprintf("%v: does not exist", ref),
			}
		case *ast.Array, ast.Object, ast.Set:
			return nil, fmt.Errorf("composites cannot be base document keys: %v", ref)
		default:
			return nil, fmt.Errorf("unresolved reference (indicates error in caller): %v", ref)
		}
	}

	return path, nil
}

// Compare performs lexigraphical comparison on p and other and returns -1 if p
// is less than other, 0 if p is equal to other, or 1 if p is greater than
// other.
func (p Path) Compare(other Path) (cmp int) {
	return slices.Compare(p, other)
}

// Equal returns true if p is the same as other.
func (p Path) Equal(other Path) bool {
	if len(p) != len(other) {
		return false
	}

	// Fast path: pointer equality for interned strings
	for i := range p {
		// Try pointer comparison first (works for interned strings)
		if len(p[i]) > 0 && len(other[i]) > 0 {
			if unsafe.StringData(p[i]) != unsafe.StringData(other[i]) {
				// Fallback to value comparison
				if p[i] != other[i] {
					return false
				}
			}
		} else if p[i] != other[i] {
			return false
		}
	}

	return true
}

// HasPrefix returns true if p starts with other.
func (p Path) HasPrefix(other Path) bool {
	return len(other) <= len(p) && p[:len(other)].Equal(other)
}

// Ref returns a ref that represents p rooted at head.
func (p Path) Ref(head *ast.Term) (ref ast.Ref) {
	ref = make(ast.Ref, len(p)+1)
	ref[0] = head
	for i := range p {
		idx, err := strconv.ParseInt(p[i], 10, 64)
		if err == nil {
			ref[i+1] = ast.UIntNumberTerm(uint64(idx))
		} else {
			ref[i+1] = ast.StringTerm(p[i])
		}
	}
	return ref
}

const upperhex = "0123456789ABCDEF"

func shouldEscapePath(c byte) bool {
	if 'a' <= c && c <= 'z' || 'A' <= c && c <= 'Z' || '0' <= c && c <= '9' {
		return false
	}
	switch c {
	case '-', '_', '.', '~':
		return false
	}
	return true
}

func (p Path) String() string {
	if len(p) == 0 {
		return "/"
	}

	// Estimate size with escaping overhead (50% overhead for potential escapes)
	estSize := len(p) // '/' separators
	for i := range p {
		estSize += len(p[i]) + len(p[i])/2
	}

	sb := sbPool.Get()
	defer sbPool.Put(sb)
	sb.Grow(estSize)

	for i := range p {
		sb.WriteByte('/')

		// Inline escaping - avoid url.PathEscape allocation
		s := p[i]
		for j := 0; j < len(s); j++ {
			c := s[j]
			if shouldEscapePath(c) {
				sb.WriteByte('%')
				sb.WriteByte(upperhex[c>>4])
				sb.WriteByte(upperhex[c&15])
			} else {
				sb.WriteByte(c)
			}
		}
	}

	return sb.String()
}

// MustParsePath returns a new Path for s. If s cannot be parsed, this function
// will panic. This is mostly for test purposes.
func MustParsePath(s string) Path {
	path, ok := ParsePath(s)
	if !ok {
		panic(s)
	}
	return path
}
