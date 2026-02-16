// Copyright 2026 The OPA Authors.  All rights reserved.
// Use of this source code is governed by an Apache2
// license that can be found in the LICENSE file.

package ast

// ParserMetadata contains information collected during parsing to optimize
// compilation. If nil or unpopulated, compiler falls back to full traversal.
// Fields ordered from largest to smallest to minimize padding.
type ParserMetadata struct {
	functionRefs        []Ref
	builtinRefs         map[string]struct{}
	printCallCount      int
	templateStringCount int
	flags               uint16
}

const (
	flagTemplateStrings uint16 = 1 << 0
	flagPrintCalls      uint16 = 1 << 1
	flagMetadataBlocks  uint16 = 1 << 2
	flagMetadataCalls   uint16 = 1 << 3
)

func NewParserMetadata() *ParserMetadata {
	return &ParserMetadata{
		builtinRefs: make(map[string]struct{}),
	}
}

func (m *ParserMetadata) HasTemplateStrings() bool {
	if m == nil {
		return false
	}
	return m.flags&flagTemplateStrings != 0
}

func (m *ParserMetadata) HasPrintCalls() bool {
	if m == nil {
		return false
	}
	return m.flags&flagPrintCalls != 0
}

func (m *ParserMetadata) HasMetadataBlocks() bool {
	if m == nil {
		return false
	}
	return m.flags&flagMetadataBlocks != 0
}

func (m *ParserMetadata) HasMetadataCalls() bool {
	if m == nil {
		return false
	}
	return m.flags&flagMetadataCalls != 0
}

func (m *ParserMetadata) MarkPrintCall() {
	m.flags |= flagPrintCalls
	m.printCallCount++
}

func (m *ParserMetadata) MarkTemplateString() {
	m.flags |= flagTemplateStrings
	m.templateStringCount++
}

func (m *ParserMetadata) MarkMetadataBlock() {
	m.flags |= flagMetadataBlocks
}

func (m *ParserMetadata) MarkMetadataCall() {
	m.flags |= flagMetadataCalls
}

func (m *ParserMetadata) AddFunctionRef(ref Ref) {
	m.functionRefs = append(m.functionRefs, ref)
	if len(ref) > 0 {
		name := ref.String()
		if BuiltinMap[name] != nil {
			m.builtinRefs[name] = struct{}{}
		}
	}
}

func (m *ParserMetadata) FunctionRefs() []Ref {
	if m == nil {
		return nil
	}
	return m.functionRefs
}

func (m *ParserMetadata) FunctionRefCount() int {
	if m == nil {
		return 0
	}
	return len(m.functionRefs)
}

func (m *ParserMetadata) HasBuiltin(name string) bool {
	if m == nil || m.builtinRefs == nil {
		return false
	}
	_, found := m.builtinRefs[name]
	return found
}

func (m *ParserMetadata) PrintCallCount() int {
	if m == nil {
		return 0
	}
	return m.printCallCount
}

func (m *ParserMetadata) TemplateStringCount() int {
	if m == nil {
		return 0
	}
	return m.templateStringCount
}
