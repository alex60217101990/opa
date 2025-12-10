// Copyright 2024 The OPA Authors.  All rights reserved.
// Use of this source code is governed by an Apache2
// license that can be found in the LICENSE file.

package storage

import (
	"strings"
	"sync"
)

var sbPool = &stringBuilderPool{
	pool: sync.Pool{
		New: func() any {
			return &strings.Builder{}
		},
	},
}

type stringBuilderPool struct{ pool sync.Pool }

func (p *stringBuilderPool) Get() *strings.Builder {
	return p.pool.Get().(*strings.Builder)
}

func (p *stringBuilderPool) Put(sb *strings.Builder) {
	sb.Reset()
	p.pool.Put(sb)
}
