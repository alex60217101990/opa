package ast

import (
	"bytes"
	"sync"

	"github.com/open-policy-agent/opa/v1/util"
)

var (
	TermPtrPool     = util.NewSyncPool[Term]()
	BytesReaderPool = util.NewSyncPool[bytes.Reader]()
	IndexResultPool = util.NewSyncPool[IndexResult]()

	// Needs custom pool because of custom Put logic.
	varVisitorPool = &vvPool{
		pool: sync.Pool{
			New: func() any {
				return NewVarVisitor()
			},
		},
	}

	// stringCachePool provides pooled maps for string caching.
	// Direct map usage eliminates struct overhead for maximum performance.
	stringCachePool = sync.Pool{
		New: func() any {
			m := make(map[string]Value, initialCacheSize)
			return &m
		},
	}
)

type vvPool struct {
	pool sync.Pool
}

func (p *vvPool) Get() *VarVisitor {
	return p.pool.Get().(*VarVisitor)
}

func (p *vvPool) Put(vv *VarVisitor) {
	if vv != nil {
		vv.Clear()
		p.pool.Put(vv)
	}
}

// getStringCache retrieves a map from the pool
func getStringCache() map[string]Value {
	return *stringCachePool.Get().(*map[string]Value)
}

// releaseStringCache clears and returns the map to the pool
func releaseStringCache(cache map[string]Value) {
	// If map grew too large, don't pool it (let GC handle)
	if len(cache) > maxPooledCacheSize {
		return
	}
	clear(cache)
	stringCachePool.Put(&cache)
}
