// Copyright 2026 The OPA Authors.  All rights reserved.
// Use of this source code is governed by an Apache2
// license that can be found in the LICENSE file.

// Package arena implements a memory-optimized storage backend for OPA using
// arena allocation and string interning.
//
// The arena storage backend provides:
//   - Lock-free reads with multi-reader support
//   - Single-writer transactions with rollback
//   - Zero-copy path lookups using unique.Handle string interning
//   - Automatic memory reuse via freelist and background scavenging
//   - Compact 32-byte nodes for cache-friendly access
//
// This implementation is optimized for:
//   - High read throughput with minimal allocations
//   - Large JSON documents with repeated keys/values
//   - Frequent updates to the same data paths
//
// Trade-offs:
//   - Higher initial memory overhead (pre-allocated segments)
//   - Not suitable for very small datasets (<1KB)
//   - Background scavenging adds CPU overhead
package arena

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/open-policy-agent/opa/v1/storage"
)

// Arena implements an arena-based storage backend.
type Arena struct {
	// segments is a fixed-size array of node segments.
	// Each segment contains SegmentSize nodes.
	segments [MaxSegments]*[SegmentSize]Node

	// segCount tracks the number of allocated segments.
	segCount int32

	// nodeCnt tracks the total number of allocated nodes across all segments.
	nodeCnt int32

	// freeHead points to the head of the freelist (singly-linked list of free nodes).
	// -1 indicates an empty freelist.
	freeHead int32

	// mu protects segment allocation.
	mu sync.Mutex

	// rmu is the reader lock (for read transactions).
	rmu sync.RWMutex

	// wmu is the writer lock (ensures single writer).
	wmu sync.Mutex

	// xid is the transaction ID counter.
	xid uint64

	// rootIdx is the index of the root object node.
	// -1 indicates uninitialized (for lazy initialization)
	rootIdx int32

	// policies stores policy modules using simple map (not in arena).
	// This avoids arena overhead for policy-only workloads.
	policies    map[string][]byte
	policiesMu  sync.RWMutex // Protects policies map

	// triggers stores registered trigger handlers.
	// NOTE: Kept as map because TriggerConfig contains callback functions
	// which cannot be stored in arena.
	triggers map[*handle]storage.TriggerConfig

	// scavengerStop signals the scavenger goroutine to stop.
	scavengerStop chan struct{}

	// tombstoneCount tracks the number of tombstoned nodes (for scavenger trigger).
	tombstoneCount int32

	// commitCount tracks the number of commits (for periodic scavenger).
	commitCount int32
}

type handle struct {
	arena *Arena
}

// New creates a new arena-based storage backend.
func New() storage.Store {
	return NewWithOpts()
}

// Opt is a configuration option for the arena.
type Opt func(*Arena)

// WithScavenger enables background scavenging with the given interval.
func WithScavenger(interval time.Duration) Opt {
	return func(a *Arena) {
		if interval > 0 {
			a.StartScavenger(interval)
		}
	}
}

// NewWithOpts creates a new arena with custom options.
func NewWithOpts(opts ...Opt) storage.Store {
	a := &Arena{
		freeHead:      -1,
		rootIdx:       -1, // Lazy initialization - allocated on first data operation
		policies:      make(map[string][]byte),
		triggers:      make(map[*handle]storage.TriggerConfig),
		scavengerStop: make(chan struct{}),
	}

	// DON'T allocate segment or root yet - fully lazy initialization
	// For policy-only workloads, this avoids 16 KB arena overhead completely
	// Root will be initialized on first data operation (Read/Write)

	for _, opt := range opts {
		opt(a)
	}

	return a
}

// ensureRoot ensures the root node is initialized (lazy initialization).
func (a *Arena) ensureRoot() {
	if a.rootIdx == -1 {
		// First data operation - initialize root
		a.rootIdx = a.alloc()
		root := a.getNode(a.rootIdx)
		root.SetObject(-1) // Empty object
	}
}

// extend allocates a new segment.
// Must be called with a.mu held or during initialization.
func (a *Arena) extend() {
	newIdx := atomic.LoadInt32(&a.segCount)
	if newIdx >= MaxSegments {
		panic("arena: maximum segments exceeded")
	}

	seg := new([SegmentSize]Node)
	// Initialize all nodes in the segment
	for i := range seg {
		seg[i].Reset()
	}

	a.segments[newIdx] = seg
	atomic.AddInt32(&a.segCount, 1)
}

// getNode returns a pointer to the node at the given index.
func (a *Arena) getNode(idx int32) *Node {
	if idx < 0 {
		return nil
	}
	segIdx := idx / SegmentSize
	nodeIdx := idx % SegmentSize
	return &a.segments[segIdx][nodeIdx]
}

// alloc allocates a new node from the freelist or extends the arena.
func (a *Arena) alloc() int32 {
	// Try to allocate from freelist first
	for {
		oldFree := atomic.LoadInt32(&a.freeHead)
		if oldFree == -1 {
			break
		}

		node := a.getNode(oldFree)
		nextFree := node.next

		if atomic.CompareAndSwapInt32(&a.freeHead, oldFree, nextFree) {
			node.Reset()
			return oldFree
		}
	}

	// Freelist is empty, allocate a new node
	idx := atomic.AddInt32(&a.nodeCnt, 1) - 1

	// Check if we need to extend
	segIdx := idx / SegmentSize
	if segIdx >= atomic.LoadInt32(&a.segCount) {
		a.mu.Lock()
		// Double-check after acquiring lock
		if segIdx >= atomic.LoadInt32(&a.segCount) {
			a.extend()
		}
		a.mu.Unlock()
	}

	node := a.getNode(idx)
	node.Reset()
	return idx
}

// free adds a node back to the freelist.
func (a *Arena) free(idx int32) {
	if idx < 0 {
		return
	}

	node := a.getNode(idx)
	node.Reset()

	for {
		oldHead := atomic.LoadInt32(&a.freeHead)
		node.next = oldHead
		if atomic.CompareAndSwapInt32(&a.freeHead, oldHead, idx) {
			return
		}
	}
}

// NewTransaction implements storage.Store.
func (a *Arena) NewTransaction(_ context.Context, params ...storage.TransactionParams) (storage.Transaction, error) {
	var write bool
	var ctx *storage.Context

	if len(params) > 0 {
		write = params[0].Write
		ctx = params[0].Context
	}

	// Acquire locks before creating transaction to ensure consistent snapshot
	if write {
		a.wmu.Lock()
	}

	// Always lock briefly to get consistent snapshot, then release for reads
	a.rmu.RLock()
	rootIdx := a.rootIdx
	a.rmu.RUnlock()

	txn := &transaction{
		arena:   a,
		xid:     atomic.AddUint64(&a.xid, 1),
		rootIdx: rootIdx,
		write:   write,
		context: ctx,
	}

	return txn, nil
}

// Read implements storage.Store.
func (a *Arena) Read(ctx context.Context, txn storage.Transaction, path storage.Path) (any, error) {
	underlying, err := a.underlying(txn)
	if err != nil {
		return nil, err
	}

	return underlying.Read(path)
}

// Write implements storage.Store.
func (a *Arena) Write(ctx context.Context, txn storage.Transaction, op storage.PatchOp, path storage.Path, value any) error {
	underlying, err := a.underlying(txn)
	if err != nil {
		return err
	}

	// Lazy initialization: ensure root exists for data operations
	a.ensureRoot()

	return underlying.Write(op, path, value)
}

// Commit implements storage.Store.
func (a *Arena) Commit(ctx context.Context, txn storage.Transaction) error {
	underlying, err := a.underlying(txn)
	if err != nil {
		return err
	}

	if underlying.write {
		a.rmu.Lock()
		event := underlying.Commit()
		a.rmu.Unlock()
		a.runOnCommitTriggers(ctx, txn, event)
		underlying.stale = true
		a.wmu.Unlock()

		// Periodic scavenger: run every 10 commits
		// This keeps memory usage stable for write-heavy workloads
		count := atomic.AddInt32(&a.commitCount, 1)
		if count%10 == 0 {
			a.scavenge()
		}
	}

	return nil
}

// Abort implements storage.Store.
func (a *Arena) Abort(_ context.Context, txn storage.Transaction) {
	underlying, err := a.underlying(txn)
	if err != nil {
		panic(err)
	}

	underlying.stale = true

	if underlying.write {
		a.wmu.Unlock()
	}
}

// Truncate implements storage.Store.
func (a *Arena) Truncate(ctx context.Context, txn storage.Transaction, params storage.TransactionParams, it storage.Iterator) error {
	// For now, use the default implementation from storage package
	// TODO: Optimize this for arena storage
	return &storage.Error{
		Code:    storage.InternalErr,
		Message: "truncate not yet optimized for arena storage",
	}
}

// ListPolicies implements storage.Store.
func (a *Arena) ListPolicies(_ context.Context, txn storage.Transaction) ([]string, error) {
	underlying, err := a.underlying(txn)
	if err != nil {
		return nil, err
	}
	return underlying.ListPolicies(), nil
}

// GetPolicy implements storage.Store.
func (a *Arena) GetPolicy(_ context.Context, txn storage.Transaction, id string) ([]byte, error) {
	underlying, err := a.underlying(txn)
	if err != nil {
		return nil, err
	}
	return underlying.GetPolicy(id)
}

// UpsertPolicy implements storage.Store.
func (a *Arena) UpsertPolicy(_ context.Context, txn storage.Transaction, id string, bs []byte) error {
	underlying, err := a.underlying(txn)
	if err != nil {
		return err
	}
	return underlying.UpsertPolicy(id, bs)
}

// DeletePolicy implements storage.Store.
func (a *Arena) DeletePolicy(_ context.Context, txn storage.Transaction, id string) error {
	underlying, err := a.underlying(txn)
	if err != nil {
		return err
	}
	return underlying.DeletePolicy(id)
}

// Register implements storage.Store.
func (a *Arena) Register(_ context.Context, txn storage.Transaction, config storage.TriggerConfig) (storage.TriggerHandle, error) {
	underlying, err := a.underlying(txn)
	if err != nil {
		return nil, err
	}

	if !underlying.write {
		return nil, &storage.Error{
			Code:    storage.InvalidTransactionErr,
			Message: "triggers must be registered with a write transaction",
		}
	}

	h := &handle{arena: a}
	a.triggers[h] = config
	return h, nil
}

// Unregister implements storage.TriggerHandle.
func (h *handle) Unregister(_ context.Context, txn storage.Transaction) {
	underlying, err := h.arena.underlying(txn)
	if err != nil {
		panic(err)
	}

	if !underlying.write {
		panic(&storage.Error{
			Code:    storage.InvalidTransactionErr,
			Message: "triggers must be unregistered with a write transaction",
		})
	}

	delete(h.arena.triggers, h)
}

// underlying extracts the arena transaction from a storage.Transaction.
func (a *Arena) underlying(txn storage.Transaction) (*transaction, error) {
	underlying, ok := txn.(*transaction)
	if !ok {
		return nil, &storage.Error{
			Code:    storage.InvalidTransactionErr,
			Message: "unexpected transaction type",
		}
	}

	if underlying.arena != a {
		return nil, &storage.Error{
			Code:    storage.InvalidTransactionErr,
			Message: "transaction belongs to different arena",
		}
	}

	if underlying.stale {
		return nil, &storage.Error{
			Code:    storage.InvalidTransactionErr,
			Message: "stale transaction",
		}
	}

	return underlying, nil
}

// runOnCommitTriggers invokes all registered triggers.
func (a *Arena) runOnCommitTriggers(ctx context.Context, txn storage.Transaction, event storage.TriggerEvent) {
	for _, trigger := range a.triggers {
		trigger.OnCommit(ctx, txn, event)
	}
}

// StartScavenger starts a background goroutine that reclaims tombstoned nodes.
func (a *Arena) StartScavenger(interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				a.scavenge()
			case <-a.scavengerStop:
				return
			}
		}
	}()
}

// StopScavenger stops the background scavenger.
func (a *Arena) StopScavenger() {
	close(a.scavengerStop)
}

// scavenge reclaims tombstoned nodes.
func (a *Arena) scavenge() {
	limit := atomic.LoadInt32(&a.nodeCnt)

	for i := int32(0); i < limit; i++ {
		node := a.getNode(i)

		// Use atomic load to avoid race with concurrent writers
		vType := ValueType(atomic.LoadUint32((*uint32)(&node.vType)))

		if vType == TypeTombstone {
			a.free(i)
		}

		// Yield periodically to avoid hogging CPU
		if i%1000 == 0 {
			runtime.Gosched()
		}
	}
}

// LoadMap loads a map[string]any into the arena and returns the root node index.
func (a *Arena) LoadMap(m map[string]any) int32 {
	if len(m) == 0 {
		idx := a.alloc()
		node := a.getNode(idx)
		node.SetObject(-1)
		return idx
	}

	head := int32(-1)
	var last int32 = -1

	for k, v := range m {
		nodeIdx := a.alloc()
		node := a.getNode(nodeIdx)
		node.SetKey(k)
		node.next = -1
		a.fillNode(node, v)

		if head == -1 {
			head = nodeIdx
		}
		if last != -1 {
			a.getNode(last).next = nodeIdx
		}
		last = nodeIdx
	}

	objIdx := a.alloc()
	objNode := a.getNode(objIdx)
	objNode.SetObject(head)

	return objIdx
}

// LoadSlice loads a []any into the arena and returns the root node index.
func (a *Arena) LoadSlice(s []any) int32 {
	if len(s) == 0 {
		idx := a.alloc()
		node := a.getNode(idx)
		node.SetArray(-1)
		return idx
	}

	head := int32(-1)
	var last int32 = -1

	for _, v := range s {
		nodeIdx := a.alloc()
		node := a.getNode(nodeIdx)
		node.next = -1
		a.fillNode(node, v)

		if head == -1 {
			head = nodeIdx
		}
		if last != -1 {
			a.getNode(last).next = nodeIdx
		}
		last = nodeIdx
	}

	arrIdx := a.alloc()
	arrNode := a.getNode(arrIdx)
	arrNode.SetArray(head)

	return arrIdx
}

// fillNode fills a node with a value.
func (a *Arena) fillNode(n *Node, val any) {
	switch v := val.(type) {
	case nil:
		n.SetNull()
	case bool:
		n.SetBool(v)
	case int:
		n.SetInt(v)
	case int64:
		n.SetInt(int(v))
	case float64:
		n.SetFloat(v)
	case string:
		n.SetString(v)
	case map[string]any:
		childIdx := a.LoadMap(v)
		n.SetObject(a.getNode(childIdx).AsChildIndex())
	case []any:
		childIdx := a.LoadSlice(v)
		n.SetArray(a.getNode(childIdx).AsChildIndex())
	default:
		n.SetNull()
	}
}

// SetValue sets a value at a path in the arena.
func (a *Arena) SetValue(rootIdx int32, path []string, value any) (int32, error) {
	if len(path) == 0 {
		// Setting root
		return a.LoadMap(map[string]any{}), nil
	}

	root := a.getNode(rootIdx)
	if root.vType != TypeObject {
		return -1, &storage.Error{
			Code:    storage.NotFoundErr,
			Message: "root must be an object",
		}
	}

	return a.setValue(rootIdx, path, 0, value)
}

// setValue is a recursive helper for SetValue.
func (a *Arena) setValue(parentIdx int32, path []string, depth int, value any) (int32, error) {
	parent := a.getNode(parentIdx)
	key := path[depth]
	keyHandle := InternString(key)

	// Find or create the child
	childIdx := parent.AsChildIndex()
	var prevIdx int32 = -1

	for childIdx != -1 {
		child := a.getNode(childIdx)
		if child.key == keyHandle {
			// Found the key
			if depth == len(path)-1 {
				// This is the target, replace it
				// First, free old children if this was an object/array
				a.freeNodeChildren(childIdx)
				// Then fill with new value
				a.fillNode(child, value)
				return parentIdx, nil
			}
			// Recurse deeper
			return a.setValue(childIdx, path, depth+1, value)
		}
		prevIdx = childIdx
		childIdx = child.next
	}

	// Key not found, create a new node
	newIdx := a.alloc()
	newNode := a.getNode(newIdx)
	newNode.SetKey(key)
	newNode.next = -1

	if depth == len(path)-1 {
		// This is the target
		a.fillNode(newNode, value)
	} else {
		// Create intermediate object
		newNode.SetObject(-1)
		a.setValue(newIdx, path, depth+1, value)
	}

	// Link the new node
	if prevIdx == -1 {
		// First child
		parent.SetObject(newIdx)
	} else {
		a.getNode(prevIdx).next = newIdx
	}

	return parentIdx, nil
}

// removeValue removes a value at the given path with eager cleanup.
func (a *Arena) removeValue(parentIdx int32, path []string, depth int) error {
	parent := a.getNode(parentIdx)
	key := path[depth]
	keyHandle := InternString(key)

	// Find the child to remove
	childIdx := parent.AsChildIndex()
	var prevIdx int32 = -1

	for childIdx != -1 {
		child := a.getNode(childIdx)
		if child.key == keyHandle {
			// Found the key
			if depth == len(path)-1 {
				// This is the target - eager cleanup!
				// First, unlink from parent's child chain
				a.unlinkChild(parentIdx, childIdx, prevIdx)

				// Then recursively free this node and all descendants
				a.freeNodeRecursive(childIdx)

				return nil
			}
			// Recurse deeper
			return a.removeValue(childIdx, path, depth+1)
		}
		prevIdx = childIdx
		childIdx = child.next
	}

	// Key not found
	return &storage.Error{
		Code:    storage.NotFoundErr,
		Message: "document does not exist",
	}
}

// freeNodeRecursive recursively frees a node and all its descendants.
func (a *Arena) freeNodeRecursive(idx int32) {
	if idx == -1 {
		return
	}

	node := a.getNode(idx)

	// Free all children first (depth-first)
	switch node.vType {
	case TypeObject, TypeArray:
		childIdx := node.AsChildIndex()
		for childIdx != -1 {
			nextIdx := a.getNode(childIdx).next
			a.freeNodeRecursive(childIdx)
			childIdx = nextIdx
		}
	}

	// Free this node
	a.free(idx)
}

// freeNodeChildren frees all children of a node without freeing the node itself.
// Used when replacing node value (e.g., object → string).
func (a *Arena) freeNodeChildren(idx int32) {
	if idx == -1 {
		return
	}

	node := a.getNode(idx)

	// Free all children if this is an object/array
	switch node.vType {
	case TypeObject, TypeArray:
		childIdx := node.AsChildIndex()
		for childIdx != -1 {
			nextIdx := a.getNode(childIdx).next
			a.freeNodeRecursive(childIdx) // Recursively free each child
			childIdx = nextIdx
		}
	}
	// Note: Don't free the node itself, just its children
}

// unlinkChild removes a child node from parent's linked list.
func (a *Arena) unlinkChild(parentIdx, childIdx, prevIdx int32) {
	parent := a.getNode(parentIdx)
	child := a.getNode(childIdx)

	if prevIdx == -1 {
		// First child - update parent's head pointer
		// This works correctly even when child.next = -1
		parent.vRaw = uint64(child.next)
	} else {
		// Not first child - update previous sibling's next pointer
		prev := a.getNode(prevIdx)
		prev.next = child.next
	}
}
