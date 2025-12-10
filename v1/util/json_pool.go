// Copyright 2025 The OPA Authors.  All rights reserved.
// Use of this source code is governed by an Apache2
// license that can be found in the LICENSE file.

package util

import (
	"bytes"
	"encoding/json"
	"sync"
)

// bufferPool provides a pool of reusable byte buffers for JSON operations.
// This reduces allocations during frequent marshal/unmarshal operations.
var bufferPool = sync.Pool{
	New: func() any {
		// Pre-allocate 1KB buffer for typical JSON objects
		return bytes.NewBuffer(make([]byte, 0, 1024))
	},
}

// getBuffer retrieves a buffer from the pool.
func getBuffer() *bytes.Buffer {
	return bufferPool.Get().(*bytes.Buffer)
}

// putBuffer returns a buffer to the pool after resetting it.
func putBuffer(buf *bytes.Buffer) {
	buf.Reset()
	bufferPool.Put(buf)
}

// RoundTripWithPool is an optimized version of RoundTrip that uses pooled buffers
// for JSON operations. This reduces memory allocations for repeated JSON operations.
//
// This function should be used in hot paths where JSON round-tripping is frequent.
func RoundTripWithPool(x *any) error {
	// Fast path: skip round-trip for types that won't change
	if x == nil || !NeedsRoundTrip(*x) {
		return nil
	}

	// Fast path: direct conversion for numeric types to json.Number
	a := *x
	switch v := a.(type) {
	case int:
		*x = intToJSONNumber(v)
		return nil
	case int8:
		*x = int8ToJSONNumber(v)
		return nil
	case int16:
		*x = int16ToJSONNumber(v)
		return nil
	case int32:
		*x = int32ToJSONNumber(v)
		return nil
	case int64:
		*x = int64ToJSONNumber(v)
		return nil
	case uint:
		*x = uintToJSONNumber(v)
		return nil
	case uint8:
		*x = uint8ToJSONNumber(v)
		return nil
	case uint16:
		*x = uint16ToJSONNumber(v)
		return nil
	case uint32:
		*x = uint32ToJSONNumber(v)
		return nil
	case uint64:
		*x = uint64ToJSONNumber(v)
		return nil
	case float32:
		*x = float32ToJSONNumber(v)
		return nil
	case float64:
		*x = float64ToJSONNumber(v)
		return nil
	}

	// Use pooled buffer for JSON marshal/unmarshal
	buf := getBuffer()
	defer putBuffer(buf)

	encoder := json.NewEncoder(buf)
	if err := encoder.Encode(x); err != nil {
		return err
	}

	// Remove trailing newline added by Encoder.Encode
	bs := buf.Bytes()
	if len(bs) > 0 && bs[len(bs)-1] == '\n' {
		bs = bs[:len(bs)-1]
	}

	return UnmarshalJSON(bs, x)
}

// UnmarshalJSONWithPool is an optimized version of UnmarshalJSON that uses
// a pooled buffer. Use this when you already have []byte data and want to
// unmarshal with json.Number support.
func UnmarshalJSONWithPool(bs []byte, x any) error {
	// For small byte slices, creating a decoder directly from bytes.NewBuffer
	// with pooling might add overhead. Use standard unmarshal for small data.
	if len(bs) < 256 {
		return UnmarshalJSON(bs, x)
	}

	buf := getBuffer()
	defer putBuffer(buf)

	buf.Write(bs)
	decoder := NewJSONDecoder(buf)
	return decoder.Decode(x)
}
