// Copyright 2025 The OPA Authors.  All rights reserved.
// Use of this source code is governed by an Apache2
// license that can be found in the LICENSE file.

package util

import (
	"encoding/json"
	"strconv"
)

// These functions provide fast-path conversions from Go numeric types to json.Number.
// They are small enough to be inlined by the compiler, avoiding function call overhead.

func intToJSONNumber(v int) json.Number {
	return json.Number(strconv.Itoa(v))
}

func int8ToJSONNumber(v int8) json.Number {
	return json.Number(strconv.FormatInt(int64(v), 10))
}

func int16ToJSONNumber(v int16) json.Number {
	return json.Number(strconv.FormatInt(int64(v), 10))
}

func int32ToJSONNumber(v int32) json.Number {
	return json.Number(strconv.FormatInt(int64(v), 10))
}

func int64ToJSONNumber(v int64) json.Number {
	return json.Number(strconv.FormatInt(v, 10))
}

func uintToJSONNumber(v uint) json.Number {
	return json.Number(strconv.FormatUint(uint64(v), 10))
}

func uint8ToJSONNumber(v uint8) json.Number {
	return json.Number(strconv.FormatUint(uint64(v), 10))
}

func uint16ToJSONNumber(v uint16) json.Number {
	return json.Number(strconv.FormatUint(uint64(v), 10))
}

func uint32ToJSONNumber(v uint32) json.Number {
	return json.Number(strconv.FormatUint(uint64(v), 10))
}

func uint64ToJSONNumber(v uint64) json.Number {
	return json.Number(strconv.FormatUint(v, 10))
}

func float32ToJSONNumber(v float32) json.Number {
	return json.Number(strconv.FormatFloat(float64(v), 'f', -1, 32))
}

func float64ToJSONNumber(v float64) json.Number {
	return json.Number(strconv.FormatFloat(v, 'f', -1, 64))
}
