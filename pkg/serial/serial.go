// Package serial provides serialization utilities.
package serial

import (
	"encoding" // Use top-level encoding package
	"fmt"
)

// TryMarshal attempts to marshal an object if it implements BinaryMarshaler.
func TryMarshal(v interface{}) ([]byte, error) {
	if marshaler, ok := v.(encoding.BinaryMarshaler); ok {
		return marshaler.MarshalBinary()
	}
	return nil, fmt.Errorf("type %T does not implement encoding.BinaryMarshaler", v)
}

// TryUnmarshal attempts to unmarshal data into a pointer if it implements BinaryUnmarshaler.
// v must be a pointer to the target object.
func TryUnmarshal(v interface{}, data []byte) error {
	if unmarshaler, ok := v.(encoding.BinaryUnmarshaler); ok {
		return unmarshaler.UnmarshalBinary(data)
	}
	return fmt.Errorf("type %T does not implement encoding.BinaryUnmarshaler", v)
}
