// Package serial provides serialization utilities.
package serial

import (
	"encoding" // Use top-level encoding package
	"fmt"
	"reflect"
)

// TryMarshal attempts to marshal an object if it implements BinaryMarshaler.
// It handles both pointer and value receiver implementations.
func TryMarshal(v interface{}) ([]byte, error) {
	// Check if the value itself implements the interface
	if marshaler, ok := v.(encoding.BinaryMarshaler); ok {
		return marshaler.MarshalBinary()
	}

	// Check if a pointer to the value implements the interface
	// (only possible if v is addressable)
	pv := reflect.ValueOf(v)
	if pv.CanAddr() {
		if marshaler, ok := pv.Addr().Interface().(encoding.BinaryMarshaler); ok {
			// fmt.Printf("DEBUG: Marshalling via pointer for type %T\n", v)
			return marshaler.MarshalBinary()
		}
	}

	return nil, fmt.Errorf("type %T (or pointer) does not implement encoding.BinaryMarshaler", v)
}

// TryUnmarshal attempts to unmarshal data into a pointer if it implements BinaryUnmarshaler.
// v must be a non-nil pointer to the target object (e.g., &myStruct).
func TryUnmarshal(v interface{}, data []byte) error {
	// v must be a pointer to implement UnmarshalBinary correctly (to modify the value)
	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Ptr || rv.IsNil() {
		return fmt.Errorf("TryUnmarshal target must be a non-nil pointer, got %T", v)
	}

	if unmarshaler, ok := v.(encoding.BinaryUnmarshaler); ok {
		return unmarshaler.UnmarshalBinary(data)
	}

	return fmt.Errorf("type %T does not implement encoding.BinaryUnmarshaler", v)
}
