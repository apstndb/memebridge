package memebridge

import (
	"cloud.google.com/go/spanner"
	"google.golang.org/protobuf/types/known/structpb"
)

// isNullValue reports whether v represents SQL NULL on the wire.
// Both absent values (nil) and explicit protobuf null are treated as null.
func isNullValue(v *structpb.Value) bool {
	if v == nil {
		return true
	}
	_, ok := v.GetKind().(*structpb.Value_NullValue)
	return ok
}

// normalizeValue returns an explicit protobuf null for absent wire values.
func normalizeValue(v *structpb.Value) *structpb.Value {
	if isNullValue(v) {
		return structpb.NewNullValue()
	}
	return v
}

// gcvToProtoValue is an unexported prototype for a requested root spanvalue API
// (not gcvctor — result is *structpb.Value, not GenericColumnValue):
// spanvalue.ToProtoValue(gcv spanner.GenericColumnValue) *structpb.Value.
// It materializes protobuf NULL for SQL NULL cells before ARRAY/STRUCT assembly.
func gcvToProtoValue(gcv spanner.GenericColumnValue) *structpb.Value {
	if isNullGCV(gcv) {
		return structpb.NewNullValue()
	}
	return normalizeValue(gcv.Value)
}

// gcvArrayWireValues is an unexported prototype for a requested root spanvalue API
// (not gcvctor — result is []*structpb.Value):
// spanvalue.ArrayWireValues(elems []spanner.GenericColumnValue) []*structpb.Value.
func gcvArrayWireValues(elems []spanner.GenericColumnValue) []*structpb.Value {
	out := make([]*structpb.Value, len(elems))
	for i, gcv := range elems {
		out[i] = gcvToProtoValue(gcv)
	}
	return out
}
