package memebridge

import (
	"cloud.google.com/go/spanner"
	"github.com/apstndb/spanvalue"
	"google.golang.org/protobuf/types/known/structpb"
)

// gcvToProtoValue is an unexported prototype for a requested root spanvalue API
// (not gcvctor — result is *structpb.Value, not GenericColumnValue):
// spanvalue.ToProtoValue(gcv spanner.GenericColumnValue) *structpb.Value.
// It materializes protobuf NULL for SQL NULL cells before ARRAY/STRUCT assembly.
func gcvToProtoValue(gcv spanner.GenericColumnValue) *structpb.Value {
	if spanvalue.IsNull(gcv) {
		return structpb.NewNullValue()
	}
	return gcv.Value
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
