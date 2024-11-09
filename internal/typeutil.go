package internal

import sppb "cloud.google.com/go/spanner/apiv1/spannerpb"

func CodeToSimpleType(code sppb.TypeCode) *sppb.Type {
	return &sppb.Type{Code: code}
}

func ElemCodeToArrayType(code sppb.TypeCode) *sppb.Type {
	return ElemTypeToArrayType(CodeToSimpleType(code))
}

func ElemTypeToArrayType(typ *sppb.Type) *sppb.Type {
	return &sppb.Type{Code: sppb.TypeCode_ARRAY, ArrayElementType: typ}
}

func StructTypeFieldsToStructType(fields []*sppb.StructType_Field) *sppb.Type {
	return &sppb.Type{Code: sppb.TypeCode_STRUCT, StructType: &sppb.StructType{Fields: fields}}
}

func FQNToProtoType(fqn string) *sppb.Type {
	return &sppb.Type{Code: sppb.TypeCode_PROTO, ProtoTypeFqn: fqn}
}

func FQNToEnumType(fqn string) *sppb.Type {
	return &sppb.Type{Code: sppb.TypeCode_ENUM, ProtoTypeFqn: fqn}
}
