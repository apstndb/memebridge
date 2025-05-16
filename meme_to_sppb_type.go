package memebridge

import (
	"fmt"
	"strings"

	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/apstndb/spantype/typector"
	"github.com/cloudspannerecosystem/memefish/ast"
)

var ScalarTypeNameToTypeCodeMap = map[ast.ScalarTypeName]sppb.TypeCode{
	ast.BoolTypeName:      sppb.TypeCode_BOOL,
	ast.Int64TypeName:     sppb.TypeCode_INT64,
	ast.Float64TypeName:   sppb.TypeCode_FLOAT64,
	ast.Float32TypeName:   sppb.TypeCode_FLOAT32,
	ast.StringTypeName:    sppb.TypeCode_STRING,
	ast.BytesTypeName:     sppb.TypeCode_BYTES,
	ast.DateTypeName:      sppb.TypeCode_DATE,
	ast.TimestampTypeName: sppb.TypeCode_TIMESTAMP,
	ast.NumericTypeName:   sppb.TypeCode_NUMERIC,
	ast.JSONTypeName:      sppb.TypeCode_JSON,
	ast.IntervalTypeName:  sppb.TypeCode_INTERVAL,
}

func memefishScalarTypeToSpannerpbType(typename ast.ScalarTypeName) (*sppb.Type, error) {
	if code, ok := ScalarTypeNameToTypeCodeMap[typename]; ok {
		return typector.CodeToSimpleType(code), nil
	} else {
		return nil, fmt.Errorf("unknown type: %v", typename)
	}
}

func fieldNameOrEmpty(field *ast.StructField) string {
	if field != nil && field.Ident != nil {
		return field.Ident.Name
	}
	return ""
}

func memefishStructFieldToStructTypeField(field *ast.StructField) (*sppb.StructType_Field, error) {
	t, err := MemefishTypeToSpannerpbType(field.Type)
	if err != nil {
		return nil, err
	}

	return typector.NameTypeToStructTypeField(fieldNameOrEmpty(field), t), nil
}

func MemefishTypeToSpannerpbType(typ ast.Type) (*sppb.Type, error) {
	switch t := typ.(type) {
	case *ast.SimpleType:
		return memefishScalarTypeToSpannerpbType(t.Name)
	case *ast.ArrayType:
		if t.Item == nil {
			return nil, fmt.Errorf("invalid array type: %v", t)
		}

		typ, err := MemefishTypeToSpannerpbType(t.Item)
		if err != nil {
			return nil, err
		}

		return typector.ElemTypeToArrayType(typ), nil
	case *ast.StructType:
		var fields []*sppb.StructType_Field
		for _, field := range t.Fields {
			f, err := memefishStructFieldToStructTypeField(field)
			if err != nil {
				return nil, err
			}
			fields = append(fields, f)
		}

		return typector.StructTypeFieldsToStructType(fields), nil
	case *ast.NamedType:
		if len(t.Path) == 1 {
			switch strings.ToUpper(t.Path[0].Name) {
			case "UUID":
				return typector.CodeToSimpleType(sppb.TypeCode_UUID), nil
			}
		}
		return nil, fmt.Errorf("not known whether the named type is STRUCT or ENUM: %s", t.SQL())
	default:
		return nil, fmt.Errorf("not implemented: %s", t.SQL())
	}
}
