package memebridge_test

import (
	"fmt"
	"testing"

	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/apstndb/memebridge"
	"github.com/apstndb/spantype/typector"
	"github.com/cloudspannerecosystem/memefish/ast"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestMemefishExprToGCV(t *testing.T) {
	tests := []struct {
		input ast.Expr
		want  spanner.GenericColumnValue
	}{
		{&ast.BoolLiteral{Value: true},
			spanner.GenericColumnValue{
				Type:  typector.CodeToSimpleType(sppb.TypeCode_BOOL),
				Value: structpb.NewBoolValue(true),
			},
		},
		{&ast.IntLiteral{Value: "42", Base: 10},
			spanner.GenericColumnValue{
				Type:  typector.CodeToSimpleType(sppb.TypeCode_INT64),
				Value: structpb.NewStringValue("42"),
			},
		},
		{&ast.FloatLiteral{Value: "3.14"},
			spanner.GenericColumnValue{
				Type:  typector.CodeToSimpleType(sppb.TypeCode_FLOAT64),
				Value: structpb.NewNumberValue(3.14),
			},
		},
		/* TODO: Support FLOAT32
		{&ast.FloatLiteral{Value: "3.14"},
			spanner.GenericColumnValue{
				Type:  typector.CodeToSimpleType(sppb.TypeCode_FLOAT32),
				Value: structpb.NewNumberValue(3.14),
			},
		},
		*/
		{&ast.TimestampLiteral{Value: &ast.StringLiteral{Value: `2024-01-01T00:00:00Z`}},
			spanner.GenericColumnValue{
				Type:  typector.CodeToSimpleType(sppb.TypeCode_TIMESTAMP),
				Value: structpb.NewStringValue(`2024-01-01T00:00:00Z`),
			},
		},
		{&ast.DateLiteral{Value: &ast.StringLiteral{Value: `2024-01-01`}},
			spanner.GenericColumnValue{
				Type:  typector.CodeToSimpleType(sppb.TypeCode_DATE),
				Value: structpb.NewStringValue(`2024-01-01`),
			},
		},
		{&ast.StringLiteral{Value: `foo`},
			spanner.GenericColumnValue{
				Type:  typector.CodeToSimpleType(sppb.TypeCode_STRING),
				Value: structpb.NewStringValue(`foo`),
			},
		},
		{&ast.BytesLiteral{Value: []byte(`foo`)},
			spanner.GenericColumnValue{
				Type:  typector.CodeToSimpleType(sppb.TypeCode_BYTES),
				Value: structpb.NewStringValue(`Zm9v`),
			},
		},
		{&ast.NumericLiteral{Value: &ast.StringLiteral{Value: "1234567890.123456789"}},
			spanner.GenericColumnValue{
				Type:  typector.CodeToSimpleType(sppb.TypeCode_NUMERIC),
				Value: structpb.NewStringValue("1234567890.123456789"),
			},
		},
		{&ast.JSONLiteral{Value: &ast.StringLiteral{Value: `{"string_value": "foo"}`}},
			spanner.GenericColumnValue{
				Type:  typector.CodeToSimpleType(sppb.TypeCode_JSON),
				Value: structpb.NewStringValue(`{"string_value": "foo"}`),
			},
		},
		{&ast.ArrayLiteral{
			Type: &ast.SimpleType{Name: ast.Int64TypeName},
			Values: []ast.Expr{&ast.IntLiteral{
				Base:  10,
				Value: "1",
			}},
		},
			spanner.GenericColumnValue{
				Type:  typector.ElemCodeToArrayType(sppb.TypeCode_INT64),
				Value: structpb.NewListValue(&structpb.ListValue{Values: []*structpb.Value{structpb.NewStringValue("1")}}),
			},
		},
		{&ast.ArrayLiteral{
			Type: nil,
			Values: []ast.Expr{&ast.IntLiteral{
				Base:  10,
				Value: "1",
			}},
		},
			spanner.GenericColumnValue{
				Type:  typector.ElemCodeToArrayType(sppb.TypeCode_INT64),
				Value: structpb.NewListValue(&structpb.ListValue{Values: []*structpb.Value{structpb.NewStringValue("1")}}),
			},
		},
		// TODO: STRUCT, PROTO, ENUM
		{&ast.ParenExpr{Expr: &ast.IntLiteral{Value: "42", Base: 10}},
			spanner.GenericColumnValue{
				Type:  typector.CodeToSimpleType(sppb.TypeCode_INT64),
				Value: structpb.NewStringValue("42"),
			},
		},

		{&ast.CallExpr{Func: &ast.Ident{Name: "PENDING_COMMIT_TIMESTAMP"}},
			spanner.GenericColumnValue{
				Type:  typector.CodeToSimpleType(sppb.TypeCode_TIMESTAMP),
				Value: structpb.NewStringValue("spanner.commit_timestamp()"),
			},
		},
	}
	for _, test := range tests {
		t.Run(fmt.Sprintf("%T: %v", test.input, test.input.SQL()), func(t *testing.T) {
			got, err := memebridge.MemefishExprToGCV(test.input)
			if err != nil {
				t.Errorf("should not fail, but err: %v", err)
			}
			if diff := cmp.Diff(test.want, got, protocmp.Transform()); diff != "" {
				t.Errorf("mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
