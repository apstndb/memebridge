package memebridge

import (
	"encoding/base64"
	"fmt"
	"github.com/apstndb/go-spannulls"
	"github.com/apstndb/spantype/ctorutil"
	"github.com/cloudspannerecosystem/memefish/char"
	"slices"
	"spheric.cloud/xiter"
	"strconv"

	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/cloudspannerecosystem/memefish/ast"
	"google.golang.org/protobuf/types/known/structpb"
)

const commitTimestampPlaceholderString = "spanner.commit_timestamp()"

var zeroGCV spanner.GenericColumnValue

func newStructGCV(fields []*sppb.StructType_Field, values []*structpb.Value) spanner.GenericColumnValue {
	return spanner.GenericColumnValue{
		Type:  ctorutil.StructTypeFieldsToStructType(fields),
		Value: structpb.NewListValue(&structpb.ListValue{Values: values}),
	}
}

func typelessStructLiteralArgToNameWithGCV(arg ast.TypelessStructLiteralArg) (string, spanner.GenericColumnValue, error) {
	switch a := arg.(type) {
	case *ast.ExprArg:
		gcv, err := MemefishExprToGCV(a.Expr)
		if err != nil {
			return "", zeroGCV, err
		}
		return "", gcv, nil
	case *ast.Alias:
		gcv, err := MemefishExprToGCV(a.Expr)
		if err != nil {
			return "", zeroGCV, err
		}
		return a.As.Alias.Name, gcv, nil
	default:
		return "", zeroGCV, fmt.Errorf("unknown struct literal arg: %v", a)
	}
}

func astStructLiteralsToGCV(expr ast.Expr) (spanner.GenericColumnValue, error) {
	var fields []*sppb.StructType_Field
	var values []*structpb.Value

	switch e := expr.(type) {
	case *ast.TypelessStructLiteral:
		for _, value := range e.Values {
			name, gcv, err := typelessStructLiteralArgToNameWithGCV(value)
			if err != nil {
				return zeroGCV, err
			}
			fields = append(fields, &sppb.StructType_Field{
				Name: name,
				Type: gcv.Type,
			})
			values = append(values, gcv.Value)
		}
	case *ast.TupleStructLiteral:
		for _, value := range e.Values {
			gcv, err := MemefishExprToGCV(value)
			if err != nil {
				return zeroGCV, err
			}
			fields = append(fields, &sppb.StructType_Field{Type: gcv.Type})
			values = append(values, gcv.Value)
		}

	case *ast.TypedStructLiteral:
		gcvs, err := xiter.TryCollect(xiter.MapErr(slices.Values(e.Values), MemefishExprToGCV))
		if err != nil {
			return zeroGCV, err
		}

		fields, err = xiter.TryCollect(xiter.MapErr(xiter.Zip(
			slices.Values(e.Fields),
			slices.Values(gcvs)), tupledWithErr(generateStructTypeField)))
		if err != nil {
			return zeroGCV, err
		}

		values = slices.Collect(xiter.Map(slices.Values(gcvs), gcvToValue))
	default:
		return zeroGCV, fmt.Errorf("expr is not struct literal: %v", e)
	}

	return newStructGCV(fields, values), nil
}

func MemefishExprToGCV(expr ast.Expr) (spanner.GenericColumnValue, error) {
	switch e := expr.(type) {
	case *ast.NullLiteral:
		// emulate behavior of query parameter with unknown type as INT64
		return spanner.GenericColumnValue{
			Type:  ctorutil.CodeToSimpleType(sppb.TypeCode_INT64),
			Value: structpb.NewNullValue(),
		}, nil
	case *ast.BoolLiteral:
		return spanner.GenericColumnValue{
			Type:  ctorutil.CodeToSimpleType(sppb.TypeCode_BOOL),
			Value: structpb.NewBoolValue(e.Value),
		}, nil
	case *ast.IntLiteral:
		i, err := strconv.ParseInt(e.Value, e.Base, 64)
		if err != nil {
			return zeroGCV, err
		}
		return spanner.GenericColumnValue{
			Type:  ctorutil.CodeToSimpleType(sppb.TypeCode_INT64),
			Value: structpb.NewStringValue(strconv.FormatInt(i, 10)),
		}, nil
	case *ast.FloatLiteral:
		f, err := strconv.ParseFloat(e.Value, 64)
		if err != nil {
			return zeroGCV, err
		}
		return spanner.GenericColumnValue{
			Type:  ctorutil.CodeToSimpleType(sppb.TypeCode_FLOAT64),
			Value: structpb.NewNumberValue(f),
		}, nil
	case *ast.StringLiteral:
		return spanner.GenericColumnValue{
			Type:  ctorutil.CodeToSimpleType(sppb.TypeCode_STRING),
			Value: structpb.NewStringValue(e.Value),
		}, nil
	case *ast.BytesLiteral:
		return spanner.GenericColumnValue{
			Type:  ctorutil.CodeToSimpleType(sppb.TypeCode_BYTES),
			Value: structpb.NewStringValue(base64.StdEncoding.EncodeToString(e.Value)),
		}, nil
	case *ast.DateLiteral:
		return spanner.GenericColumnValue{
			Type:  ctorutil.CodeToSimpleType(sppb.TypeCode_DATE),
			Value: structpb.NewStringValue(e.Value.Value),
		}, nil
	case *ast.TimestampLiteral:
		return spanner.GenericColumnValue{
			Type:  ctorutil.CodeToSimpleType(sppb.TypeCode_TIMESTAMP),
			Value: structpb.NewStringValue(e.Value.Value),
		}, nil
	case *ast.NumericLiteral:
		return spanner.GenericColumnValue{
			Type:  ctorutil.CodeToSimpleType(sppb.TypeCode_NUMERIC),
			Value: structpb.NewStringValue(e.Value.Value),
		}, nil
	case *ast.JSONLiteral:
		return spanner.GenericColumnValue{
			Type:  ctorutil.CodeToSimpleType(sppb.TypeCode_JSON),
			Value: structpb.NewStringValue(e.Value.Value),
		}, nil
	case *ast.ArrayLiteral:
		gcvs, err := xiter.TryCollect(
			xiter.MapErr(slices.Values(e.Values), MemefishExprToGCV))
		if err != nil {
			return zeroGCV, err
		}

		// ARRAY<Type> has more precedence than element type
		// TODO: May be more correct if it can detect common super type of gcvs[].Type
		var typ *sppb.Type
		if e.Type != nil {
			typ, err = MemefishTypeToSpannerpbType(e.Type)
			if err != nil {
				return zeroGCV, err
			}
		} else if len(gcvs) > 0 {
			typ = gcvs[0].Type
		}

		return spanner.GenericColumnValue{
			Type:  ctorutil.ElemTypeToArrayType(typ),
			Value: structpb.NewListValue(&structpb.ListValue{Values: slices.Collect(xiter.Map(slices.Values(gcvs), gcvToValue))}),
		}, nil
	case *ast.TypelessStructLiteral,
		*ast.TupleStructLiteral,
		*ast.TypedStructLiteral:
		return astStructLiteralsToGCV(e)
	case *ast.ParenExpr:
		return MemefishExprToGCV(e.Expr)
	case *ast.CallExpr:
		if char.EqualFold(e.Func.Name, "PENDING_COMMIT_TIMESTAMP") {
			return spanner.GenericColumnValue{
				Type:  ctorutil.CodeToSimpleType(sppb.TypeCode_TIMESTAMP),
				Value: structpb.NewStringValue(commitTimestampPlaceholderString),
			}, nil
		}
		// fallthrough
	default:
		// fallthrough
	}
	return zeroGCV, fmt.Errorf("not implemented: %s", expr.SQL())
}

func astTypeToGenericColumnValue(t ast.Type) (spanner.GenericColumnValue, error) {
	typ, err := MemefishTypeToSpannerpbType(t)
	if err != nil {
		return zeroGCV, err
	}
	return spannulls.NullGenericColumnValueFromType(typ), nil
}

func nameOrEmpty(f *ast.StructField) string {
	if f != nil && f.Ident != nil {
		return f.Ident.Name
	}
	return ""
}

func gcvToValue(gcv spanner.GenericColumnValue) *structpb.Value {
	return gcv.Value
}

func generateStructTypeField(field *ast.StructField, gcv spanner.GenericColumnValue) (*sppb.StructType_Field, error) {
	var typ *sppb.Type
	if field != nil && field.Type != nil {
		typeGcv, err := astTypeToGenericColumnValue(field.Type)
		if err != nil {
			return nil, err
		}
		typ = typeGcv.Type
	} else {
		typ = gcv.Type
	}
	return &sppb.StructType_Field{
		Name: nameOrEmpty(field),
		Type: typ,
	}, nil
}
