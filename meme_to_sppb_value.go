package memebridge

import (
	"errors"
	"fmt"
	"math"
	"slices"
	"strconv"

	"github.com/apstndb/spantype/typector"
	"github.com/apstndb/spanvalue/gcvctor"
	"github.com/cloudspannerecosystem/memefish/char"
	"spheric.cloud/xiter"

	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/cloudspannerecosystem/memefish/ast"
	"google.golang.org/protobuf/types/known/structpb"
)

const commitTimestampPlaceholderString = "spanner.commit_timestamp()"

var zeroGCV spanner.GenericColumnValue

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
	var names []string
	var gcvs []spanner.GenericColumnValue
	switch e := expr.(type) {
	case *ast.TypelessStructLiteral:
		for _, value := range e.Values {
			name, gcv, err := typelessStructLiteralArgToNameWithGCV(value)
			if err != nil {
				return zeroGCV, err
			}
			// fields = append(fields, typector.NameTypeToStructTypeField(name, gcv.Type))
			// values = append(values, gcv.Value)
			names = append(names, name)
			gcvs = append(gcvs, gcv)
		}
	case *ast.TupleStructLiteral, *ast.TypedStructLiteral:
		astValues, err := extractValues(e)
		if err != nil {
			return zeroGCV, errors.New("invalid state")
		}

		gcvs, err = xiter.TryCollect(xiter.MapErr(slices.Values(astValues), MemefishExprToGCV))
		if err != nil {
			return zeroGCV, err
		}

		switch e := expr.(type) {
		case *ast.TupleStructLiteral:
			names = slices.Repeat([]string{""}, len(gcvs))
		case *ast.TypedStructLiteral:
			names = slices.Collect(xiter.Map(slices.Values(e.Fields), nameOrEmpty))
		}
	default:
		return zeroGCV, fmt.Errorf("expr is not struct literal: %v", e)
	}

	return gcvctor.StructValue(names, gcvs)
}

func extractValues(expr ast.Expr) ([]ast.Expr, error) {
	switch e := expr.(type) {
	case *ast.TupleStructLiteral:
		return e.Values, nil
	case *ast.TypedStructLiteral:
		return e.Values, nil
	default:
		return nil, fmt.Errorf("invalid argument, must be *ast.TupleStructLiteral or *ast.TypedStructLiteral, but %T", expr)
	}
}

func MemefishExprToGCV(expr ast.Expr) (spanner.GenericColumnValue, error) {
	switch e := expr.(type) {
	case *ast.NullLiteral:
		// emulate behavior of query parameter with unknown type as INT64
		return gcvctor.SimpleTypedNull(sppb.TypeCode_INT64), nil
	case *ast.BoolLiteral:
		return gcvctor.BoolValue(e.Value), nil
	case *ast.IntLiteral:
		i, err := strconv.ParseInt(e.Value, e.Base, 64)
		if err != nil {
			return zeroGCV, err
		}
		return gcvctor.Int64Value(i), nil
	case *ast.FloatLiteral:
		f, err := strconv.ParseFloat(e.Value, 64)
		if err != nil {
			return zeroGCV, err
		}
		return gcvctor.Float64Value(f), nil
	case *ast.StringLiteral:
		return gcvctor.StringValue(e.Value), nil
	case *ast.BytesLiteral:
		return gcvctor.BytesValue(e.Value), nil
	case *ast.DateLiteral:
		return gcvctor.StringBasedValue(sppb.TypeCode_DATE, e.Value.Value), nil
	case *ast.TimestampLiteral:
		return gcvctor.StringBasedValue(sppb.TypeCode_TIMESTAMP, e.Value.Value), nil
	case *ast.NumericLiteral:
		return gcvctor.StringBasedValue(sppb.TypeCode_NUMERIC, e.Value.Value), nil
	case *ast.JSONLiteral:
		return gcvctor.StringBasedValue(sppb.TypeCode_JSON, e.Value.Value), nil
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
			Type:  typector.ElemTypeToArrayType(typ),
			Value: structpb.NewListValue(&structpb.ListValue{Values: slices.Collect(xiter.Map(slices.Values(gcvs), gcvToValue))}),
		}, nil
	case *ast.TypelessStructLiteral,
		*ast.TupleStructLiteral,
		*ast.TypedStructLiteral:
		return astStructLiteralsToGCV(e)
	case *ast.ParenExpr:
		return MemefishExprToGCV(e.Expr)
	case *ast.CastExpr:
		return memefishCastExprToGCV(e)
	case *ast.CallExpr:
		if len(e.Func.Idents) == 1 && char.EqualFold(e.Func.Idents[0].Name, "PENDING_COMMIT_TIMESTAMP") {
			return gcvctor.StringBasedValue(sppb.TypeCode_TIMESTAMP, commitTimestampPlaceholderString), nil
		}
		// break
	default:
		// break
	}
	return zeroGCV, fmt.Errorf("not implemented: %s", expr.SQL())
}

// See https://github.com/google/zetasql/blob/master/zetasql/public/cast.cc
func memefishCastExprToGCV(cast *ast.CastExpr) (spanner.GenericColumnValue, error) {
	destType, err := MemefishTypeToSpannerpbType(cast.Type)
	if err != nil {
		return zeroGCV, err
	}

	if _, ok := cast.Expr.(*ast.NullLiteral); ok {
		return gcvctor.SimpleTypedNull(destType.GetCode()), nil
	}

	// Prioritize types without literals
	switch destType.GetCode() {
	case sppb.TypeCode_FLOAT64:
		switch e := cast.Expr.(type) {
		case *ast.StringLiteral:
			switch {
			case char.EqualFold(e.Value, "NaN"), char.EqualFold(e.Value, "-NaN"):
				return gcvctor.Float64Value(math.NaN()), nil
			case char.EqualFold(e.Value, "Infinity"):
				return gcvctor.Float64Value(math.Inf(1)), nil
			case char.EqualFold(e.Value, "-Infinity"):
				return gcvctor.Float64Value(math.Inf(-1)), nil
			}
		}
		return zeroGCV, fmt.Errorf("unsupported expr for %v: %v", destType.GetCode(), cast.Expr.SQL())
	case sppb.TypeCode_FLOAT32:
		switch e := cast.Expr.(type) {
		case *ast.FloatLiteral:
			f, err := strconv.ParseFloat(e.Value, 32)
			if err != nil {
				return zeroGCV, err
			}
			return gcvctor.Float32Value(float32(f)), nil
		case *ast.StringLiteral:
			switch {
			case char.EqualFold(e.Value, "NaN"), char.EqualFold(e.Value, "-NaN"):
				return gcvctor.Float32Value(float32(math.NaN())), nil
			case char.EqualFold(e.Value, "Infinity"):
				return gcvctor.Float32Value(float32(math.Inf(1))), nil
			case char.EqualFold(e.Value, "-Infinity"):
				return gcvctor.Float32Value(float32(math.Inf(-1))), nil
			}
		}
		return zeroGCV, fmt.Errorf("unsupported expr for %v: %v", destType.GetCode(), cast.Expr.SQL())
	case sppb.TypeCode_UUID, sppb.TypeCode_INTERVAL:
		switch e := cast.Expr.(type) {
		case *ast.StringLiteral:
			return gcvctor.StringBasedValue(destType.GetCode(), e.Value), nil
		}
	default:
		return zeroGCV, fmt.Errorf("unsupported type: %v", destType.GetCode())
	}
	return zeroGCV, fmt.Errorf("unsupported expr for %v: %v", destType.GetCode(), cast.Expr.SQL())
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
