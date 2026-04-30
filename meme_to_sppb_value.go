package memebridge

import (
	"errors"
	"fmt"
	"math/big"
	"slices"
	"strconv"

	"github.com/apstndb/spantype/typector"
	"github.com/apstndb/spanvalue/gcvctor"
	"github.com/cloudspannerecosystem/memefish/char"
	"github.com/samber/lo"

	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/cloudspannerecosystem/memefish/ast"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

const commitTimestampPlaceholderString = "spanner.commit_timestamp()"

var (
	ErrCannotInferArrayElementType = errors.New("cannot infer element type for array literal without explicit type")
	zeroGCV                        spanner.GenericColumnValue
)

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

		gcvs, err = lo.MapErr(astValues, func(expr ast.Expr, _ int) (spanner.GenericColumnValue, error) {
			return MemefishExprToGCV(expr)
		})
		if err != nil {
			return zeroGCV, err
		}

		switch e := expr.(type) {
		case *ast.TupleStructLiteral:
			names = slices.Repeat([]string{""}, len(gcvs))
		case *ast.TypedStructLiteral:
			names = lo.Map(e.Fields, func(f *ast.StructField, _ int) string {
				return nameOrEmpty(f)
			})
		}
	default:
		return zeroGCV, fmt.Errorf("expr is not struct literal: %v", e)
	}

	return gcvctor.StructValueOf(names, gcvs)
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
		return gcvctor.NullOf(typector.Int64()), nil
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
		return gcvctor.StringBasedValueFromCode(sppb.TypeCode_DATE, e.Value.Value), nil
	case *ast.TimestampLiteral:
		return gcvctor.StringBasedValueFromCode(sppb.TypeCode_TIMESTAMP, e.Value.Value), nil
	case *ast.NumericLiteral:
		return gcvctor.StringBasedValueFromCode(sppb.TypeCode_NUMERIC, e.Value.Value), nil
	case *ast.JSONLiteral:
		return gcvctor.StringBasedValueFromCode(sppb.TypeCode_JSON, e.Value.Value), nil
	case *ast.ArrayLiteral:
		gcvs, err := lo.MapErr(e.Values, func(expr ast.Expr, _ int) (spanner.GenericColumnValue, error) {
			return MemefishExprToGCV(expr)
		})
		if err != nil {
			return zeroGCV, err
		}

		// An explicit ARRAY<T> annotation takes precedence; otherwise infer a
		// local common supertype from the literal elements.
		var typ *sppb.Type
		if e.Type != nil {
			// memefish stores the explicit type from ARRAY<T>[...] as the element type T,
			// so MemefishTypeToSpannerpbType already returns the correct elemType here.
			typ, err = MemefishTypeToSpannerpbType(e.Type)
			if err != nil {
				return zeroGCV, err
			}
		} else {
			typ = inferArrayElementType(e.Values, gcvs)
		}
		if typ == nil {
			return zeroGCV, ErrCannotInferArrayElementType
		}

		return arrayLiteralValueOf(typ, gcvs)
	case *ast.TypelessStructLiteral,
		*ast.TupleStructLiteral,
		*ast.TypedStructLiteral:
		return astStructLiteralsToGCV(e)
	case *ast.IntervalLiteralSingle, *ast.IntervalLiteralRange:
		return astIntervalLiteralsToGCV(e)
	case *ast.ParenExpr:
		return MemefishExprToGCV(e.Expr)
	case *ast.CastExpr:
		return memefishCastExprToGCV(e)
	case *ast.CallExpr:
		if len(e.Func.Idents) == 1 && char.EqualFold(e.Func.Idents[0].Name, "PENDING_COMMIT_TIMESTAMP") {
			return gcvctor.StringBasedValueFromCode(sppb.TypeCode_TIMESTAMP, commitTimestampPlaceholderString), nil
		}
		// break
	default:
		// break
	}
	return zeroGCV, fmt.Errorf("not implemented: %s", expr.SQL())
}

func inferArrayElementType(exprs []ast.Expr, gcvs []spanner.GenericColumnValue) *sppb.Type {
	// exprs and gcvs are derived from the same ArrayLiteral.Values slice, so
	// their indexes stay aligned here.
	if len(exprs) == 0 {
		return nil
	}

	var first *sppb.Type
	var hasInt64, hasNumeric, hasFloat32, hasFloat64, hasOther bool
	for i, expr := range exprs {
		if _, ok := unwrapParenExpr(expr).(*ast.NullLiteral); ok {
			continue
		}
		typ := gcvs[i].Type
		if first == nil {
			first = typ
		}
		switch typ.GetCode() {
		case sppb.TypeCode_INT64:
			hasInt64 = true
		case sppb.TypeCode_NUMERIC:
			hasNumeric = true
		case sppb.TypeCode_FLOAT32:
			hasFloat32 = true
		case sppb.TypeCode_FLOAT64:
			hasFloat64 = true
		default:
			hasOther = true
		}
	}
	if first == nil {
		return typector.Int64()
	}
	if hasOther {
		return first
	}

	switch {
	case hasFloat64:
		return typector.Float64()
	case hasFloat32 && (hasInt64 || hasNumeric):
		return typector.Float64()
	case hasFloat32:
		return typector.Float32()
	case hasNumeric:
		return typector.Numeric()
	case hasInt64:
		return typector.Int64()
	default:
		return first
	}
}

func unwrapParenExpr(expr ast.Expr) ast.Expr {
	for {
		paren, ok := expr.(*ast.ParenExpr)
		if !ok {
			return expr
		}
		expr = paren.Expr
	}
}

func arrayLiteralValueOf(elemType *sppb.Type, gcvs []spanner.GenericColumnValue) (spanner.GenericColumnValue, error) {
	normalized, err := gcvctor.NormalizeArrayElements(elemType, gcvs...)
	if err != nil {
		if !errors.Is(err, gcvctor.ErrTypeMismatch) {
			return zeroGCV, err
		}

		coerced, coerceErr := coerceArrayElements(elemType, gcvs)
		if coerceErr == nil {
			return gcvctor.ArrayValueOf(elemType, coerced...)
		}

		// Preserve the current permissive behavior for array literals whose
		// element values do not all match elemType. This intentionally keeps the
		// original element wire values even when they disagree with elemType,
		// because memebridge does not model array coercion yet and tightening this
		// path would change the pre-v0.3 behavior that downstream callers already
		// rely on.
		return spanner.GenericColumnValue{
			Type: typector.ElemTypeToArrayType(elemType),
			Value: structpb.NewListValue(&structpb.ListValue{Values: lo.Map(gcvs, func(gcv spanner.GenericColumnValue, _ int) *structpb.Value {
				return gcvToValue(gcv)
			})}),
		}, nil
	}

	return gcvctor.ArrayValueOf(elemType, normalized...)
}

func coerceArrayElements(elemType *sppb.Type, gcvs []spanner.GenericColumnValue) ([]spanner.GenericColumnValue, error) {
	coerced := make([]spanner.GenericColumnValue, 0, len(gcvs))
	for _, gcv := range gcvs {
		if isNullGCV(gcv) {
			coerced = append(coerced, gcvctor.NullOf(elemType))
			continue
		}
		if proto.Equal(gcv.Type, elemType) {
			coerced = append(coerced, gcv)
			continue
		}
		elem, err := coerceArrayElement(elemType, gcv)
		if err != nil {
			return nil, err
		}
		coerced = append(coerced, elem)
	}
	return coerced, nil
}

func coerceArrayElement(elemType *sppb.Type, gcv spanner.GenericColumnValue) (spanner.GenericColumnValue, error) {
	// This is not the full CAST matrix. It only models array literal coercions
	// that are safe locally; CAST-only conversions such as FLOAT64 to NUMERIC
	// and NUMERIC to FLOAT32 intentionally fall back to preserving wire values.
	switch elemType.GetCode() {
	case sppb.TypeCode_NUMERIC:
		if gcv.Type.GetCode() == sppb.TypeCode_INT64 {
			v, err := int64FromGCV(gcv)
			if err != nil {
				return zeroGCV, err
			}
			return gcvctor.NumericValueChecked(big.NewRat(v, 1))
		}
	case sppb.TypeCode_FLOAT32:
		return coerceArrayElementToFloat32(gcv)
	case sppb.TypeCode_FLOAT64:
		return coerceArrayElementToFloat64(gcv)
	}
	return zeroGCV, fmt.Errorf("cannot coerce array element from %v to %v", gcv.Type.GetCode(), elemType.GetCode())
}

func coerceArrayElementToFloat32(gcv spanner.GenericColumnValue) (spanner.GenericColumnValue, error) {
	// Reuse scalar CAST helpers from cast.go so float narrowing and wire-value
	// extraction stay consistent between CAST emulation and array coercion.
	switch gcv.Type.GetCode() {
	case sppb.TypeCode_INT64:
		v, err := int64FromGCV(gcv)
		if err != nil {
			return zeroGCV, err
		}
		return float32ValueFromFloat64(float64(v))
	case sppb.TypeCode_FLOAT64:
		v, err := float64FromGCV(gcv, 64)
		if err != nil {
			return zeroGCV, err
		}
		return float32ValueFromFloat64(v)
	default:
		return zeroGCV, fmt.Errorf("cannot coerce array element from %v to FLOAT32", gcv.Type.GetCode())
	}
}

func coerceArrayElementToFloat64(gcv spanner.GenericColumnValue) (spanner.GenericColumnValue, error) {
	switch gcv.Type.GetCode() {
	case sppb.TypeCode_INT64:
		v, err := int64FromGCV(gcv)
		if err != nil {
			return zeroGCV, err
		}
		return gcvctor.Float64Value(float64(v)), nil
	case sppb.TypeCode_FLOAT32:
		v, err := float64FromGCV(gcv, 32)
		if err != nil {
			return zeroGCV, err
		}
		return gcvctor.Float64Value(v), nil
	case sppb.TypeCode_NUMERIC:
		v, err := stringFromGCV(gcv)
		if err != nil {
			return zeroGCV, err
		}
		n, ok := new(big.Rat).SetString(v)
		if !ok {
			return zeroGCV, fmt.Errorf("invalid NUMERIC wire value: %q", v)
		}
		f, _ := n.Float64()
		return gcvctor.Float64Value(f), nil
	default:
		return zeroGCV, fmt.Errorf("cannot coerce array element from %v to FLOAT64", gcv.Type.GetCode())
	}
}

func nameOrEmpty(f *ast.StructField) string {
	if f != nil && f.Ident != nil {
		return f.Ident.Name
	}
	return ""
}

func gcvToValue(gcv spanner.GenericColumnValue) *structpb.Value {
	if gcv.Value == nil {
		return structpb.NewNullValue()
	}
	return gcv.Value
}
