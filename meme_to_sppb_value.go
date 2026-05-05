package memebridge

import (
	"errors"
	"fmt"
	"math/big"
	"slices"
	"strconv"
	"strings"

	"github.com/apstndb/spantype/typector"
	"github.com/apstndb/spanvalue/gcvctor"
	"github.com/cloudspannerecosystem/memefish/char"
	"github.com/samber/lo"

	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/cloudspannerecosystem/memefish/ast"
	"github.com/google/uuid"
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
	case *ast.TupleStructLiteral:
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

		names = slices.Repeat([]string{""}, len(gcvs))
	case *ast.TypedStructLiteral:
		return typedStructLiteralToGCV(e)
	default:
		return zeroGCV, fmt.Errorf("expr is not struct literal: %v", e)
	}

	return gcvctor.StructValueOf(names, gcvs)
}

func typedStructLiteralToGCV(expr *ast.TypedStructLiteral) (spanner.GenericColumnValue, error) {
	if len(expr.Fields) != len(expr.Values) {
		return zeroGCV, fmt.Errorf("typed struct literal has %d fields but %d values", len(expr.Fields), len(expr.Values))
	}

	names := make([]string, len(expr.Fields))
	gcvs := make([]spanner.GenericColumnValue, len(expr.Values))
	for i, field := range expr.Fields {
		if field == nil {
			return zeroGCV, fmt.Errorf("typed struct literal has nil field at index %d", i)
		}
		fieldType, err := MemefishTypeToSpannerpbType(field.Type)
		if err != nil {
			return zeroGCV, err
		}
		coerced, err := memefishExprToGCVWithExpectedType(fieldType, expr.Values[i])
		if err != nil {
			return zeroGCV, fmt.Errorf("cannot coerce typed struct field %d (%s): %w", i, expr.Values[i].SQL(), err)
		}
		names[i] = fieldNameOrEmpty(field)
		gcvs[i] = coerced
	}
	return gcvctor.StructValueOf(names, gcvs)
}

func memefishExprToGCVWithExpectedType(expectedType *sppb.Type, expr ast.Expr) (spanner.GenericColumnValue, error) {
	if expectedType == nil {
		return MemefishExprToGCV(expr)
	}

	unwrapped := unwrapParenExpr(expr)
	switch expectedType.GetCode() {
	case sppb.TypeCode_ARRAY:
		array, ok := unwrapped.(*ast.ArrayLiteral)
		if ok && array.Type != nil {
			gcv, err := arrayLiteralToGCVStrict(array, nil)
			if err != nil {
				return zeroGCV, err
			}
			return castGCV(gcv, expectedType, expr.SQL())
		}
	case sppb.TypeCode_STRUCT:
		switch e := unwrapped.(type) {
		case *ast.TypedStructLiteral:
			gcv, err := typedStructLiteralToGCV(e)
			if err != nil {
				return zeroGCV, err
			}
			return castGCV(gcv, expectedType, expr.SQL())
		case *ast.TypelessStructLiteral, *ast.TupleStructLiteral:
			return structLiteralToGCVWithExpectedType(expectedType, unwrapped)
		}
	}

	gcv, err := MemefishExprToGCV(expr)
	if err != nil {
		return zeroGCV, err
	}
	return coerceToExpectedType(expectedType, gcv, expr)
}

func structLiteralToGCVWithExpectedType(expectedType *sppb.Type, expr ast.Expr) (spanner.GenericColumnValue, error) {
	structType := expectedType.GetStructType()
	if structType == nil {
		return zeroGCV, fmt.Errorf("malformed expected STRUCT type")
	}
	fields := structType.GetFields()
	values, err := structLiteralValueExprs(expr)
	if err != nil {
		return zeroGCV, err
	}
	if len(fields) != len(values) {
		return zeroGCV, fmt.Errorf("STRUCT literal has %d fields but expected type has %d fields", len(values), len(fields))
	}

	names := make([]string, len(fields))
	gcvs := make([]spanner.GenericColumnValue, len(values))
	for i, field := range fields {
		if field == nil || field.Type == nil {
			return zeroGCV, fmt.Errorf("expected STRUCT type has nil field at index %d", i)
		}
		gcv, err := memefishExprToGCVWithExpectedType(field.Type, values[i])
		if err != nil {
			return zeroGCV, fmt.Errorf("cannot coerce struct field %d (%s): %w", i, values[i].SQL(), err)
		}
		names[i] = field.Name
		gcvs[i] = gcv
	}
	return gcvctor.StructValueOf(names, gcvs)
}

func structLiteralValueExprs(expr ast.Expr) ([]ast.Expr, error) {
	switch e := expr.(type) {
	case *ast.TypelessStructLiteral:
		values := make([]ast.Expr, len(e.Values))
		for i, value := range e.Values {
			switch v := value.(type) {
			case *ast.ExprArg:
				values[i] = v.Expr
			case *ast.Alias:
				values[i] = v.Expr
			default:
				return nil, fmt.Errorf("unknown struct literal arg: %v", value)
			}
		}
		return values, nil
	case *ast.TupleStructLiteral:
		return e.Values, nil
	case *ast.TypedStructLiteral:
		return e.Values, nil
	default:
		return nil, fmt.Errorf("expr is not struct literal: %v", e)
	}
}

func coerceToExpectedType(
	expectedType *sppb.Type,
	gcv spanner.GenericColumnValue,
	expr ast.Expr,
) (spanner.GenericColumnValue, error) {
	if proto.Equal(gcv.Type, expectedType) {
		return spanner.GenericColumnValue{Type: expectedType, Value: gcv.Value}, nil
	}
	if isNullGCV(gcv) && isUntypedNullLiteral(expr) {
		return gcvctor.NullOf(expectedType), nil
	}
	if isNullGCV(gcv) {
		if canCoerceTypedNullToField(expectedType, gcv.Type) {
			return gcvctor.NullOf(expectedType), nil
		}
		return zeroGCV, fmt.Errorf(
			"cannot coerce expression from %v to %v: %s",
			gcv.Type.GetCode(),
			expectedType.GetCode(),
			expr.SQL(),
		)
	}
	if !canCoerceToExpectedType(expectedType, gcv.Type, expr) {
		return zeroGCV, fmt.Errorf(
			"cannot coerce expression from %v to %v: %s",
			gcv.Type.GetCode(),
			expectedType.GetCode(),
			expr.SQL(),
		)
	}
	if isStringLiteralCoercion(expectedType, gcv.Type, expr) {
		return coerceStringLiteralToExpectedType(expectedType, expr)
	}
	return castGCV(gcv, expectedType, expr.SQL())
}

func canCoerceToExpectedType(expectedType, valueType *sppb.Type, expr ast.Expr) bool {
	switch expectedType.GetCode() {
	case sppb.TypeCode_NUMERIC:
		return valueType.GetCode() == sppb.TypeCode_INT64 ||
			isNumericLiteralForType(expr, valueType, sppb.TypeCode_NUMERIC)
	case sppb.TypeCode_FLOAT32:
		return isNumericLiteralForType(expr, valueType, sppb.TypeCode_FLOAT32)
	case sppb.TypeCode_FLOAT64:
		switch valueType.GetCode() {
		case sppb.TypeCode_INT64, sppb.TypeCode_FLOAT32, sppb.TypeCode_NUMERIC:
			return true
		default:
			return false
		}
	case sppb.TypeCode_DATE, sppb.TypeCode_TIMESTAMP, sppb.TypeCode_UUID:
		return valueType.GetCode() == sppb.TypeCode_STRING && isStringLiteral(expr)
	default:
		return false
	}
}

func canCoerceTypedNullToField(fieldType, valueType *sppb.Type) bool {
	switch fieldType.GetCode() {
	case sppb.TypeCode_NUMERIC:
		return valueType.GetCode() == sppb.TypeCode_INT64
	case sppb.TypeCode_FLOAT64:
		switch valueType.GetCode() {
		case sppb.TypeCode_INT64, sppb.TypeCode_FLOAT32, sppb.TypeCode_NUMERIC:
			return true
		default:
			return false
		}
	default:
		return false
	}
}

func isUntypedNullLiteral(expr ast.Expr) bool {
	_, ok := unwrapParenExpr(expr).(*ast.NullLiteral)
	return ok
}

func isNumericLiteralForType(expr ast.Expr, typ *sppb.Type, target sppb.TypeCode) bool {
	switch unwrapParenExpr(expr).(type) {
	case *ast.IntLiteral:
		return target == sppb.TypeCode_FLOAT32 || target == sppb.TypeCode_NUMERIC
	case *ast.FloatLiteral:
		return typ.GetCode() == sppb.TypeCode_FLOAT64 &&
			(target == sppb.TypeCode_FLOAT32 || target == sppb.TypeCode_NUMERIC)
	default:
		return false
	}
}

func isStringLiteral(expr ast.Expr) bool {
	_, ok := unwrapParenExpr(expr).(*ast.StringLiteral)
	return ok
}

func isStringLiteralCoercion(fieldType, valueType *sppb.Type, expr ast.Expr) bool {
	switch fieldType.GetCode() {
	case sppb.TypeCode_DATE, sppb.TypeCode_TIMESTAMP, sppb.TypeCode_UUID:
		return valueType.GetCode() == sppb.TypeCode_STRING && isStringLiteral(expr)
	default:
		return false
	}
}

func coerceStringLiteralToExpectedType(
	expectedType *sppb.Type,
	expr ast.Expr,
) (spanner.GenericColumnValue, error) {
	lit, ok := unwrapParenExpr(expr).(*ast.StringLiteral)
	if !ok {
		return zeroGCV, fmt.Errorf("expected string literal for coercion: %s", expr.SQL())
	}
	// GoogleSQL literal coercion is stricter than CAST parsing here. Do not
	// trim whitespace; only canonical literal text should satisfy an expected
	// DATE, TIMESTAMP, or UUID field type.
	switch expectedType.GetCode() {
	case sppb.TypeCode_DATE:
		return gcvctor.DateStringValue(lit.Value)
	case sppb.TypeCode_TIMESTAMP:
		return gcvctor.TimestampStringValue(lit.Value)
	case sppb.TypeCode_UUID:
		u, err := uuid.Parse(lit.Value)
		if err != nil {
			return zeroGCV, fmt.Errorf("invalid UUID literal %q for expected type %v: %w", lit.Value, expectedType.GetCode(), err)
		}
		if !strings.EqualFold(u.String(), lit.Value) {
			return zeroGCV, fmt.Errorf("invalid UUID literal %q for expected type %v", lit.Value, expectedType.GetCode())
		}
		return gcvctor.UUIDValue(u), nil
	default:
		return zeroGCV, fmt.Errorf("cannot coerce string literal to expected type %v", expectedType.GetCode())
	}
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
		return arrayLiteralToGCV(e, nil)
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

func arrayLiteralToGCV(
	expr *ast.ArrayLiteral,
	expectedElemType *sppb.Type,
) (spanner.GenericColumnValue, error) {
	return arrayLiteralToGCVWithFallback(expr, expectedElemType, true)
}

func arrayLiteralToGCVStrict(
	expr *ast.ArrayLiteral,
	expectedElemType *sppb.Type,
) (spanner.GenericColumnValue, error) {
	return arrayLiteralToGCVWithFallback(expr, expectedElemType, false)
}

func arrayLiteralToGCVWithFallback(
	expr *ast.ArrayLiteral,
	expectedElemType *sppb.Type,
	allowFallback bool,
) (spanner.GenericColumnValue, error) {
	// An explicit ARRAY<T> annotation takes precedence; otherwise use a local
	// expected element type if one is available, then fall back to inference.
	elemType := expectedElemType
	var err error
	if expr.Type != nil {
		elemType, err = MemefishTypeToSpannerpbType(expr.Type)
		if err != nil {
			return zeroGCV, err
		}
	}
	gcvs, err := arrayLiteralElementsToGCVs(expr.Values, elemType, allowFallback)
	if err != nil {
		return zeroGCV, err
	}
	if elemType == nil {
		elemType = inferArrayElementType(expr.Values, gcvs)
	}
	if elemType == nil {
		return zeroGCV, ErrCannotInferArrayElementType
	}

	return arrayLiteralValueOf(elemType, expr.Values, gcvs, allowFallback)
}

func arrayLiteralElementsToGCVs(
	values []ast.Expr,
	expectedElemType *sppb.Type,
	allowFallback bool,
) ([]spanner.GenericColumnValue, error) {
	if expectedElemType != nil {
		gcvs, err := lo.MapErr(values, func(value ast.Expr, _ int) (spanner.GenericColumnValue, error) {
			return memefishExprToGCVWithExpectedType(expectedElemType, value)
		})
		if err == nil || !allowFallback {
			return gcvs, err
		}
	}
	return lo.MapErr(values, func(value ast.Expr, _ int) (spanner.GenericColumnValue, error) {
		return MemefishExprToGCV(value)
	})
}

func inferArrayElementType(exprs []ast.Expr, gcvs []spanner.GenericColumnValue) *sppb.Type {
	// exprs and gcvs are derived from the same ArrayLiteral.Values slice, so
	// their indexes stay aligned here.
	if len(exprs) == 0 {
		return nil
	}

	var (
		first                             *sppb.Type
		hasInt64, hasNumeric              bool
		hasFloat32, hasFloat64, hasString bool
		hasNonLiteralString, hasOther     bool
	)
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
		case sppb.TypeCode_STRING:
			hasString = true
			if !isStringLiteral(expr) {
				hasNonLiteralString = true
			}
		default:
			hasOther = true
		}
	}
	if first == nil {
		return typector.Int64()
	}

	allSame := true
	var nonStringType *sppb.Type
	for i, expr := range exprs {
		if _, ok := unwrapParenExpr(expr).(*ast.NullLiteral); ok {
			continue
		}
		typ := gcvs[i].Type
		if !equivalentSpannerTypes(first, typ) {
			allSame = false
		}
		if typ.GetCode() == sppb.TypeCode_STRING {
			continue
		}
		if nonStringType == nil {
			nonStringType = typ
			continue
		}
		if !equivalentSpannerTypes(nonStringType, typ) {
			nonStringType = nil
			break
		}
	}
	if allSame {
		return first
	}
	if hasString {
		if !hasNonLiteralString && nonStringType != nil && isStringLiteralCoercibleTypeCode(nonStringType.GetCode()) {
			return nonStringType
		}
		return nil
	}
	if hasOther {
		return nil
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
	}
	return nil
}

// Cloud Spanner's literal coercion table is narrower than explicit CAST:
// STRING literals may implicitly coerce to DATE/TIMESTAMP/UUID in typed
// contexts, but not to INTERVAL/INT64/NUMERIC. See:
// https://docs.cloud.google.com/spanner/docs/reference/standard-sql/conversion_rules
// https://github.com/google/googlesql/blob/36dd14aa0657ea299725504bc0f938732f58f380/googlesql/public/cast.h#L45-L66
// https://github.com/google/googlesql/blob/36dd14aa0657ea299725504bc0f938732f58f380/googlesql/public/cast.cc#L213-L297
func isStringLiteralCoercibleTypeCode(code sppb.TypeCode) bool {
	switch code {
	case sppb.TypeCode_DATE, sppb.TypeCode_TIMESTAMP, sppb.TypeCode_UUID:
		return true
	default:
		return false
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

func arrayLiteralValueOf(
	elemType *sppb.Type,
	exprs []ast.Expr,
	gcvs []spanner.GenericColumnValue,
	allowFallback bool,
) (spanner.GenericColumnValue, error) {
	if !allowFallback {
		coerced, err := coerceArrayElementsStrict(elemType, exprs, gcvs)
		if err != nil {
			return zeroGCV, err
		}
		return gcvctor.ArrayValueOf(elemType, coerced...)
	}

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

func coerceArrayElementsStrict(
	elemType *sppb.Type,
	exprs []ast.Expr,
	gcvs []spanner.GenericColumnValue,
) ([]spanner.GenericColumnValue, error) {
	coerced := make([]spanner.GenericColumnValue, len(gcvs))
	for i, gcv := range gcvs {
		elem, err := coerceToExpectedType(elemType, gcv, exprs[i])
		if err != nil {
			return nil, fmt.Errorf("cannot coerce array element %d (%s): %w", i, exprs[i].SQL(), err)
		}
		coerced[i] = elem
	}
	return coerced, nil
}

func coerceArrayElements(elemType *sppb.Type, gcvs []spanner.GenericColumnValue) ([]spanner.GenericColumnValue, error) {
	coerced := make([]spanner.GenericColumnValue, len(gcvs))
	for i, gcv := range gcvs {
		if isNullGCV(gcv) {
			coerced[i] = gcvctor.NullOf(elemType)
			continue
		}
		if equivalentSpannerTypes(gcv.Type, elemType) {
			coerced[i] = spanner.GenericColumnValue{Type: elemType, Value: gcv.Value}
			continue
		}
		elem, err := coerceArrayElement(elemType, gcv)
		if err != nil {
			return nil, err
		}
		coerced[i] = elem
	}
	return coerced, nil
}

func coerceArrayElement(elemType *sppb.Type, gcv spanner.GenericColumnValue) (spanner.GenericColumnValue, error) {
	// This is not the full CAST matrix. It only models array literal coercions
	// that are safe locally; CAST-only conversions such as FLOAT64 to NUMERIC
	// and NUMERIC to FLOAT32 intentionally fall back to preserving wire values.

	// Allow STRING values to coerce to any type that CAST supports.
	if gcv.Type.GetCode() == sppb.TypeCode_STRING {
		return castGCV(gcv, elemType, "")
	}

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
		n, err := numericFromGCV(gcv)
		if err != nil {
			return zeroGCV, err
		}
		f, _ := n.Float64()
		return gcvctor.Float64Value(f), nil
	default:
		return zeroGCV, fmt.Errorf("cannot coerce array element from %v to FLOAT64", gcv.Type.GetCode())
	}
}
