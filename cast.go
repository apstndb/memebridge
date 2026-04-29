package memebridge

import (
	"encoding/base64"
	"fmt"
	"math"
	"math/big"
	"strconv"
	"strings"
	"unicode/utf8"

	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/apstndb/spanvalue/gcvctor"
	"github.com/cloudspannerecosystem/memefish/ast"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

const (
	minInt64Float          = -9223372036854775808.0
	maxInt64FloatExclusive = 9223372036854775808.0
)

func memefishCastExprToGCV(cast *ast.CastExpr) (spanner.GenericColumnValue, error) {
	destType, err := MemefishTypeToSpannerpbType(cast.Type)
	if err != nil {
		return zeroGCV, err
	}

	if _, ok := unwrapParenExpr(cast.Expr).(*ast.NullLiteral); ok {
		return gcvctor.NullOf(destType), nil
	}

	src, err := MemefishExprToGCV(cast.Expr)
	if err != nil {
		return zeroGCV, err
	}
	if isNullGCV(src) {
		return gcvctor.NullOf(destType), nil
	}

	return castGCV(src, destType, cast.Expr.SQL())
}

func castGCV(src spanner.GenericColumnValue, destType *sppb.Type, exprSQL string) (spanner.GenericColumnValue, error) {
	srcCode := src.Type.GetCode()
	destCode := destType.GetCode()
	if proto.Equal(src.Type, destType) {
		return spanner.GenericColumnValue{Type: destType, Value: src.Value}, nil
	}

	switch destCode {
	case sppb.TypeCode_BOOL:
		return castGCVToBool(src, exprSQL)
	case sppb.TypeCode_INT64:
		return castGCVToInt64(src, exprSQL)
	case sppb.TypeCode_FLOAT32:
		return castGCVToFloat32(src, exprSQL)
	case sppb.TypeCode_FLOAT64:
		return castGCVToFloat64(src, exprSQL)
	case sppb.TypeCode_NUMERIC:
		return castGCVToNumeric(src, exprSQL)
	case sppb.TypeCode_STRING:
		return castGCVToString(src, exprSQL)
	case sppb.TypeCode_BYTES:
		return castGCVToBytes(src, exprSQL)
	case sppb.TypeCode_DATE:
		return castGCVToDate(src, exprSQL)
	case sppb.TypeCode_TIMESTAMP:
		return castGCVToTimestamp(src, exprSQL)
	case sppb.TypeCode_UUID, sppb.TypeCode_INTERVAL:
		return castStringBasedGCV(src, destCode, exprSQL)
	default:
		return zeroGCV, unsupportedCastError(srcCode, destCode, exprSQL)
	}
}

func castGCVToBool(src spanner.GenericColumnValue, exprSQL string) (spanner.GenericColumnValue, error) {
	switch src.Type.GetCode() {
	case sppb.TypeCode_INT64:
		v, err := int64FromGCV(src)
		if err != nil {
			return zeroGCV, err
		}
		return gcvctor.BoolValue(v != 0), nil
	case sppb.TypeCode_STRING:
		v, err := stringFromGCV(src)
		if err != nil {
			return zeroGCV, err
		}
		switch {
		case strings.EqualFold(v, "true"):
			return gcvctor.BoolValue(true), nil
		case strings.EqualFold(v, "false"):
			return gcvctor.BoolValue(false), nil
		default:
			return zeroGCV, fmt.Errorf("invalid BOOL literal for cast of %s to BOOL: %q", exprSQL, v)
		}
	default:
		return zeroGCV, unsupportedCastError(src.Type.GetCode(), sppb.TypeCode_BOOL, exprSQL)
	}
}

func castGCVToInt64(src spanner.GenericColumnValue, exprSQL string) (spanner.GenericColumnValue, error) {
	switch src.Type.GetCode() {
	case sppb.TypeCode_BOOL:
		v, err := boolFromGCV(src)
		if err != nil {
			return zeroGCV, err
		}
		if v {
			return gcvctor.Int64Value(1), nil
		}
		return gcvctor.Int64Value(0), nil
	case sppb.TypeCode_STRING:
		v, err := stringFromGCV(src)
		if err != nil {
			return zeroGCV, err
		}
		i, err := parseSpannerInt64(v)
		if err != nil {
			return zeroGCV, err
		}
		return gcvctor.Int64Value(i), nil
	case sppb.TypeCode_FLOAT32, sppb.TypeCode_FLOAT64:
		v, err := float64FromGCV(src, 64)
		if err != nil {
			return zeroGCV, err
		}
		i, err := roundFloatToInt64(v)
		if err != nil {
			return zeroGCV, err
		}
		return gcvctor.Int64Value(i), nil
	default:
		return zeroGCV, unsupportedCastError(src.Type.GetCode(), sppb.TypeCode_INT64, exprSQL)
	}
}

func castGCVToFloat32(src spanner.GenericColumnValue, exprSQL string) (spanner.GenericColumnValue, error) {
	switch src.Type.GetCode() {
	case sppb.TypeCode_INT64:
		v, err := int64FromGCV(src)
		if err != nil {
			return zeroGCV, err
		}
		return float32ValueFromFloat64(float64(v))
	case sppb.TypeCode_FLOAT64, sppb.TypeCode_FLOAT32:
		v, err := float64FromGCV(src, 32)
		if err != nil {
			return zeroGCV, err
		}
		return float32ValueFromFloat64(v)
	case sppb.TypeCode_STRING:
		v, err := stringFromGCV(src)
		if err != nil {
			return zeroGCV, err
		}
		f, err := parseSpannerFloat(v, 32)
		if err != nil {
			return zeroGCV, err
		}
		return float32ValueFromFloat64(f)
	default:
		return zeroGCV, unsupportedCastError(src.Type.GetCode(), sppb.TypeCode_FLOAT32, exprSQL)
	}
}

func castGCVToFloat64(src spanner.GenericColumnValue, exprSQL string) (spanner.GenericColumnValue, error) {
	switch src.Type.GetCode() {
	case sppb.TypeCode_INT64:
		v, err := int64FromGCV(src)
		if err != nil {
			return zeroGCV, err
		}
		return gcvctor.Float64Value(float64(v)), nil
	case sppb.TypeCode_FLOAT32, sppb.TypeCode_FLOAT64:
		v, err := float64FromGCV(src, 64)
		if err != nil {
			return zeroGCV, err
		}
		return gcvctor.Float64Value(v), nil
	case sppb.TypeCode_STRING:
		v, err := stringFromGCV(src)
		if err != nil {
			return zeroGCV, err
		}
		f, err := parseSpannerFloat(v, 64)
		if err != nil {
			return zeroGCV, err
		}
		return gcvctor.Float64Value(f), nil
	default:
		return zeroGCV, unsupportedCastError(src.Type.GetCode(), sppb.TypeCode_FLOAT64, exprSQL)
	}
}

func castGCVToNumeric(src spanner.GenericColumnValue, exprSQL string) (spanner.GenericColumnValue, error) {
	switch src.Type.GetCode() {
	case sppb.TypeCode_INT64:
		v, err := int64FromGCV(src)
		if err != nil {
			return zeroGCV, err
		}
		return gcvctor.NumericValueChecked(big.NewRat(v, 1))
	case sppb.TypeCode_STRING:
		v, err := stringFromGCV(src)
		if err != nil {
			return zeroGCV, err
		}
		v = strings.TrimSpace(v)
		if strings.Contains(v, "/") {
			return zeroGCV, fmt.Errorf("invalid NUMERIC literal for cast of %s to NUMERIC: %q", exprSQL, v)
		}
		if strings.Contains(v, "0x") || strings.Contains(v, "0X") {
			return zeroGCV, fmt.Errorf("invalid NUMERIC literal for cast of %s to NUMERIC: %q", exprSQL, v)
		}
		if strings.ContainsAny(v, "eE") {
			return zeroGCV, fmt.Errorf("invalid NUMERIC literal for cast of %s to NUMERIC: %q", exprSQL, v)
		}
		n, ok := new(big.Rat).SetString(v)
		if !ok {
			return zeroGCV, fmt.Errorf("invalid NUMERIC literal for cast of %s to NUMERIC: %q", exprSQL, v)
		}
		return gcvctor.NumericValueChecked(n)
	default:
		return zeroGCV, unsupportedCastError(src.Type.GetCode(), sppb.TypeCode_NUMERIC, exprSQL)
	}
}

func castGCVToString(src spanner.GenericColumnValue, exprSQL string) (spanner.GenericColumnValue, error) {
	switch src.Type.GetCode() {
	case sppb.TypeCode_BOOL:
		v, err := boolFromGCV(src)
		if err != nil {
			return zeroGCV, err
		}
		return gcvctor.StringValue(strconv.FormatBool(v)), nil
	case sppb.TypeCode_INT64,
		sppb.TypeCode_DATE,
		sppb.TypeCode_TIMESTAMP,
		sppb.TypeCode_UUID,
		sppb.TypeCode_INTERVAL:
		v, err := stringFromGCV(src)
		if err != nil {
			return zeroGCV, err
		}
		return gcvctor.StringValue(v), nil
	case sppb.TypeCode_NUMERIC:
		v, err := stringFromGCV(src)
		if err != nil {
			return zeroGCV, err
		}
		return gcvctor.StringValue(formatNumericString(v)), nil
	case sppb.TypeCode_FLOAT32:
		v, err := float64FromGCV(src, 32)
		if err != nil {
			return zeroGCV, err
		}
		return gcvctor.StringValue(formatSpannerFloat(v, 32)), nil
	case sppb.TypeCode_FLOAT64:
		v, err := float64FromGCV(src, 64)
		if err != nil {
			return zeroGCV, err
		}
		return gcvctor.StringValue(formatSpannerFloat(v, 64)), nil
	case sppb.TypeCode_BYTES:
		v, err := bytesFromGCV(src)
		if err != nil {
			return zeroGCV, err
		}
		if !utf8.Valid(v) {
			return zeroGCV, fmt.Errorf("invalid UTF-8 bytes for STRING cast in expression %q", exprSQL)
		}
		return gcvctor.StringValue(string(v)), nil
	default:
		return zeroGCV, unsupportedCastError(src.Type.GetCode(), sppb.TypeCode_STRING, exprSQL)
	}
}

func castGCVToBytes(src spanner.GenericColumnValue, exprSQL string) (spanner.GenericColumnValue, error) {
	if src.Type.GetCode() != sppb.TypeCode_STRING {
		return zeroGCV, unsupportedCastError(src.Type.GetCode(), sppb.TypeCode_BYTES, exprSQL)
	}
	v, err := stringFromGCV(src)
	if err != nil {
		return zeroGCV, err
	}
	return gcvctor.BytesValue([]byte(v)), nil
}

func castGCVToDate(src spanner.GenericColumnValue, exprSQL string) (spanner.GenericColumnValue, error) {
	if src.Type.GetCode() != sppb.TypeCode_STRING {
		return zeroGCV, unsupportedCastError(src.Type.GetCode(), sppb.TypeCode_DATE, exprSQL)
	}
	v, err := stringFromGCV(src)
	if err != nil {
		return zeroGCV, err
	}
	return gcvctor.DateStringValue(strings.TrimSpace(v))
}

func castGCVToTimestamp(src spanner.GenericColumnValue, exprSQL string) (spanner.GenericColumnValue, error) {
	if src.Type.GetCode() != sppb.TypeCode_STRING {
		return zeroGCV, unsupportedCastError(src.Type.GetCode(), sppb.TypeCode_TIMESTAMP, exprSQL)
	}
	v, err := stringFromGCV(src)
	if err != nil {
		return zeroGCV, err
	}
	return gcvctor.TimestampStringValue(strings.TrimSpace(v))
}

func castStringBasedGCV(src spanner.GenericColumnValue, destCode sppb.TypeCode, exprSQL string) (spanner.GenericColumnValue, error) {
	if src.Type.GetCode() != sppb.TypeCode_STRING {
		return zeroGCV, unsupportedCastError(src.Type.GetCode(), destCode, exprSQL)
	}
	v, err := stringFromGCV(src)
	if err != nil {
		return zeroGCV, err
	}
	v = strings.TrimSpace(v)
	switch destCode {
	case sppb.TypeCode_UUID:
		u, err := uuid.Parse(v)
		if err != nil || !strings.EqualFold(u.String(), v) {
			return zeroGCV, fmt.Errorf("invalid UUID literal for cast of %s to UUID: %q", exprSQL, v)
		}
		return gcvctor.UUIDValue(u), nil
	case sppb.TypeCode_INTERVAL:
		return gcvctor.IntervalStringValue(v)
	default:
		return gcvctor.StringBasedValueFromCode(destCode, v), nil
	}
}

func isNullGCV(gcv spanner.GenericColumnValue) bool {
	if gcv.Value == nil {
		return true
	}
	_, ok := gcv.Value.GetKind().(*structpb.Value_NullValue)
	return ok
}

func boolFromGCV(gcv spanner.GenericColumnValue) (bool, error) {
	v, ok := gcv.Value.GetKind().(*structpb.Value_BoolValue)
	if !ok {
		return false, fmt.Errorf("expected BOOL wire value, got %T", gcv.Value.GetKind())
	}
	return v.BoolValue, nil
}

func stringFromGCV(gcv spanner.GenericColumnValue) (string, error) {
	v, ok := gcv.Value.GetKind().(*structpb.Value_StringValue)
	if !ok {
		return "", fmt.Errorf("expected STRING wire value, got %T", gcv.Value.GetKind())
	}
	return v.StringValue, nil
}

func bytesFromGCV(gcv spanner.GenericColumnValue) ([]byte, error) {
	v, err := stringFromGCV(gcv)
	if err != nil {
		return nil, err
	}
	return base64.StdEncoding.DecodeString(v)
}

func int64FromGCV(gcv spanner.GenericColumnValue) (int64, error) {
	v, err := stringFromGCV(gcv)
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(v, 10, 64)
}

func float64FromGCV(gcv spanner.GenericColumnValue, bitSize int) (float64, error) {
	switch v := gcv.Value.GetKind().(type) {
	case *structpb.Value_NumberValue:
		return v.NumberValue, nil
	case *structpb.Value_StringValue:
		return parseSpannerFloat(v.StringValue, bitSize)
	default:
		return 0, fmt.Errorf("expected floating-point wire value, got %T", gcv.Value.GetKind())
	}
}

func parseSpannerInt64(v string) (int64, error) {
	v = strings.TrimSpace(v)
	unsigned := v
	if strings.HasPrefix(unsigned, "+") || strings.HasPrefix(unsigned, "-") {
		unsigned = unsigned[1:]
	}
	if strings.HasPrefix(unsigned, "0x") || strings.HasPrefix(unsigned, "0X") {
		return strconv.ParseInt(v, 0, 64)
	}
	return strconv.ParseInt(v, 10, 64)
}

func parseSpannerFloat(v string, bitSize int) (float64, error) {
	v = strings.TrimSpace(v)
	switch strings.ToLower(v) {
	case "nan", "+nan", "-nan":
		return math.NaN(), nil
	case "inf", "+inf", "infinity", "+infinity":
		return math.Inf(1), nil
	case "-inf", "-infinity":
		return math.Inf(-1), nil
	default:
		return strconv.ParseFloat(v, bitSize)
	}
}

func float32ValueFromFloat64(v float64) (spanner.GenericColumnValue, error) {
	f32 := float32(v)
	if !math.IsInf(v, 0) && math.IsInf(float64(f32), 0) {
		return zeroGCV, fmt.Errorf("FLOAT64 value out of FLOAT32 range: %v", v)
	}
	return gcvctor.Float32Value(f32), nil
}

func roundFloatToInt64(v float64) (int64, error) {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return 0, fmt.Errorf("cannot cast non-finite floating-point value to INT64: %v", v)
	}
	// Spanner CAST(FLOAT* AS INT64) rounds halfway cases away from zero.
	rounded := math.Round(v)
	if rounded < minInt64Float || rounded >= maxInt64FloatExclusive {
		return 0, fmt.Errorf("floating-point value out of INT64 range: %v", v)
	}
	return int64(rounded), nil
}

func formatNumericString(v string) string {
	if !strings.Contains(v, ".") {
		return v
	}
	v = strings.TrimRight(v, "0")
	return strings.TrimSuffix(v, ".")
}

func formatSpannerFloat(v float64, bitSize int) string {
	switch {
	case math.IsNaN(v):
		return "NaN"
	case math.IsInf(v, 1):
		return "Infinity"
	case math.IsInf(v, -1):
		return "-Infinity"
	case v == 0:
		return "0"
	default:
		return strconv.FormatFloat(v, 'g', -1, bitSize)
	}
}

func unsupportedCastError(srcCode, destCode sppb.TypeCode, exprSQL string) error {
	if exprSQL == "" {
		return fmt.Errorf("unsupported cast from %s to %s", srcCode, destCode)
	}
	return fmt.Errorf("unsupported cast from %s to %s: %s", srcCode, destCode, exprSQL)
}
