package memebridge

import (
	"encoding/base64"
	"errors"
	"fmt"
	"math"
	"math/big"
	"strconv"
	"strings"
	"sync"
	"time"
	_ "time/tzdata" // keep Spanner's default time zone available in minimal runtimes
	"unicode/utf8"

	"cloud.google.com/go/civil"
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
	spannerDefaultTimeZone = "America/Los_Angeles"
)

var (
	errUnsupportedCast = errors.New("unsupported cast")

	numericScaleFactor = pow10Int(spanner.NumericScaleDigits)
	maxScaledNumeric   = new(big.Int).Sub(pow10Int(spanner.NumericPrecisionDigits), big.NewInt(1))

	spannerDefaultLocation = sync.OnceValues(func() (*time.Location, error) {
		loc, err := time.LoadLocation(spannerDefaultTimeZone)
		if err != nil {
			return nil, fmt.Errorf("load Spanner default time zone %q: %w", spannerDefaultTimeZone, err)
		}
		return loc, nil
	})
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

	gcv, err := castGCV(src, destType, cast.Expr.SQL())
	if err == nil {
		return gcv, nil
	}
	if cast.Safe && !errors.Is(err, errUnsupportedCast) {
		return gcvctor.NullOf(destType), nil
	}
	return zeroGCV, err
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
		// BOOL casts only accept case-insensitive "true" or "false"; unlike
		// numeric and temporal string casts, surrounding whitespace stays invalid.
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
		i, err := roundFloatToInt64(v, exprSQL)
		if err != nil {
			return zeroGCV, err
		}
		return gcvctor.Int64Value(i), nil
	case sppb.TypeCode_NUMERIC:
		v, err := numericFromGCV(src)
		if err != nil {
			return zeroGCV, err
		}
		i, err := roundRatToInt64(v, exprSQL)
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
	case sppb.TypeCode_NUMERIC:
		v, err := numericFromGCV(src)
		if err != nil {
			return zeroGCV, err
		}
		f, _ := v.Float32()
		return gcvctor.Float32Value(f), nil
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
	case sppb.TypeCode_NUMERIC:
		v, err := numericFromGCV(src)
		if err != nil {
			return zeroGCV, err
		}
		f, _ := v.Float64()
		return gcvctor.Float64Value(f), nil
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
	case sppb.TypeCode_FLOAT32, sppb.TypeCode_FLOAT64:
		v, err := float64FromGCV(src, 64)
		if err != nil {
			return zeroGCV, err
		}
		return float64ToNumericValue(v, exprSQL)
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
		sppb.TypeCode_UUID,
		sppb.TypeCode_INTERVAL:
		v, err := stringFromGCV(src)
		if err != nil {
			return zeroGCV, err
		}
		return gcvctor.StringValue(v), nil
	case sppb.TypeCode_TIMESTAMP:
		wireValue, err := stringFromGCV(src)
		if err != nil {
			return zeroGCV, err
		}
		if wireValue == commitTimestampPlaceholderString {
			return gcvctor.StringValue(wireValue), nil
		}
		v, err := parseTimestampWireValueForCast(wireValue, exprSQL)
		if err != nil {
			return zeroGCV, err
		}
		loc, err := loadSpannerDefaultLocation()
		if err != nil {
			return zeroGCV, err
		}
		return gcvctor.StringValue(formatSpannerTimestampString(v.In(loc))), nil
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
	switch src.Type.GetCode() {
	case sppb.TypeCode_STRING:
		v, err := stringFromGCV(src)
		if err != nil {
			return zeroGCV, err
		}
		return gcvctor.DateStringValue(strings.TrimSpace(v))
	case sppb.TypeCode_TIMESTAMP:
		v, err := timestampFromGCV(src, exprSQL)
		if err != nil {
			return zeroGCV, err
		}
		loc, err := loadSpannerDefaultLocation()
		if err != nil {
			return zeroGCV, err
		}
		return gcvctor.DateValue(civil.DateOf(v.In(loc))), nil
	default:
		return zeroGCV, unsupportedCastError(src.Type.GetCode(), sppb.TypeCode_DATE, exprSQL)
	}
}

func castGCVToTimestamp(src spanner.GenericColumnValue, exprSQL string) (spanner.GenericColumnValue, error) {
	switch src.Type.GetCode() {
	case sppb.TypeCode_STRING:
		v, err := stringFromGCV(src)
		if err != nil {
			return zeroGCV, err
		}
		return timestampStringValueForCast(v, exprSQL)
	case sppb.TypeCode_DATE:
		v, err := dateFromGCV(src)
		if err != nil {
			return zeroGCV, err
		}
		loc, err := loadSpannerDefaultLocation()
		if err != nil {
			return zeroGCV, err
		}
		return gcvctor.TimestampValue(v.In(loc).UTC()), nil
	default:
		return zeroGCV, unsupportedCastError(src.Type.GetCode(), sppb.TypeCode_TIMESTAMP, exprSQL)
	}
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

func dateFromGCV(gcv spanner.GenericColumnValue) (civil.Date, error) {
	v, err := stringFromGCV(gcv)
	if err != nil {
		return civil.Date{}, err
	}
	return civil.ParseDate(v)
}

func timestampFromGCV(gcv spanner.GenericColumnValue, exprSQL string) (time.Time, error) {
	v, err := stringFromGCV(gcv)
	if err != nil {
		return time.Time{}, err
	}
	if v == commitTimestampPlaceholderString {
		return time.Time{}, fmt.Errorf("cannot cast pending commit timestamp placeholder%s", exprContextSuffix(exprSQL))
	}
	return parseTimestampWireValueForCast(v, exprSQL)
}

func parseTimestampWireValueForCast(v, exprSQL string) (time.Time, error) {
	t, err := parseSpannerTimestampForCast(strings.TrimSpace(v))
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid TIMESTAMP wire value for cast%s: %q: %w", exprContextSuffix(exprSQL), v, err)
	}
	return t, nil
}

func timestampStringValueForCast(v, exprSQL string) (spanner.GenericColumnValue, error) {
	t, err := parseSpannerTimestampForCast(strings.TrimSpace(v))
	if err != nil {
		return zeroGCV, fmt.Errorf("invalid TIMESTAMP literal for cast of %s to TIMESTAMP: %q: %w", exprSQL, v, err)
	}
	return gcvctor.TimestampValue(t.UTC()), nil
}

func parseSpannerTimestampForCast(v string) (time.Time, error) {
	if strings.HasSuffix(v, "z") {
		v = strings.TrimSuffix(v, "z") + "Z"
	}
	v = normalizeSpannerTimestampOffset(v)

	zonedLayouts := []string{
		time.RFC3339Nano,
		"2006-01-02 15:04:05.999999999Z07:00",
		"2006-01-02 15:04:05Z07:00",
		"2006-01-02 15:04:05.999999999Z07",
		"2006-01-02 15:04:05Z07",
	}
	for _, layout := range zonedLayouts {
		t, err := time.Parse(layout, v)
		if err == nil {
			return t, nil
		}
	}

	if hasNamedTimeZoneSuffix(v) {
		return parseSpannerTimestampWithNamedLocation(v)
	}

	loc, err := loadSpannerDefaultLocation()
	if err != nil {
		return time.Time{}, err
	}
	return parseSpannerTimestampInLocation(v, loc)
}

func parseSpannerTimestampWithNamedLocation(v string) (time.Time, error) {
	i := strings.LastIndexByte(v, ' ')
	loc, err := time.LoadLocation(v[i+1:])
	if err != nil {
		return time.Time{}, fmt.Errorf("load timestamp time zone %q for %q: %w", v[i+1:], v, err)
	}
	return parseSpannerTimestampInLocation(v[:i], loc)
}

func hasNamedTimeZoneSuffix(v string) bool {
	i := strings.LastIndexByte(v, ' ')
	return i >= 0 && strings.Contains(v[i+1:], "/")
}

func parseSpannerTimestampInLocation(v string, loc *time.Location) (time.Time, error) {
	localLayouts := []string{
		"2006-01-02",
		"2006-01-02T15:04:05.999999999",
		"2006-01-02T15:04:05",
		"2006-01-02 15:04:05.999999999",
		"2006-01-02 15:04:05",
	}
	for _, layout := range localLayouts {
		t, err := time.ParseInLocation(layout, v, loc)
		if err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("unsupported timestamp format: %q", v)
}

func normalizeSpannerTimestampOffset(v string) string {
	if len(v) < len("-0:00") {
		return v
	}
	i := len(v) - len("-0:00")
	if (v[i] != '+' && v[i] != '-') || v[i+2] != ':' {
		return v
	}
	if !isASCIIDigit(v[i+1]) || !isASCIIDigit(v[i+3]) || !isASCIIDigit(v[i+4]) {
		return v
	}
	return v[:i+1] + "0" + v[i+1:]
}

func isASCIIDigit(b byte) bool {
	return b >= '0' && b <= '9'
}

func loadSpannerDefaultLocation() (*time.Location, error) {
	return spannerDefaultLocation()
}

func formatSpannerTimestampString(t time.Time) string {
	formatted := t.Format("2006-01-02 15:04:05")
	if t.Nanosecond() != 0 {
		formatted += formatSpannerTimestampFraction(t.Nanosecond())
	}
	return formatted + t.Format("-07")
}

func formatSpannerTimestampFraction(ns int) string {
	switch {
	case ns%1e6 == 0:
		return fmt.Sprintf(".%03d", ns/1e6)
	case ns%1e3 == 0:
		return fmt.Sprintf(".%06d", ns/1e3)
	default:
		return fmt.Sprintf(".%09d", ns)
	}
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

func numericFromGCV(gcv spanner.GenericColumnValue) (*big.Rat, error) {
	v, err := stringFromGCV(gcv)
	if err != nil {
		return nil, err
	}
	if !isDecimalNumericString(v) {
		return nil, fmt.Errorf("invalid NUMERIC wire value: %q", v)
	}
	n, ok := new(big.Rat).SetString(v)
	if !ok {
		return nil, fmt.Errorf("invalid NUMERIC wire value: %q", v)
	}
	return n, nil
}

func isDecimalNumericString(v string) bool {
	if v == "" {
		return false
	}
	if v[0] == '+' || v[0] == '-' {
		v = v[1:]
	}
	var hasDigit, hasDot bool
	for _, r := range v {
		switch {
		case r >= '0' && r <= '9':
			hasDigit = true
		case r == '.' && !hasDot:
			hasDot = true
		default:
			return false
		}
	}
	return hasDigit
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
		return zeroGCV, fmt.Errorf("value out of FLOAT32 range: %v", v)
	}
	return gcvctor.Float32Value(f32), nil
}

func float64ToNumericValue(v float64, exprSQL string) (spanner.GenericColumnValue, error) {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return zeroGCV, fmt.Errorf("cannot cast non-finite floating-point value to NUMERIC: %v%s", v, exprContextSuffix(exprSQL))
	}
	n := new(big.Rat).SetFloat64(v)
	n, err := roundRatToNumeric(n, exprSQL)
	if err != nil {
		return zeroGCV, err
	}
	return gcvctor.NumericValueChecked(n)
}

func roundFloatToInt64(v float64, exprSQL string) (int64, error) {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return 0, fmt.Errorf("cannot cast non-finite floating-point value to INT64: %v%s", v, exprContextSuffix(exprSQL))
	}
	// Spanner CAST(FLOAT* AS INT64) rounds halfway cases away from zero.
	rounded := math.Round(v)
	if rounded < minInt64Float || rounded >= maxInt64FloatExclusive {
		return 0, fmt.Errorf("floating-point value out of INT64 range: %v%s", v, exprContextSuffix(exprSQL))
	}
	return int64(rounded), nil
}

func roundRatToInt64(v *big.Rat, exprSQL string) (int64, error) {
	rounded := roundRatHalfAwayFromZero(v)
	if !rounded.IsInt64() {
		return 0, fmt.Errorf("NUMERIC value out of INT64 range: %s%s", v.FloatString(spanner.NumericScaleDigits), exprContextSuffix(exprSQL))
	}
	return rounded.Int64(), nil
}

func roundRatToNumeric(v *big.Rat, exprSQL string) (*big.Rat, error) {
	scaled := new(big.Rat).Mul(v, new(big.Rat).SetInt(numericScaleFactor))
	rounded := roundRatHalfAwayFromZero(scaled)

	if new(big.Int).Abs(rounded).Cmp(maxScaledNumeric) > 0 {
		return nil, fmt.Errorf("NUMERIC value out of range: %s%s", v.FloatString(spanner.NumericScaleDigits), exprContextSuffix(exprSQL))
	}
	return new(big.Rat).SetFrac(rounded, numericScaleFactor), nil
}

func roundRatHalfAwayFromZero(v *big.Rat) *big.Int {
	abs := new(big.Rat).Abs(v)
	abs.Add(abs, big.NewRat(1, 2))
	rounded := new(big.Int).Quo(abs.Num(), abs.Denom())
	if v.Sign() < 0 {
		rounded.Neg(rounded)
	}
	return rounded
}

func pow10Int(exp int) *big.Int {
	return new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(exp)), nil)
}

func exprContextSuffix(exprSQL string) string {
	if exprSQL == "" {
		return ""
	}
	return ": " + exprSQL
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
		// Spanner documents that FLOAT-to-STRING results for 0 are not signed.
		return "0"
	default:
		return strconv.FormatFloat(v, 'g', -1, bitSize)
	}
}

func unsupportedCastError(srcCode, destCode sppb.TypeCode, exprSQL string) error {
	err := fmt.Errorf("%w from %v to %v", errUnsupportedCast, srcCode, destCode)
	if exprSQL != "" {
		return fmt.Errorf("%w: %s", err, exprSQL)
	}
	return err
}
