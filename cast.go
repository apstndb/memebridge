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
	// Spanner's temporal CAST documentation and live Cloud Spanner both use
	// America/Los_Angeles for DATE/TIMESTAMP/STRING casts without an explicit
	// time zone; this is distinct from TIMESTAMP's internal UTC storage.
	spannerDefaultTimeZone = "America/Los_Angeles"
)

var (
	errUnsupportedCast = errors.New("unsupported cast")

	numericScaleFactor = pow10Int(spanner.NumericScaleDigits)
	maxScaledNumeric   = new(big.Int).Sub(pow10Int(spanner.NumericPrecisionDigits), big.NewInt(1))

	spannerTimestampZonedLayouts = [...]string{
		time.RFC3339Nano,
		"2006-01-02T15:04:05.999999999Z07",
		"2006-01-02 15:04:05.999999999Z07:00",
		"2006-01-02 15:04:05.999999999Z07",
	}

	spannerTimestampLocalLayouts = [...]string{
		"2006-01-02",
		"2006-01-02T15:04:05.999999999",
		"2006-01-02 15:04:05.999999999",
	}

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
	if equivalentSpannerTypes(src.Type, destType) {
		return spanner.GenericColumnValue{Type: destType, Value: src.Value}, nil
	}
	if isNullGCV(src) {
		return gcvctor.NullOf(destType), nil
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
	case sppb.TypeCode_UUID:
		return castGCVToUUID(src, exprSQL)
	case sppb.TypeCode_INTERVAL:
		return castStringBasedGCV(src, destCode, exprSQL)
	case sppb.TypeCode_ARRAY:
		return castGCVToArray(src, destType, exprSQL)
	case sppb.TypeCode_STRUCT:
		return castGCVToStruct(src, destType, exprSQL)
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
		n, err := parseNumericLiteralForCast(v, exprSQL)
		if err != nil {
			return zeroGCV, err
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
	code := src.Type.GetCode()
	if code != sppb.TypeCode_STRING && code != sppb.TypeCode_UUID {
		return zeroGCV, unsupportedCastError(code, sppb.TypeCode_BYTES, exprSQL)
	}

	v, err := stringFromGCV(src)
	if err != nil {
		return zeroGCV, err
	}

	if code == sppb.TypeCode_STRING {
		return gcvctor.BytesValue([]byte(v)), nil
	}

	u, err := uuid.Parse(v)
	if err != nil {
		return zeroGCV, fmt.Errorf("invalid UUID value %q for cast of %s to BYTES: %w", v, exprSQL, err)
	}
	return gcvctor.BytesValue(u[:]), nil
}

func castGCVToDate(src spanner.GenericColumnValue, exprSQL string) (spanner.GenericColumnValue, error) {
	switch src.Type.GetCode() {
	case sppb.TypeCode_STRING:
		v, err := stringFromGCV(src)
		if err != nil {
			return zeroGCV, err
		}
		d, err := gcvctor.DateStringValue(v)
		if err != nil {
			return zeroGCV, fmt.Errorf("invalid DATE literal for cast of %s to DATE: %q: %w", exprSQL, v, err)
		}
		return d, nil
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

func castGCVToUUID(src spanner.GenericColumnValue, exprSQL string) (spanner.GenericColumnValue, error) {
	switch src.Type.GetCode() {
	case sppb.TypeCode_STRING:
		v, err := stringFromGCV(src)
		if err != nil {
			return zeroGCV, err
		}
		u, err := uuid.Parse(v)
		if err != nil || !strings.EqualFold(u.String(), v) {
			return zeroGCV, fmt.Errorf("invalid UUID literal for cast of %s to UUID: %q", exprSQL, v)
		}
		return gcvctor.UUIDValue(u), nil
	case sppb.TypeCode_BYTES:
		v, err := bytesFromGCV(src)
		if err != nil {
			return zeroGCV, err
		}
		if len(v) != 16 {
			return zeroGCV, fmt.Errorf("invalid BYTES length for cast of %s to UUID: expected 16, got %d", exprSQL, len(v))
		}
		u, err := uuid.FromBytes(v)
		if err != nil {
			return zeroGCV, fmt.Errorf("invalid BYTES value for cast of %s to UUID: %w", exprSQL, err)
		}
		return gcvctor.UUIDValue(u), nil
	default:
		return zeroGCV, unsupportedCastError(src.Type.GetCode(), sppb.TypeCode_UUID, exprSQL)
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
	switch destCode {
	case sppb.TypeCode_INTERVAL:
		return gcvctor.IntervalStringValue(v)
	default:
		return gcvctor.StringBasedValueFromCode(destCode, v), nil
	}
}

func castGCVToArray(src spanner.GenericColumnValue, destType *sppb.Type, exprSQL string) (spanner.GenericColumnValue, error) {
	if src.Type.GetCode() != sppb.TypeCode_ARRAY {
		return zeroGCV, unsupportedCastError(src.Type.GetCode(), sppb.TypeCode_ARRAY, exprSQL)
	}
	// Real Cloud Spanner does not support element-wise ARRAY casts such as
	// CAST([1] AS ARRAY<FLOAT64>) or SAFE_CAST(["x"] AS ARRAY<DATE>). The
	// GoogleSQL cast map marks ARRAY as IMPLICIT, but only for equivalent
	// non-simple types; callers must reject non-equivalent ARRAY casts.
	// See:
	// https://docs.cloud.google.com/spanner/docs/reference/standard-sql/conversion_rules
	// https://github.com/google/googlesql/blob/36dd14aa0657ea299725504bc0f938732f58f380/googlesql/public/cast.h#L45-L66
	// https://github.com/google/googlesql/blob/36dd14aa0657ea299725504bc0f938732f58f380/googlesql/public/cast.cc#L282-L289
	if !equivalentSpannerTypes(src.Type, destType) {
		return zeroGCV, unsupportedArrayCastError(src.Type, destType, exprSQL)
	}
	return spanner.GenericColumnValue{Type: destType, Value: src.Value}, nil
}

func gcvToValue(gcv spanner.GenericColumnValue) *structpb.Value {
	if gcv.Value == nil {
		return structpb.NewNullValue()
	}
	return gcv.Value
}

func castGCVToStruct(src spanner.GenericColumnValue, destType *sppb.Type, exprSQL string) (spanner.GenericColumnValue, error) {
	if src.Type.GetCode() != sppb.TypeCode_STRUCT {
		return zeroGCV, unsupportedCastError(src.Type.GetCode(), sppb.TypeCode_STRUCT, exprSQL)
	}
	srcStructType := src.Type.GetStructType()
	if srcStructType == nil {
		return zeroGCV, fmt.Errorf("malformed STRUCT source type%s", exprContextSuffix(exprSQL))
	}
	srcFields := srcStructType.GetFields()
	destStructType := destType.GetStructType()
	if destStructType == nil {
		return zeroGCV, fmt.Errorf("malformed STRUCT destination type%s", exprContextSuffix(exprSQL))
	}
	destFields := destStructType.GetFields()
	if len(srcFields) != len(destFields) {
		return zeroGCV, fmt.Errorf("cannot cast STRUCT with %d fields to STRUCT with %d fields%s", len(srcFields), len(destFields), exprContextSuffix(exprSQL))
	}
	// Cloud Spanner ignores field names during STRUCT CAST and only requires
	// the number of fields to match, so name parity is intentionally not enforced.
	listValue, ok := src.Value.GetKind().(*structpb.Value_ListValue)
	if !ok {
		return zeroGCV, fmt.Errorf("expected STRUCT wire value, got %T%s", src.Value.GetKind(), exprContextSuffix(exprSQL))
	}
	if listValue.ListValue == nil {
		return zeroGCV, fmt.Errorf("malformed STRUCT wire value: missing ListValue detail%s", exprContextSuffix(exprSQL))
	}
	values := listValue.ListValue.Values
	if len(values) != len(srcFields) {
		return zeroGCV, fmt.Errorf("STRUCT wire value has %d fields, but type has %d fields%s", len(values), len(srcFields), exprContextSuffix(exprSQL))
	}
	coerced := make([]*structpb.Value, len(values))
	for i, v := range values {
		elemGCV := spanner.GenericColumnValue{Type: srcFields[i].Type, Value: v}
		casted, err := castGCV(elemGCV, destFields[i].Type, exprSQL)
		if err != nil {
			return zeroGCV, fmt.Errorf("cannot cast struct field %d from %v to %v: %w", i, srcFields[i].Type.GetCode(), destFields[i].Type.GetCode(), err)
		}
		coerced[i] = gcvToValue(casted)
	}
	return spanner.GenericColumnValue{
		Type:  destType,
		Value: structpb.NewListValue(&structpb.ListValue{Values: coerced}),
	}, nil
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
	t, err := parseSpannerTimestampForCast(v)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid TIMESTAMP wire value for cast%s: %q: %w", exprContextSuffix(exprSQL), v, err)
	}
	return t, nil
}

func timestampStringValueForCast(v, exprSQL string) (spanner.GenericColumnValue, error) {
	t, err := parseSpannerTimestampForCast(v)
	if err != nil {
		return zeroGCV, fmt.Errorf("invalid TIMESTAMP literal for cast of %s to TIMESTAMP: %q: %w", exprSQL, v, err)
	}
	return gcvctor.TimestampValue(t.UTC()), nil
}

func parseSpannerTimestampForCast(v string) (time.Time, error) {
	if strings.HasSuffix(v, "z") && !hasNamedTimeZoneSuffix(v) {
		v = strings.TrimSuffix(v, "z") + "Z"
	}
	v = normalizeSpannerTimestampOffset(v)

	for _, layout := range spannerTimestampZonedLayouts {
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
	return i >= 0 && i < len(v)-1 && isASCIIAlpha(v[i+1])
}

func parseSpannerTimestampInLocation(v string, loc *time.Location) (time.Time, error) {
	for _, layout := range spannerTimestampLocalLayouts {
		t, err := time.ParseInLocation(layout, v, loc)
		if err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("unsupported timestamp format: %q", v)
}

func normalizeSpannerTimestampOffset(v string) string {
	if len(v) < len("-0:00") {
		return normalizeSpannerTimestampHourOffset(v)
	}
	i := len(v) - len("-0:00")
	if (v[i] != '+' && v[i] != '-') || v[i+2] != ':' {
		return normalizeSpannerTimestampHourOffset(v)
	}
	if !isASCIIDigit(v[i+1]) || !isASCIIDigit(v[i+3]) || !isASCIIDigit(v[i+4]) {
		return normalizeSpannerTimestampHourOffset(v)
	}
	return v[:i+1] + "0" + v[i+1:]
}

func normalizeSpannerTimestampHourOffset(v string) string {
	if len(v) < len("-0") {
		return v
	}
	i := len(v) - len("-0")
	if (v[i] != '+' && v[i] != '-') || !isASCIIDigit(v[i+1]) {
		return v
	}
	if i > 0 && !isASCIIDigit(v[i-1]) {
		return v
	}
	return v[:i+1] + "0" + v[i+1:]
}

func isASCIIAlpha(b byte) bool {
	return (b >= 'A' && b <= 'Z') || (b >= 'a' && b <= 'z')
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
	// TIMESTAMP-to-STRING is formatted in Spanner's default cast time zone, so
	// live Cloud Spanner emits -07/-08 rather than Z for UTC input instants.
	return formatted + formatSpannerTimestampOffset(t)
}

func formatSpannerTimestampOffset(t time.Time) string {
	_, offset := t.Zone()
	if offset%3600 == 0 {
		return t.Format("-07")
	}
	return t.Format("-07:00")
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

func parseNumericLiteralForCast(v, exprSQL string) (*big.Rat, error) {
	v = strings.TrimSpace(v)
	if strings.Contains(v, "/") {
		return nil, fmt.Errorf("invalid NUMERIC literal for cast of %s to NUMERIC: %q", exprSQL, v)
	}

	unsigned := v
	sign := 1
	if len(unsigned) > 0 {
		switch unsigned[0] {
		case '+':
			unsigned = unsigned[1:]
		case '-':
			sign = -1
			unsigned = unsigned[1:]
		}
	}
	if strings.HasPrefix(unsigned, "0x") || strings.HasPrefix(unsigned, "0X") {
		return nil, fmt.Errorf("invalid NUMERIC literal for cast of %s to NUMERIC: %q", exprSQL, v)
	}

	mantissa := unsigned
	expText := ""
	hasExponent := false
	if idx := strings.IndexAny(unsigned, "eE"); idx >= 0 {
		hasExponent = true
		if strings.ContainsAny(unsigned[idx+1:], "eE") {
			return nil, fmt.Errorf("invalid NUMERIC literal for cast of %s to NUMERIC: %q", exprSQL, v)
		}
		expText = unsigned[idx+1:]
		mantissa = unsigned[:idx]
	}

	digits := make([]rune, 0, len(mantissa))
	fracDigits := 0
	hasDot := false
	for _, r := range mantissa {
		switch {
		case r >= '0' && r <= '9':
			digits = append(digits, r)
			if hasDot {
				fracDigits++
			}
		case r == '.' && !hasDot:
			hasDot = true
		default:
			return nil, fmt.Errorf("invalid NUMERIC literal for cast of %s to NUMERIC: %q", exprSQL, v)
		}
	}
	if len(digits) == 0 {
		return nil, fmt.Errorf("invalid NUMERIC literal for cast of %s to NUMERIC: %q", exprSQL, v)
	}

	trimmedDigits := strings.TrimLeft(string(digits), "0")
	if trimmedDigits == "" {
		return new(big.Rat), nil
	}

	exp, err := parseNumericExponentForCast(expText, hasExponent, exprSQL, v)
	if err != nil {
		return nil, err
	}
	scaledInt, err := roundedScaledNumericInt(trimmedDigits, exp, int64(fracDigits), exprSQL, v)
	if err != nil {
		return nil, err
	}
	if sign < 0 {
		scaledInt.Neg(scaledInt)
	}
	return new(big.Rat).SetFrac(scaledInt, numericScaleFactor), nil
}

func parseNumericExponentForCast(expText string, hasExponent bool, exprSQL, original string) (int64, error) {
	if !hasExponent {
		return 0, nil
	}
	if expText == "" {
		return 0, fmt.Errorf("invalid NUMERIC literal for cast of %s to NUMERIC: %q", exprSQL, original)
	}
	exp, err := strconv.ParseInt(expText, 10, 64)
	if err != nil {
		var numErr *strconv.NumError
		if errors.As(err, &numErr) && errors.Is(numErr.Err, strconv.ErrRange) {
			return 0, fmt.Errorf("NUMERIC value out of range: %q%s", original, exprContextSuffix(exprSQL))
		}
		return 0, fmt.Errorf("invalid NUMERIC literal for cast of %s to NUMERIC: %q", exprSQL, original)
	}
	return exp, nil
}

func roundedScaledNumericInt(digits string, exp, fracDigits int64, exprSQL, original string) (*big.Int, error) {
	scale, ok := safeSubInt64(exp, fracDigits)
	if !ok {
		if exp < 0 {
			return new(big.Int), nil
		}
		return nil, fmt.Errorf("NUMERIC value out of range: %q%s", original, exprContextSuffix(exprSQL))
	}
	shift, ok := safeAddInt64(scale, int64(spanner.NumericScaleDigits))
	if !ok {
		if scale < 0 {
			return new(big.Int), nil
		}
		return nil, fmt.Errorf("NUMERIC value out of range: %q%s", original, exprContextSuffix(exprSQL))
	}
	digitsLen := int64(len(digits))
	if shift >= 0 {
		if digitsLen > int64(spanner.NumericPrecisionDigits)-shift {
			return nil, fmt.Errorf("NUMERIC value out of range: %q%s", original, exprContextSuffix(exprSQL))
		}
		scaled, ok := new(big.Int).SetString(digits, 10)
		if !ok {
			return nil, fmt.Errorf("invalid NUMERIC literal for cast of %s to NUMERIC: %q", exprSQL, original)
		}
		if shift > 0 {
			scaled.Mul(scaled, pow10Int(int(shift)))
		}
		return scaled, nil
	}

	denomDigits := -shift
	if digitsLen < denomDigits {
		return new(big.Int), nil
	}

	quotientDigits := "0"
	if digitsLen > denomDigits {
		quotientDigits = digits[:len(digits)-int(denomDigits)]
	}
	quotient, ok := new(big.Int).SetString(quotientDigits, 10)
	if !ok {
		return nil, fmt.Errorf("invalid NUMERIC literal for cast of %s to NUMERIC: %q", exprSQL, original)
	}

	remainderFirstDigit := digits[len(digits)-int(denomDigits)]
	if remainderFirstDigit >= '5' {
		quotient.Add(quotient, big.NewInt(1))
	}
	if quotient.Cmp(maxScaledNumeric) > 0 {
		return nil, fmt.Errorf("NUMERIC value out of range: %q%s", original, exprContextSuffix(exprSQL))
	}
	return quotient, nil
}

func safeAddInt64(a, b int64) (int64, bool) {
	if b > 0 && a > math.MaxInt64-b {
		return 0, false
	}
	if b < 0 && a < math.MinInt64-b {
		return 0, false
	}
	return a + b, true
}

func safeSubInt64(a, b int64) (int64, bool) {
	if b > 0 && a < math.MinInt64+b {
		return 0, false
	}
	if b < 0 && a > math.MaxInt64+b {
		return 0, false
	}
	return a - b, true
}

func equivalentSpannerTypes(a, b *sppb.Type) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	if a.GetCode() != b.GetCode() {
		return false
	}
	switch a.GetCode() {
	case sppb.TypeCode_ARRAY:
		return equivalentSpannerTypes(a.GetArrayElementType(), b.GetArrayElementType())
	case sppb.TypeCode_STRUCT:
		aFields := a.GetStructType().GetFields()
		bFields := b.GetStructType().GetFields()
		if len(aFields) != len(bFields) {
			return false
		}
		for i := range aFields {
			if !equivalentSpannerTypes(aFields[i].GetType(), bFields[i].GetType()) {
				return false
			}
		}
		return true
	default:
		return proto.Equal(a, b)
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

func unsupportedArrayCastError(srcType, destType *sppb.Type, exprSQL string) error {
	srcElemType := srcType.GetArrayElementType()
	destElemType := destType.GetArrayElementType()
	srcElemCode := sppb.TypeCode_TYPE_CODE_UNSPECIFIED
	if srcElemType != nil {
		srcElemCode = srcElemType.GetCode()
	}
	destElemCode := sppb.TypeCode_TYPE_CODE_UNSPECIFIED
	if destElemType != nil {
		destElemCode = destElemType.GetCode()
	}
	err := fmt.Errorf("%w from ARRAY<%v> to ARRAY<%v>", errUnsupportedCast, srcElemCode, destElemCode)
	if exprSQL != "" {
		return fmt.Errorf("%w: %s", err, exprSQL)
	}
	return err
}
