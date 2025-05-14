package memebridge_test

import (
	"math"
	"math/big"
	"testing"
	"time"

	"cloud.google.com/go/civil"
	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/apstndb/spantype/typector"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"google.golang.org/protobuf/testing/protocmp"

	"github.com/apstndb/memebridge"

	"github.com/apstndb/spanvalue/gcvctor"
)

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

func mustOk[T any](v T, ok bool) T {
	if !ok {
		panic(ok)
	}
	return v
}

func TestParseExpr(t *testing.T) {
	tests := []struct {
		input string
		want  spanner.GenericColumnValue
	}{
		{`NULL`, gcvctor.SimpleTypedNull(sppb.TypeCode_INT64)},
		{`TRUE`, gcvctor.BoolValue(true)},
		{`FALSE`, gcvctor.BoolValue(false)},
		{"1", gcvctor.Int64Value(1)},
		{`3.14`, gcvctor.Float64Value(3.14)},
		{`"foo"`, gcvctor.StringValue("foo")},
		{`b"foo"`, gcvctor.BytesValue([]byte("foo"))},
		{`DATE "1970-01-01"`, gcvctor.DateValue(civil.Date{Year: 1970, Month: time.January, Day: 1})},
		{`TIMESTAMP "1970-01-01T00:00:00Z"`, gcvctor.TimestampValue(time.Date(1970, time.January, 1, 0, 0, 0, 0, time.UTC))},
		// NUMERIC is tested in TestParseExpr_Numeric

		// Note: Usually, JSON representation is not stable.
		{`JSON '{"foo":"bar"}'`, must(gcvctor.JSONValue(map[string]string{"foo": "bar"}))},
		{`[1, 2, 3]`, must(gcvctor.ArrayValue(gcvctor.Int64Value(1), gcvctor.Int64Value(2), gcvctor.Int64Value(3)))},
		{
			`(1, "foo", 3.14)`,
			must(gcvctor.StructValue(
				[]string{"", "", ""},
				[]spanner.GenericColumnValue{gcvctor.Int64Value(1), gcvctor.StringValue("foo"), gcvctor.Float64Value(3.14)},
			)),
		},
		{
			`STRUCT(1 AS int64_value, "foo" AS string_value, 3.14 AS float64_value)`,
			must(gcvctor.StructValue(
				[]string{"int64_value", "string_value", "float64_value"},
				[]spanner.GenericColumnValue{gcvctor.Int64Value(1), gcvctor.StringValue("foo"), gcvctor.Float64Value(3.14)},
			)),
		},
		{
			`STRUCT<int64_value INT64, string_value STRING, float64_value FLOAT64>(1, "foo", 3.14)`,
			must(gcvctor.StructValue(
				[]string{"int64_value", "string_value", "float64_value"},
				[]spanner.GenericColumnValue{gcvctor.Int64Value(1), gcvctor.StringValue("foo"), gcvctor.Float64Value(3.14)},
			)),
		},
		{"(1)", gcvctor.Int64Value(1)},
		{`CAST("NaN" AS FLOAT64)`, gcvctor.Float64Value(math.NaN())},
		{`CAST("Infinity" AS FLOAT64)`, gcvctor.Float64Value(math.Inf(1))},
		{`CAST("-Infinity" AS FLOAT64)`, gcvctor.Float64Value(math.Inf(-1))},
		{`CAST(1.0 AS FLOAT32)`, gcvctor.Float32Value(1.0)},
		{`CAST("NaN" AS FLOAT32)`, gcvctor.Float32Value(float32(math.NaN()))},
		{`CAST("Infinity" AS FLOAT32)`, gcvctor.Float32Value(float32(math.Inf(1)))},
		{`CAST("-Infinity" AS FLOAT32)`, gcvctor.Float32Value(float32(math.Inf(-1)))},
		{`CAST("94a01a73-d90a-432d-a03f-5db58ea8058f" AS UUID)`, gcvctor.StringBasedValue(sppb.TypeCode_UUID, `94a01a73-d90a-432d-a03f-5db58ea8058f`)},

		{"PENDING_COMMIT_TIMESTAMP()", gcvctor.StringBasedValue(sppb.TypeCode_TIMESTAMP, "spanner.commit_timestamp()")},

		{`CAST("P1Y2M3DT4H5M6.5S" AS INTERVAL)`, gcvctor.StringBasedValue(sppb.TypeCode_INTERVAL, `P1Y2M3DT4H5M6.5S`)},
		{"CAST(NULL AS INTERVAL)", gcvctor.SimpleTypedNull(sppb.TypeCode_INTERVAL)},

		{"INTERVAL 3 YEAR", gcvctor.StringBasedValue(sppb.TypeCode_INTERVAL, "P3Y")},
		{"INTERVAL 3 QUARTER", gcvctor.StringBasedValue(sppb.TypeCode_INTERVAL, "P9M")},
		{"INTERVAL 3 MONTH", gcvctor.StringBasedValue(sppb.TypeCode_INTERVAL, "P3M")},
		{"INTERVAL 3 WEEK", gcvctor.StringBasedValue(sppb.TypeCode_INTERVAL, "P21D")},
		{"INTERVAL 3 DAY", gcvctor.StringBasedValue(sppb.TypeCode_INTERVAL, "P3D")},
		{"INTERVAL 3 HOUR", gcvctor.StringBasedValue(sppb.TypeCode_INTERVAL, "PT3H")},
		{"INTERVAL 3 MINUTE", gcvctor.StringBasedValue(sppb.TypeCode_INTERVAL, "PT3M")},
		{"INTERVAL 3 SECOND", gcvctor.StringBasedValue(sppb.TypeCode_INTERVAL, "PT3S")},
		{"INTERVAL 3 MILLISECOND", gcvctor.StringBasedValue(sppb.TypeCode_INTERVAL, "PT0.003S")},
		{"INTERVAL 3 MICROSECOND", gcvctor.StringBasedValue(sppb.TypeCode_INTERVAL, "PT0.000003S")},
		{"INTERVAL 3 NANOSECOND", gcvctor.StringBasedValue(sppb.TypeCode_INTERVAL, "PT0.000000003S")},

		{`INTERVAL '2-11' YEAR TO MONTH`, gcvctor.StringBasedValue(sppb.TypeCode_INTERVAL, "P2Y11M")},
		{`INTERVAL '2-11 28' YEAR TO DAY`, gcvctor.StringBasedValue(sppb.TypeCode_INTERVAL, "P2Y11M28D")},
		{`INTERVAL '2-11 28 16' YEAR TO HOUR`, gcvctor.StringBasedValue(sppb.TypeCode_INTERVAL, "P2Y11M28DT16H")},
		{`INTERVAL '2-11 28 16:15' YEAR TO MINUTE`, gcvctor.StringBasedValue(sppb.TypeCode_INTERVAL, "P2Y11M28DT16H15M")},
		{`INTERVAL '2-11 28 16:15:14' YEAR TO SECOND`, gcvctor.StringBasedValue(sppb.TypeCode_INTERVAL, "P2Y11M28DT16H15M14S")},
		{`INTERVAL '11 28' MONTH TO DAY`, gcvctor.StringBasedValue(sppb.TypeCode_INTERVAL, "P11M28D")},
		{`INTERVAL '11 28 16' MONTH TO HOUR`, gcvctor.StringBasedValue(sppb.TypeCode_INTERVAL, "P11M28DT16H")},
		{`INTERVAL '11 28 16:15' MONTH TO MINUTE`, gcvctor.StringBasedValue(sppb.TypeCode_INTERVAL, "P11M28DT16H15M")},
		{`INTERVAL '11 28 16:15:14' MONTH TO SECOND`, gcvctor.StringBasedValue(sppb.TypeCode_INTERVAL, "P11M28DT16H15M14S")},
		{`INTERVAL '28 16' DAY TO HOUR`, gcvctor.StringBasedValue(sppb.TypeCode_INTERVAL, "P28DT16H")},
		{`INTERVAL '28 16:15' DAY TO MINUTE`, gcvctor.StringBasedValue(sppb.TypeCode_INTERVAL, "P28DT16H15M")},
		{`INTERVAL '28 16:15:14' DAY TO SECOND`, gcvctor.StringBasedValue(sppb.TypeCode_INTERVAL, "P28DT16H15M14S")},
		{`INTERVAL '16:15' HOUR TO MINUTE`, gcvctor.StringBasedValue(sppb.TypeCode_INTERVAL, "PT16H15M")},
		{`INTERVAL '16:15:14' HOUR TO SECOND`, gcvctor.StringBasedValue(sppb.TypeCode_INTERVAL, "PT16H15M14S")},
		{`INTERVAL '15:14' MINUTE TO SECOND`, gcvctor.StringBasedValue(sppb.TypeCode_INTERVAL, "PT15M14S")},
		{`INTERVAL '10:20:30.52' HOUR TO SECOND`, gcvctor.StringBasedValue(sppb.TypeCode_INTERVAL, "PT10H20M30.520S")},
		{`INTERVAL '20:30.123456789' MINUTE TO SECOND`, gcvctor.StringBasedValue(sppb.TypeCode_INTERVAL, "PT20M30.123456789S")},

		// Casted NULLs
		{"CAST(NULL AS INT64)", gcvctor.SimpleTypedNull(sppb.TypeCode_INT64)},
		{"CAST(NULL AS FLOAT64)", gcvctor.SimpleTypedNull(sppb.TypeCode_FLOAT64)},
		{"CAST(NULL AS UUID)", gcvctor.SimpleTypedNull(sppb.TypeCode_UUID)},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := memebridge.ParseExpr("", tt.input)
			if err != nil {
				t.Errorf("should not fail, but err: %v", err)
			}

			if diff := cmp.Diff(tt.want, got, protocmp.Transform(), cmpopts.EquateNaNs()); diff != "" {
				t.Errorf("mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestParseExpr_Numeric(t *testing.T) {
	tests := []struct {
		input string
		want  *big.Rat
	}{
		{`NUMERIC "3.14"`, big.NewRat(314, 100)},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := memebridge.ParseExpr("", tt.input)
			if err != nil {
				t.Errorf("should not fail, but err: %v", err)
			}

			if diff := cmp.Diff(typector.CodeToSimpleType(sppb.TypeCode_NUMERIC), got.Type, protocmp.Transform()); diff != "" {
				t.Errorf("type mismatch (-want +got):\n%s", diff)
			}

			gotRat := mustOk((&big.Rat{}).SetString(got.Value.GetStringValue()))
			if tt.want.Cmp(gotRat) != 0 {
				t.Errorf("mismatch want: %v, got: %v", tt.want, gotRat)
			}
		})
	}
}
