package memebridge_test

import (
	"math"
	"math/big"
	"strings"
	"testing"
	"time"
	_ "time/tzdata" // ensure IANA tzdata is available for tests on minimal runtimes

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
		{`NULL`, gcvctor.NullOf(typector.Int64())},
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
		{`ARRAY<INT64>[1]`, must(gcvctor.ArrayValueOf(typector.Int64(), gcvctor.Int64Value(1)))},
		{`[NULL, NULL]`, must(gcvctor.ArrayValueOf(typector.Int64(), gcvctor.NullOf(typector.Int64()), gcvctor.NullOf(typector.Int64())))},
		{`[CAST(NULL AS STRING)]`, must(gcvctor.ArrayValueOf(typector.String(), gcvctor.NullOf(typector.String())))},
		{`["foo", NULL]`, must(gcvctor.ArrayValueOf(typector.String(), gcvctor.StringValue("foo"), gcvctor.NullOf(typector.String())))},
		{`[1, 2.5]`, must(gcvctor.ArrayValueOf(typector.Float64(), gcvctor.Float64Value(1), gcvctor.Float64Value(2.5)))},
		{
			`[1, NUMERIC "2.5"]`,
			must(gcvctor.ArrayValueOf(
				typector.Numeric(),
				gcvctor.NumericValue(big.NewRat(1, 1)),
				gcvctor.StringBasedValueFromCode(sppb.TypeCode_NUMERIC, "2.5"),
			)),
		},
		{`[NUMERIC "1.5", 2.5]`, must(gcvctor.ArrayValueOf(typector.Float64(), gcvctor.Float64Value(1.5), gcvctor.Float64Value(2.5)))},
		{`[CAST("1.5" AS FLOAT32), 2]`, must(gcvctor.ArrayValueOf(typector.Float64(), gcvctor.Float64Value(1.5), gcvctor.Float64Value(2)))},
		{
			`ARRAY<NUMERIC>[1, NULL]`,
			must(gcvctor.ArrayValueOf(
				typector.Numeric(),
				gcvctor.NumericValue(big.NewRat(1, 1)),
				gcvctor.NullOf(typector.Numeric()),
			)),
		},
		{`ARRAY<FLOAT64>[1, NUMERIC "2.5"]`, must(gcvctor.ArrayValueOf(typector.Float64(), gcvctor.Float64Value(1), gcvctor.Float64Value(2.5)))},
		{`ARRAY<FLOAT32>[1, 2.5]`, must(gcvctor.ArrayValueOf(typector.Float32(), gcvctor.Float32Value(1), gcvctor.Float32Value(2.5)))},
		{
			`(1, "foo", 3.14)`,
			must(gcvctor.StructValueOf(
				[]string{"", "", ""},
				[]spanner.GenericColumnValue{gcvctor.Int64Value(1), gcvctor.StringValue("foo"), gcvctor.Float64Value(3.14)},
			)),
		},
		{
			`STRUCT(1 AS int64_value, "foo" AS string_value, 3.14 AS float64_value)`,
			must(gcvctor.StructValueOf(
				[]string{"int64_value", "string_value", "float64_value"},
				[]spanner.GenericColumnValue{gcvctor.Int64Value(1), gcvctor.StringValue("foo"), gcvctor.Float64Value(3.14)},
			)),
		},
		{
			`STRUCT<int64_value INT64, string_value STRING, float64_value FLOAT64>(1, "foo", 3.14)`,
			must(gcvctor.StructValueOf(
				[]string{"int64_value", "string_value", "float64_value"},
				[]spanner.GenericColumnValue{gcvctor.Int64Value(1), gcvctor.StringValue("foo"), gcvctor.Float64Value(3.14)},
			)),
		},
		{
			`STRUCT<n NUMERIC, f FLOAT64, f32 FLOAT32>(1, NUMERIC "2.5", 3.5)`,
			must(gcvctor.StructValueOf(
				[]string{"n", "f", "f32"},
				[]spanner.GenericColumnValue{
					gcvctor.NumericValue(big.NewRat(1, 1)),
					gcvctor.Float64Value(2.5),
					gcvctor.Float32Value(3.5),
				},
			)),
		},
		{
			`STRUCT<d DATE, ts TIMESTAMP, u UUID, i INTERVAL>("1970-01-01", "1970-01-01T00:00:00Z", "94a01a73-d90a-432d-a03f-5db58ea8058f", "P1Y")`,
			must(gcvctor.StructValueOf(
				[]string{"d", "ts", "u", "i"},
				[]spanner.GenericColumnValue{
					gcvctor.DateValue(civil.Date{Year: 1970, Month: time.January, Day: 1}),
					gcvctor.TimestampValue(time.Date(1970, time.January, 1, 0, 0, 0, 0, time.UTC)),
					gcvctor.StringBasedValueFromCode(sppb.TypeCode_UUID, "94a01a73-d90a-432d-a03f-5db58ea8058f"),
					gcvctor.StringBasedValueFromCode(sppb.TypeCode_INTERVAL, "P1Y"),
				},
			)),
		},
		{
			`STRUCT<s STRING>(NULL)`,
			must(gcvctor.StructValueOf(
				[]string{"s"},
				[]spanner.GenericColumnValue{gcvctor.NullOf(typector.String())},
			)),
		},
		{
			`STRUCT<n NUMERIC>(CAST(NULL AS INT64))`,
			must(gcvctor.StructValueOf(
				[]string{"n"},
				[]spanner.GenericColumnValue{gcvctor.NullOf(typector.Numeric())},
			)),
		},
		{
			`STRUCT<a ARRAY<NUMERIC>>([1, NULL])`,
			must(gcvctor.StructValueOf(
				[]string{"a"},
				[]spanner.GenericColumnValue{
					must(gcvctor.ArrayValueOf(
						typector.Numeric(),
						gcvctor.NumericValue(big.NewRat(1, 1)),
						gcvctor.NullOf(typector.Numeric()),
					)),
				},
			)),
		},
		{
			`STRUCT<a ARRAY<FLOAT64>>([1, NUMERIC "2.5"])`,
			must(gcvctor.StructValueOf(
				[]string{"a"},
				[]spanner.GenericColumnValue{
					must(gcvctor.ArrayValueOf(
						typector.Float64(),
						gcvctor.Float64Value(1),
						gcvctor.Float64Value(2.5),
					)),
				},
			)),
		},
		{
			`STRUCT<a ARRAY<DATE>>(["1970-01-01"])`,
			must(gcvctor.StructValueOf(
				[]string{"a"},
				[]spanner.GenericColumnValue{
					must(gcvctor.ArrayValueOf(
						typector.Date(),
						gcvctor.DateValue(civil.Date{Year: 1970, Month: time.January, Day: 1}),
					)),
				},
			)),
		},
		{"(1)", gcvctor.Int64Value(1)},
		{`CAST(TRUE AS INT64)`, gcvctor.Int64Value(1)},
		{`CAST(FALSE AS STRING)`, gcvctor.StringValue("false")},
		{`CAST(0 AS BOOL)`, gcvctor.BoolValue(false)},
		{`CAST(2 AS BOOL)`, gcvctor.BoolValue(true)},
		{`CAST(42 AS FLOAT32)`, gcvctor.Float32Value(42)},
		{`CAST(42 AS FLOAT64)`, gcvctor.Float64Value(42)},
		{`CAST(42 AS NUMERIC)`, gcvctor.NumericValue(big.NewRat(42, 1))},
		{`CAST(NUMERIC "3.5" AS INT64)`, gcvctor.Int64Value(4)},
		{`CAST(NUMERIC ".5" AS INT64)`, gcvctor.Int64Value(1)},
		{`CAST(NUMERIC "-0.5" AS INT64)`, gcvctor.Int64Value(-1)},
		{`CAST(NUMERIC "3.25" AS FLOAT32)`, gcvctor.Float32Value(3.25)},
		{`CAST(NUMERIC "3.25" AS FLOAT64)`, gcvctor.Float64Value(3.25)},
		{`CAST(42 AS STRING)`, gcvctor.StringValue("42")},
		{`CAST(NUMERIC "3.140000000" AS STRING)`, gcvctor.StringValue("3.14")},
		{`CAST("TrUe" AS BOOL)`, gcvctor.BoolValue(true)},
		{`CAST("123" AS INT64)`, gcvctor.Int64Value(123)},
		{`CAST(" 123 " AS INT64)`, gcvctor.Int64Value(123)},
		{`CAST("0x123" AS INT64)`, gcvctor.Int64Value(291)},
		{`CAST("-0x123" AS INT64)`, gcvctor.Int64Value(-291)},
		{`CAST(1.5 AS INT64)`, gcvctor.Int64Value(2)},
		{`CAST(-0.5 AS INT64)`, gcvctor.Int64Value(-1)},
		{`CAST(3.5 AS STRING)`, gcvctor.StringValue("3.5")},
		{`CAST(CAST("Infinity" AS FLOAT64) AS STRING)`, gcvctor.StringValue("Infinity")},
		{`CAST("3.5" AS FLOAT32)`, gcvctor.Float32Value(3.5)},
		{`CAST("nan" AS FLOAT32)`, gcvctor.Float32Value(float32(math.NaN()))},
		{`CAST(CAST("3.5" AS FLOAT32) AS STRING)`, gcvctor.StringValue("3.5")},
		{`CAST("3.5" AS FLOAT64)`, gcvctor.Float64Value(3.5)},
		{`CAST(" Infinity " AS FLOAT64)`, gcvctor.Float64Value(math.Inf(1))},
		{`CAST("3.14" AS NUMERIC)`, gcvctor.NumericValue(big.NewRat(314, 100))},
		{`CAST(" 3.14 " AS NUMERIC)`, gcvctor.NumericValue(big.NewRat(314, 100))},
		{`CAST(1.125 AS NUMERIC)`, gcvctor.NumericValue(big.NewRat(1125, 1000))},
		{`CAST(1.1234567895 AS NUMERIC)`, gcvctor.NumericValue(big.NewRat(1123456790, 1000000000))},
		{`CAST(CAST("1.1234567895" AS FLOAT32) AS NUMERIC)`, gcvctor.NumericValue(big.NewRat(1123456836, 1000000000))},
		{`CAST(-1.1234567895 AS NUMERIC)`, gcvctor.NumericValue(big.NewRat(-1123456790, 1000000000))},
		{`CAST("inf" AS FLOAT64)`, gcvctor.Float64Value(math.Inf(1))},
		{`CAST("foo" AS BYTES)`, gcvctor.BytesValue([]byte("foo"))},
		{`CAST(b"foo" AS STRING)`, gcvctor.StringValue("foo")},
		{`CAST("1970-01-01" AS DATE)`, gcvctor.DateValue(civil.Date{Year: 1970, Month: time.January, Day: 1})},
		{`CAST(DATE "1970-01-01" AS STRING)`, gcvctor.StringValue("1970-01-01")},
		{`CAST(TIMESTAMP "1970-01-01T07:59:59Z" AS DATE)`, gcvctor.DateValue(civil.Date{Year: 1969, Month: time.December, Day: 31})},
		{`CAST(TIMESTAMP "1970-01-01T08:00:00Z" AS DATE)`, gcvctor.DateValue(civil.Date{Year: 1970, Month: time.January, Day: 1})},
		{`CAST(TIMESTAMP "2020-06-02T06:59:59Z" AS DATE)`, gcvctor.DateValue(civil.Date{Year: 2020, Month: time.June, Day: 1})},
		{`CAST(TIMESTAMP "2020-06-02T07:00:00Z" AS DATE)`, gcvctor.DateValue(civil.Date{Year: 2020, Month: time.June, Day: 2})},
		{`CAST(DATE "1970-01-01" AS TIMESTAMP)`, gcvctor.TimestampValue(time.Date(1970, time.January, 1, 8, 0, 0, 0, time.UTC))},
		{`CAST(DATE "2020-06-02" AS TIMESTAMP)`, gcvctor.TimestampValue(time.Date(2020, time.June, 2, 7, 0, 0, 0, time.UTC))},
		{`CAST("1970-01-01T00:00:00Z" AS TIMESTAMP)`, gcvctor.TimestampValue(time.Date(1970, time.January, 1, 0, 0, 0, 0, time.UTC))},
		{`CAST("2020-06-02" AS TIMESTAMP)`, gcvctor.TimestampValue(time.Date(2020, time.June, 2, 7, 0, 0, 0, time.UTC))},
		{`CAST("2020-06-02 00:00:00" AS TIMESTAMP)`, gcvctor.TimestampValue(time.Date(2020, time.June, 2, 7, 0, 0, 0, time.UTC))},
		{`CAST("2020-06-02T00:00:00" AS TIMESTAMP)`, gcvctor.TimestampValue(time.Date(2020, time.June, 2, 7, 0, 0, 0, time.UTC))},
		{`CAST("2020-06-02T00:00:00.123" AS TIMESTAMP)`, gcvctor.TimestampValue(time.Date(2020, time.June, 2, 7, 0, 0, 123000000, time.UTC))},
		{`CAST("2020-06-02 00:00:00.123456789" AS TIMESTAMP)`, gcvctor.TimestampValue(time.Date(2020, time.June, 2, 7, 0, 0, 123456789, time.UTC))},
		{`CAST("2020-06-02T00:00:00+05:30" AS TIMESTAMP)`, gcvctor.TimestampValue(time.Date(2020, time.June, 1, 18, 30, 0, 0, time.UTC))},
		{`CAST("2020-06-02T00:00:00-07" AS TIMESTAMP)`, gcvctor.TimestampValue(time.Date(2020, time.June, 2, 7, 0, 0, 0, time.UTC))},
		{`CAST("2020-06-02 00:00:00-07" AS TIMESTAMP)`, gcvctor.TimestampValue(time.Date(2020, time.June, 2, 7, 0, 0, 0, time.UTC))},
		{`CAST("2020-06-02 00:00:00-7:00" AS TIMESTAMP)`, gcvctor.TimestampValue(time.Date(2020, time.June, 2, 7, 0, 0, 0, time.UTC))},
		{`CAST("2020-06-02 00:00:00-7" AS TIMESTAMP)`, gcvctor.TimestampValue(time.Date(2020, time.June, 2, 7, 0, 0, 0, time.UTC))},
		{`CAST("2014-09-27 12:30:00.45z" AS TIMESTAMP)`, gcvctor.TimestampValue(time.Date(2014, time.September, 27, 12, 30, 0, 450000000, time.UTC))},
		{`CAST("2020-06-02 00:00:00 UTC" AS TIMESTAMP)`, gcvctor.TimestampValue(time.Date(2020, time.June, 2, 0, 0, 0, 0, time.UTC))},
		{`CAST("2008-12-25 15:30:00 America/Los_Angeles" AS TIMESTAMP)`, gcvctor.TimestampValue(time.Date(2008, time.December, 25, 23, 30, 0, 0, time.UTC))},
		{`CAST("2020-06-02 00:00:00.123 America/Los_Angeles" AS TIMESTAMP)`, gcvctor.TimestampValue(time.Date(2020, time.June, 2, 7, 0, 0, 123000000, time.UTC))},
		{`CAST("2020-06-02 00:00:00 Asia/Tokyo" AS TIMESTAMP)`, gcvctor.TimestampValue(time.Date(2020, time.June, 1, 15, 0, 0, 0, time.UTC))},
		{`CAST("2020-06-02 00:00:00 Europe/Vaduz" AS TIMESTAMP)`, gcvctor.TimestampValue(time.Date(2020, time.June, 1, 22, 0, 0, 0, time.UTC))},
		{`CAST(TIMESTAMP "1970-01-01T00:00:00Z" AS STRING)`, gcvctor.StringValue("1969-12-31 16:00:00-08")},
		{`CAST(TIMESTAMP "2020-06-02T00:00:00Z" AS STRING)`, gcvctor.StringValue("2020-06-01 17:00:00-07")},
		{`CAST(TIMESTAMP "2020-06-02T00:00:00.120Z" AS STRING)`, gcvctor.StringValue("2020-06-01 17:00:00.120-07")},
		{`CAST(TIMESTAMP "2020-06-02T00:00:00.123456Z" AS STRING)`, gcvctor.StringValue("2020-06-01 17:00:00.123456-07")},
		{`CAST(TIMESTAMP "2020-06-02T00:00:00.123456789Z" AS STRING)`, gcvctor.StringValue("2020-06-01 17:00:00.123456789-07")},
		{`CAST(TIMESTAMP "2014-09-27 12:30:00.45z" AS STRING)`, gcvctor.StringValue("2014-09-27 05:30:00.450-07")},
		{`CAST(TIMESTAMP "2008-12-25 15:30:00 America/Los_Angeles" AS STRING)`, gcvctor.StringValue("2008-12-25 15:30:00-08")},
		{`CAST(TIMESTAMP "2020-06-02 00:00:00-7:00" AS STRING)`, gcvctor.StringValue("2020-06-02 00:00:00-07")},
		{`CAST(PENDING_COMMIT_TIMESTAMP() AS STRING)`, gcvctor.StringValue("spanner.commit_timestamp()")},
		{`CAST(TIMESTAMP "2014-09-27 12:30:00.45z" AS DATE)`, gcvctor.DateValue(civil.Date{Year: 2014, Month: time.September, Day: 27})},
		{`CAST(TIMESTAMP "2008-12-25 15:30:00 America/Los_Angeles" AS DATE)`, gcvctor.DateValue(civil.Date{Year: 2008, Month: time.December, Day: 25})},
		{`CAST(CAST("94a01a73-d90a-432d-a03f-5db58ea8058f" AS UUID) AS STRING)`, gcvctor.StringValue("94a01a73-d90a-432d-a03f-5db58ea8058f")},
		{`CAST(b"\x00\x00\x00\x00\x00\x00\x40\x00\x80\x00\x00\x00\x00\x00\x00\x00" AS UUID)`, gcvctor.StringBasedValueFromCode(sppb.TypeCode_UUID, `00000000-0000-4000-8000-000000000000`)},
		{`CAST(CAST("00000000-0000-4000-8000-000000000000" AS UUID) AS BYTES)`, gcvctor.BytesValue([]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, 0x00, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00})},
		{`CAST(CAST("P1Y" AS INTERVAL) AS STRING)`, gcvctor.StringValue("P1Y")},
		{`CAST([1] AS ARRAY<INT64>)`, must(gcvctor.ArrayValueOf(typector.Int64(), gcvctor.Int64Value(1)))},
		{`CAST([1] AS ARRAY<FLOAT64>)`, must(gcvctor.ArrayValueOf(typector.Float64(), gcvctor.Float64Value(1.0)))},
		{`CAST([NUMERIC "1.5"] AS ARRAY<FLOAT64>)`, must(gcvctor.ArrayValueOf(typector.Float64(), gcvctor.Float64Value(1.5)))},
		{`CAST([1, NULL] AS ARRAY<FLOAT64>)`, must(gcvctor.ArrayValueOf(typector.Float64(), gcvctor.Float64Value(1.0), gcvctor.NullOf(typector.Float64())))},
		{
			`CAST(STRUCT(1 AS foo) AS STRUCT<foo INT64>)`,
			must(gcvctor.StructValueOf(
				[]string{"foo"},
				[]spanner.GenericColumnValue{gcvctor.Int64Value(1)},
			)),
		},
		{
			`CAST(STRUCT(1 AS foo) AS STRUCT<foo FLOAT64>)`,
			must(gcvctor.StructValueOf(
				[]string{"foo"},
				[]spanner.GenericColumnValue{gcvctor.Float64Value(1.0)},
			)),
		},
		{
			`CAST(STRUCT(1 AS foo, "2" AS bar) AS STRUCT<foo INT64, bar INT64>)`,
			must(gcvctor.StructValueOf(
				[]string{"foo", "bar"},
				[]spanner.GenericColumnValue{gcvctor.Int64Value(1), gcvctor.Int64Value(2)},
			)),
		},
		{
			`CAST(STRUCT(NULL AS foo) AS STRUCT<foo INT64>)`,
			must(gcvctor.StructValueOf(
				[]string{"foo"},
				[]spanner.GenericColumnValue{gcvctor.NullOf(typector.Int64())},
			)),
		},
		{
			`CAST(STRUCT(1 AS foo) AS STRUCT<bar INT64>)`,
			must(gcvctor.StructValueOf(
				[]string{"bar"},
				[]spanner.GenericColumnValue{gcvctor.Int64Value(1)},
			)),
		},
		{`CAST(CAST(42 AS STRING) AS INT64)`, gcvctor.Int64Value(42)},
		{`CAST("NaN" AS FLOAT64)`, gcvctor.Float64Value(math.NaN())},
		{`CAST("Infinity" AS FLOAT64)`, gcvctor.Float64Value(math.Inf(1))},
		{`CAST("-Infinity" AS FLOAT64)`, gcvctor.Float64Value(math.Inf(-1))},
		{`CAST(1.0 AS FLOAT32)`, gcvctor.Float32Value(1.0)},
		{`CAST("NaN" AS FLOAT32)`, gcvctor.Float32Value(float32(math.NaN()))},
		{`CAST("Infinity" AS FLOAT32)`, gcvctor.Float32Value(float32(math.Inf(1)))},
		{`CAST("-Infinity" AS FLOAT32)`, gcvctor.Float32Value(float32(math.Inf(-1)))},
		{`CAST("94a01a73-d90a-432d-a03f-5db58ea8058f" AS UUID)`, gcvctor.StringBasedValueFromCode(sppb.TypeCode_UUID, `94a01a73-d90a-432d-a03f-5db58ea8058f`)},
		{`SAFE_CAST("42" AS INT64)`, gcvctor.Int64Value(42)},
		{`SAFE_CAST("12x" AS INT64)`, gcvctor.NullOf(typector.Int64())},
		{`SAFE_CAST("maybe" AS BOOL)`, gcvctor.NullOf(typector.Bool())},
		{`SAFE_CAST("not-a-number" AS NUMERIC)`, gcvctor.NullOf(typector.Numeric())},
		{`SAFE_CAST(1e50 AS FLOAT32)`, gcvctor.NullOf(typector.Float32())},
		{`SAFE_CAST(CAST("NaN" AS FLOAT64) AS INT64)`, gcvctor.NullOf(typector.Int64())},
		{`SAFE_CAST(b"\xff" AS STRING)`, gcvctor.NullOf(typector.String())},
		{`SAFE_CAST("not-a-date" AS DATE)`, gcvctor.NullOf(typector.Date())},
		{`SAFE_CAST("not-a-timestamp" AS TIMESTAMP)`, gcvctor.NullOf(typector.Timestamp())},
		{`SAFE_CAST("2020-06-02 00:00:00 Etc/Not_A_Zone" AS TIMESTAMP)`, gcvctor.NullOf(typector.Timestamp())},
		{`SAFE_CAST("2020-06-02 00:00:00 PST" AS TIMESTAMP)`, gcvctor.NullOf(typector.Timestamp())},
		{`SAFE_CAST("2020-06-02 00:00:00+5:3" AS TIMESTAMP)`, gcvctor.NullOf(typector.Timestamp())},
		{`SAFE_CAST("2020-06-02 01:02-7" AS TIMESTAMP)`, gcvctor.NullOf(typector.Timestamp())},
		{`SAFE_CAST("2020-06-02 12:00:00 -7" AS TIMESTAMP)`, gcvctor.NullOf(typector.Timestamp())},
		{`SAFE_CAST(PENDING_COMMIT_TIMESTAMP() AS DATE)`, gcvctor.NullOf(typector.Date())},
		{`SAFE_CAST("not-a-uuid" AS UUID)`, gcvctor.NullOf(typector.UUID())},
		{`SAFE_CAST(b"foo" AS UUID)`, gcvctor.NullOf(typector.UUID())},
		{`SAFE_CAST(" 1970-01-01 " AS DATE)`, gcvctor.NullOf(typector.Date())},
		{`SAFE_CAST(" 1970-01-01T00:00:00Z " AS TIMESTAMP)`, gcvctor.NullOf(typector.Timestamp())},
		{`SAFE_CAST(" 94a01a73-d90a-432d-a03f-5db58ea8058f " AS UUID)`, gcvctor.NullOf(typector.UUID())},
		{`SAFE_CAST(" P1Y " AS INTERVAL)`, gcvctor.NullOf(typector.Interval())},
		{`SAFE_CAST("not-an-interval" AS INTERVAL)`, gcvctor.NullOf(typector.Interval())},

		{"PENDING_COMMIT_TIMESTAMP()", gcvctor.StringBasedValueFromCode(sppb.TypeCode_TIMESTAMP, "spanner.commit_timestamp()")},

		{`CAST("P1Y2M3DT4H5M6.5S" AS INTERVAL)`, must(gcvctor.IntervalStringValue(`P1Y2M3DT4H5M6.5S`))},
		{"CAST(NULL AS INTERVAL)", gcvctor.NullOf(typector.Interval())},

		{"INTERVAL 3 YEAR", gcvctor.StringBasedValueFromCode(sppb.TypeCode_INTERVAL, "P3Y")},
		{"INTERVAL 3 QUARTER", gcvctor.StringBasedValueFromCode(sppb.TypeCode_INTERVAL, "P9M")},
		{"INTERVAL 3 MONTH", gcvctor.StringBasedValueFromCode(sppb.TypeCode_INTERVAL, "P3M")},
		{"INTERVAL 3 WEEK", gcvctor.StringBasedValueFromCode(sppb.TypeCode_INTERVAL, "P21D")},
		{"INTERVAL 3 DAY", gcvctor.StringBasedValueFromCode(sppb.TypeCode_INTERVAL, "P3D")},
		{"INTERVAL 3 HOUR", gcvctor.StringBasedValueFromCode(sppb.TypeCode_INTERVAL, "PT3H")},
		{"INTERVAL 3 MINUTE", gcvctor.StringBasedValueFromCode(sppb.TypeCode_INTERVAL, "PT3M")},
		{"INTERVAL 3 SECOND", gcvctor.StringBasedValueFromCode(sppb.TypeCode_INTERVAL, "PT3S")},
		{"INTERVAL 3 MILLISECOND", gcvctor.StringBasedValueFromCode(sppb.TypeCode_INTERVAL, "PT0.003S")},
		{"INTERVAL 3 MICROSECOND", gcvctor.StringBasedValueFromCode(sppb.TypeCode_INTERVAL, "PT0.000003S")},
		{"INTERVAL 3 NANOSECOND", gcvctor.StringBasedValueFromCode(sppb.TypeCode_INTERVAL, "PT0.000000003S")},

		{`INTERVAL '2-11' YEAR TO MONTH`, gcvctor.StringBasedValueFromCode(sppb.TypeCode_INTERVAL, "P2Y11M")},
		{`INTERVAL '2-11 28' YEAR TO DAY`, gcvctor.StringBasedValueFromCode(sppb.TypeCode_INTERVAL, "P2Y11M28D")},
		{`INTERVAL '2-11 28 16' YEAR TO HOUR`, gcvctor.StringBasedValueFromCode(sppb.TypeCode_INTERVAL, "P2Y11M28DT16H")},
		{`INTERVAL '2-11 28 16:15' YEAR TO MINUTE`, gcvctor.StringBasedValueFromCode(sppb.TypeCode_INTERVAL, "P2Y11M28DT16H15M")},
		{`INTERVAL '2-11 28 16:15:14' YEAR TO SECOND`, gcvctor.StringBasedValueFromCode(sppb.TypeCode_INTERVAL, "P2Y11M28DT16H15M14S")},
		{`INTERVAL '11 28' MONTH TO DAY`, gcvctor.StringBasedValueFromCode(sppb.TypeCode_INTERVAL, "P11M28D")},
		{`INTERVAL '11 28 16' MONTH TO HOUR`, gcvctor.StringBasedValueFromCode(sppb.TypeCode_INTERVAL, "P11M28DT16H")},
		{`INTERVAL '11 28 16:15' MONTH TO MINUTE`, gcvctor.StringBasedValueFromCode(sppb.TypeCode_INTERVAL, "P11M28DT16H15M")},
		{`INTERVAL '11 28 16:15:14' MONTH TO SECOND`, gcvctor.StringBasedValueFromCode(sppb.TypeCode_INTERVAL, "P11M28DT16H15M14S")},
		{`INTERVAL '28 16' DAY TO HOUR`, gcvctor.StringBasedValueFromCode(sppb.TypeCode_INTERVAL, "P28DT16H")},
		{`INTERVAL '28 16:15' DAY TO MINUTE`, gcvctor.StringBasedValueFromCode(sppb.TypeCode_INTERVAL, "P28DT16H15M")},
		{`INTERVAL '28 16:15:14' DAY TO SECOND`, gcvctor.StringBasedValueFromCode(sppb.TypeCode_INTERVAL, "P28DT16H15M14S")},
		{`INTERVAL '16:15' HOUR TO MINUTE`, gcvctor.StringBasedValueFromCode(sppb.TypeCode_INTERVAL, "PT16H15M")},
		{`INTERVAL '16:15:14' HOUR TO SECOND`, gcvctor.StringBasedValueFromCode(sppb.TypeCode_INTERVAL, "PT16H15M14S")},
		{`INTERVAL '15:14' MINUTE TO SECOND`, gcvctor.StringBasedValueFromCode(sppb.TypeCode_INTERVAL, "PT15M14S")},
		{`INTERVAL '10:20:30.52' HOUR TO SECOND`, gcvctor.StringBasedValueFromCode(sppb.TypeCode_INTERVAL, "PT10H20M30.520S")},
		{`INTERVAL '20:30.123456789' MINUTE TO SECOND`, gcvctor.StringBasedValueFromCode(sppb.TypeCode_INTERVAL, "PT20M30.123456789S")},

		// Casted NULLs
		{"CAST(NULL AS INT64)", gcvctor.NullOf(typector.Int64())},
		{"CAST(NULL AS FLOAT64)", gcvctor.NullOf(typector.Float64())},
		{"CAST(NULL AS UUID)", gcvctor.NullOf(typector.UUID())},
		{"CAST(NULL AS ARRAY<INT64>)", gcvctor.NullArrayOf(typector.Int64())},
		{
			"CAST(NULL AS STRUCT<foo INT64>)",
			gcvctor.NullOf(typector.MustNameCodeSlicesToStructType([]string{"foo"}, []sppb.TypeCode{sppb.TypeCode_INT64})),
		},
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

	// SAFE_CAST success parity: every CAST success case should also succeed
	// under SAFE_CAST and return the same value.
	for _, tt := range tests {
		idx := strings.Index(tt.input, "CAST(")
		if idx < 0 || strings.Contains(tt.input, "SAFE_CAST(") {
			continue
		}
		safeInput := tt.input[:idx] + "SAFE_CAST(" + tt.input[idx+len("CAST("):]
		t.Run(safeInput, func(t *testing.T) {
			got, err := memebridge.ParseExpr("", safeInput)
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

			if diff := cmp.Diff(typector.Numeric(), got.Type, protocmp.Transform()); diff != "" {
				t.Errorf("type mismatch (-want +got):\n%s", diff)
			}

			gotRat := mustOk((&big.Rat{}).SetString(got.Value.GetStringValue()))
			if tt.want.Cmp(gotRat) != 0 {
				t.Errorf("mismatch want: %v, got: %v", tt.want, gotRat)
			}
		})
	}
}

func TestParseExpr_AllParenthesizedNullArrayInfersInt64(t *testing.T) {
	got, err := memebridge.ParseExpr("", "[(NULL)]")
	if err != nil {
		t.Fatalf("should not fail, but err: %v", err)
	}
	want := must(gcvctor.ArrayValueOf(typector.Int64(), gcvctor.NullOf(typector.Int64())))
	if diff := cmp.Diff(want, got, protocmp.Transform(), cmpopts.EquateNaNs()); diff != "" {
		t.Errorf("mismatch (-want +got):\n%s", diff)
	}
}

func TestParseExpr_InvalidCastReturnsError(t *testing.T) {
	tests := []string{
		`CAST("maybe" AS BOOL)`,
		`CAST(" true " AS BOOL)`,
		`CAST("12x" AS INT64)`,
		`CAST("not-a-number" AS NUMERIC)`,
		`CAST("1/2" AS NUMERIC)`,
		`CAST("0x10" AS NUMERIC)`,
		`CAST("1e2" AS NUMERIC)`,
		`CAST(1e50 AS FLOAT32)`,
		`CAST(CAST("NaN" AS FLOAT64) AS NUMERIC)`,
		`CAST(CAST("Infinity" AS FLOAT64) AS NUMERIC)`,
		`CAST(1e50 AS NUMERIC)`,
		`CAST(NUMERIC "9223372036854775807.5" AS INT64)`,
		`CAST("not-a-uuid" AS UUID)`,
		`CAST(b"foo" AS UUID)`,
		`CAST(" 1970-01-01 " AS DATE)`,
		`CAST(" 1970-01-01T00:00:00Z " AS TIMESTAMP)`,
		`CAST(" 94a01a73-d90a-432d-a03f-5db58ea8058f " AS UUID)`,
		`CAST("not-an-interval" AS INTERVAL)`,
		`CAST(" P1Y " AS INTERVAL)`,
		`CAST(CAST("nan" AS FLOAT64) AS INT64)`,
		`CAST(b"\xff" AS STRING)`,
		`CAST(PENDING_COMMIT_TIMESTAMP() AS DATE)`,
		`SAFE_CAST(TRUE AS BYTES)`,
		`SAFE_CAST([1] AS INT64)`,
		`STRUCT<i INT64>("1")`,
		`STRUCT<b BYTES>("foo")`,
		`STRUCT<i INT64>(CAST(NULL AS STRING))`,
		`STRUCT<d DATE>(" 1970-01-01 ")`,
		`STRUCT<u UUID>(" 94a01a73-d90a-432d-a03f-5db58ea8058f ")`,
		`STRUCT<a ARRAY<INT64>>(["1"])`,
		`STRUCT<a ARRAY<BYTES>>(["foo"])`,
		`STRUCT<a ARRAY<DATE>>(["not-a-date"])`,
		`STRUCT<a ARRAY<INT64>>([CAST(NULL AS STRING)])`,
		`STRUCT<f32 FLOAT32>(CAST(NULL AS INT64))`,
		`STRUCT<a ARRAY<FLOAT32>>([CAST(NULL AS INT64)])`,
		`STRUCT<a ARRAY<FLOAT32>>([CAST(1 AS FLOAT64)])`,
		`CAST([1] AS ARRAY<BYTES>)`,
		`CAST(STRUCT(1 AS foo, 2 AS bar) AS STRUCT<foo INT64>)`,
	}
	for _, input := range tests {
		t.Run(input, func(t *testing.T) {
			if _, err := memebridge.ParseExpr("", input); err == nil {
				t.Fatal("expected error")
			}
		})
	}
}
