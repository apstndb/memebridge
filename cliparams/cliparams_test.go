package cliparams_test

import (
	"strings"
	"testing"

	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/apstndb/spantype/typector"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/apstndb/memebridge/cliparams"
)

func gcvOf(typ *sppb.Type, value *structpb.Value) spanner.GenericColumnValue {
	return spanner.GenericColumnValue{Type: typ, Value: value}
}

func TestParseValue(t *testing.T) {
	for _, tt := range []struct {
		value string
		opts  []cliparams.Option
		want  spanner.GenericColumnValue
	}{
		{`1`, nil, gcvOf(typector.CodeToSimpleType(sppb.TypeCode_INT64), structpb.NewStringValue("1"))},
		{`"foo"`, nil, gcvOf(typector.CodeToSimpleType(sppb.TypeCode_STRING), structpb.NewStringValue("foo"))},
		{`TRUE`, nil, gcvOf(typector.CodeToSimpleType(sppb.TypeCode_BOOL), structpb.NewBoolValue(true))},
		{
			`["foo"]`, nil,
			gcvOf(typector.ElemCodeToArrayType(sppb.TypeCode_STRING),
				structpb.NewListValue(&structpb.ListValue{Values: []*structpb.Value{structpb.NewStringValue("foo")}})),
		},
		{
			`ARRAY<STRING>`, []cliparams.Option{cliparams.WithBareTypeAsNull()},
			gcvOf(typector.ElemCodeToArrayType(sppb.TypeCode_STRING), structpb.NewNullValue()),
		},
		{
			`STRUCT<x INT64>`, []cliparams.Option{cliparams.WithBareTypeAsNull()},
			gcvOf(typector.NameCodeToStructType("x", sppb.TypeCode_INT64), structpb.NewNullValue()),
		},
	} {
		t.Run(tt.value, func(t *testing.T) {
			got, err := cliparams.ParseValue(tt.value, tt.opts...)
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(tt.want, got, protocmp.Transform()); diff != "" {
				t.Errorf("ParseValue(%q) mismatch (-want +got):\n%s", tt.value, diff)
			}
		})
	}

	t.Run("bare type rejected without option", func(t *testing.T) {
		if _, err := cliparams.ParseValue(`ARRAY<STRING>`); err == nil {
			t.Error("ParseValue without WithBareTypeAsNull: want error, got nil")
		}
	})
	t.Run("invalid expression", func(t *testing.T) {
		if _, err := cliparams.ParseValue(`(`); err == nil {
			t.Error("want error, got nil")
		}
	})
}

func TestSplitAssignment(t *testing.T) {
	for _, tt := range []struct {
		arg       string
		opts      []cliparams.Option
		wantName  string
		wantValue string
		wantErr   bool
	}{
		{arg: `p1:1`, wantName: "p1", wantValue: "1"},
		// The value may contain the separator: split at the first occurrence.
		{arg: `ts:TIMESTAMP "2026-01-01T00:00:00Z"`, wantName: "ts", wantValue: `TIMESTAMP "2026-01-01T00:00:00Z"`},
		{arg: `p1=1`, opts: []cliparams.Option{cliparams.WithSeparator("=")}, wantName: "p1", wantValue: "1"},
		{arg: `p1`, wantErr: true},
		{arg: `:1`, wantErr: true},
	} {
		t.Run(tt.arg, func(t *testing.T) {
			name, value, err := cliparams.SplitAssignment(tt.arg, tt.opts...)
			if tt.wantErr {
				if err == nil {
					t.Errorf("SplitAssignment(%q): want error, got (%q, %q)", tt.arg, name, value)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if name != tt.wantName || value != tt.wantValue {
				t.Errorf("SplitAssignment(%q) = (%q, %q), want (%q, %q)", tt.arg, name, value, tt.wantName, tt.wantValue)
			}
		})
	}
}

func TestParseAssignments(t *testing.T) {
	got, err := cliparams.ParseAssignments([]string{`a:1`, `b:"x"`})
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]spanner.GenericColumnValue{
		"a": gcvOf(typector.CodeToSimpleType(sppb.TypeCode_INT64), structpb.NewStringValue("1")),
		"b": gcvOf(typector.CodeToSimpleType(sppb.TypeCode_STRING), structpb.NewStringValue("x")),
	}
	if diff := cmp.Diff(want, got, protocmp.Transform()); diff != "" {
		t.Errorf("ParseAssignments mismatch (-want +got):\n%s", diff)
	}

	t.Run("duplicate names", func(t *testing.T) {
		if _, err := cliparams.ParseAssignments([]string{`a:1`, `a:2`}); err == nil || !strings.Contains(err.Error(), "duplicate") {
			t.Errorf("want duplicate error, got %v", err)
		}
	})
	t.Run("per-name error context", func(t *testing.T) {
		_, err := cliparams.ParseAssignments([]string{`bad:(`})
		if err == nil || !strings.Contains(err.Error(), `"bad"`) {
			t.Errorf("want error mentioning parameter name, got %v", err)
		}
	})
}

func TestParseMap(t *testing.T) {
	got, err := cliparams.ParseMap(map[string]string{"n": "NUMERIC '1'"})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got["n"].Type.GetCode() != sppb.TypeCode_NUMERIC {
		t.Errorf("ParseMap = %v, want NUMERIC param n", got)
	}
}

func TestStatementParams(t *testing.T) {
	params := map[string]spanner.GenericColumnValue{
		"a": gcvOf(typector.CodeToSimpleType(sppb.TypeCode_INT64), structpb.NewStringValue("1")),
	}
	stmt := spanner.Statement{SQL: "SELECT @a", Params: cliparams.StatementParams(params)}
	if len(stmt.Params) != 1 {
		t.Errorf("Params = %v, want 1 entry", stmt.Params)
	}
	if _, ok := stmt.Params["a"].(spanner.GenericColumnValue); !ok {
		t.Errorf("Params[a] = %T, want spanner.GenericColumnValue", stmt.Params["a"])
	}
}
