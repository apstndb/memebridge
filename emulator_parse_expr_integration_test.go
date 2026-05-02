//go:build integration

package memebridge_test

import (
	"fmt"
	goast "go/ast"
	"go/parser"
	"go/token"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"

	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/apstndb/memebridge"
	"github.com/cloudspannerecosystem/memefish"
	mfast "github.com/cloudspannerecosystem/memefish/ast"
)

const maxSuccessBatchColumns = 16

var embeddedEmulatorKnownErrors = map[string]string{
	`[CAST("P1Y" AS INTERVAL), "P2Y"]`: "embedded emulator does not infer STRING/INTERVAL array common supertypes",
	`[1, "2"]`:                         "embedded emulator does not infer STRING-to-INT64 array coercion",
	`["P1Y", CAST("P2Y" AS INTERVAL)]`: "embedded emulator does not infer STRING/INTERVAL array common supertypes",
	`STRUCT<d DATE, ts TIMESTAMP, u UUID, i INTERVAL>("1970-01-01", "1970-01-01T00:00:00Z", "94a01a73-d90a-432d-a03f-5db58ea8058f", "P1Y")`: "embedded emulator rejects STRING-to-INTERVAL coercion inside typed struct literals",
	`STRUCT<a ARRAY<NUMERIC>>([1, NULL])`:                           "embedded emulator rejects typed ARRAY field coercion from ARRAY<INT64> to ARRAY<NUMERIC>",
	`STRUCT<a ARRAY<FLOAT64>>([1, NUMERIC "2.5"])`:                  "embedded emulator rejects typed ARRAY field coercion from ARRAY<NUMERIC> to ARRAY<FLOAT64>",
	`STRUCT<a ARRAY<DATE>>(["1970-01-01"])`:                         "embedded emulator rejects typed ARRAY field coercion from ARRAY<STRING> to ARRAY<DATE>",
	`STRUCT<a ARRAY<STRUCT<n NUMERIC>>>([STRUCT(1), STRUCT(NULL)])`: "embedded emulator rejects typed ARRAY<STRUCT> field coercion",
	`CAST(PENDING_COMMIT_TIMESTAMP() AS STRING)`:                    "PENDING_COMMIT_TIMESTAMP() cannot be used in SELECT expressions on the embedded emulator",
	`CAST([1] AS ARRAY<FLOAT64>)`:                                   "embedded emulator does not support ARRAY element-type casts",
	`CAST([NUMERIC "1.5"] AS ARRAY<FLOAT64>)`:                       "embedded emulator does not support ARRAY element-type casts",
	`CAST([1, NULL] AS ARRAY<FLOAT64>)`:                             "embedded emulator does not support ARRAY element-type casts",
	`SAFE_CAST(PENDING_COMMIT_TIMESTAMP() AS DATE)`:                 "PENDING_COMMIT_TIMESTAMP() cannot be used in SELECT expressions on the embedded emulator",
	`SAFE_CAST(["not-a-date"] AS ARRAY<DATE>)`:                      "embedded emulator does not support ARRAY element-type casts even under SAFE_CAST",
	`PENDING_COMMIT_TIMESTAMP()`:                                    "PENDING_COMMIT_TIMESTAMP() cannot be used in SELECT expressions on the embedded emulator",
}

var embeddedEmulatorKnownSuccesses = map[string]string{
	`CAST("1e2" AS NUMERIC)`: "embedded emulator accepts exponent notation for STRING-to-NUMERIC CAST",
}

type emulatorParseExprCase struct {
	name string
	expr string
}

func TestEmbeddedEmulatorParseExprSuccessCases(t *testing.T) {
	requireSpannerMyCLI(t)

	cases, err := loadParseExprSuccessCases()
	if err != nil {
		t.Fatal(err)
	}
	cases = filterCases(cases, embeddedEmulatorKnownErrors)

	for i := 0; i < len(cases); i += maxSuccessBatchColumns {
		end := min(i+maxSuccessBatchColumns, len(cases))
		batch := cases[i:end]
		t.Run(fmt.Sprintf("batch-%03d", i/maxSuccessBatchColumns+1), func(t *testing.T) {
			query, err := successBatchQuery(batch)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := runSpannerMyCLIQuery(query); err == nil {
				return
			} else {
				for _, tc := range batch {
					tc := tc
					t.Run(tc.name, func(t *testing.T) {
						query, err := singleCaseQuery(tc.expr, true)
						if err != nil {
							t.Fatal(err)
						}
						if output, err := runSpannerMyCLIQuery(query); err != nil {
							t.Fatalf("query failed: %v\nquery: %s\noutput:\n%s", err, query, output)
						}
					})
				}
			}
		})
	}
}

func TestEmbeddedEmulatorParseExprStructDescribeTypes(t *testing.T) {
	requireSpannerMyCLI(t)

	cases, err := loadParseExprSuccessCases()
	if err != nil {
		t.Fatal(err)
	}
	cases = filterCases(cases, embeddedEmulatorKnownErrors)

	for _, tc := range cases {
		tc := tc
		gcv, err := memebridge.ParseExpr("", tc.expr)
		if err != nil || gcv.Type.GetCode() != sppb.TypeCode_STRUCT {
			continue
		}

		t.Run(tc.name, func(t *testing.T) {
			query, err := describeCaseQuery(tc.expr, true)
			if err != nil {
				t.Fatal(err)
			}
			output, err := runSpannerMyCLIQuery(query)
			if err != nil {
				t.Fatalf("describe failed: %v\nquery: %s\noutput:\n%s", err, query, output)
			}

			expected := "ARRAY<" + spannerTypeToGoogleSQL(gcv.Type) + ">"
			if !strings.Contains(output, expected) {
				t.Fatalf("describe output missing expected type %q\nquery: %s\noutput:\n%s", expected, query, output)
			}
		})
	}
}

func TestEmbeddedEmulatorParseExprErrorCases(t *testing.T) {
	requireSpannerMyCLI(t)

	cases, err := loadParseExprErrorCases()
	if err != nil {
		t.Fatal(err)
	}
	cases = filterCases(cases, embeddedEmulatorKnownSuccesses)

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			query, err := singleCaseQuery(tc.expr, false)
			if err != nil {
				t.Fatal(err)
			}
			if output, err := runSpannerMyCLIQuery(query); err == nil {
				t.Fatalf("expected emulator error, but query succeeded\nquery: %s\noutput:\n%s", query, output)
			}
		})
	}
}

func TestEmbeddedEmulatorParseExprKnownDivergences(t *testing.T) {
	requireSpannerMyCLI(t)

	for expr, reason := range embeddedEmulatorKnownErrors {
		expr := expr
		reason := reason
		t.Run("known-error/"+expr, func(t *testing.T) {
			query, err := singleCaseQuery(expr, false)
			if err != nil {
				t.Fatal(err)
			}
			if output, err := runSpannerMyCLIQuery(query); err == nil {
				t.Fatalf("expected known emulator error (%s), but query succeeded\nquery: %s\noutput:\n%s", reason, query, output)
			}
		})
	}

	for expr, reason := range embeddedEmulatorKnownSuccesses {
		expr := expr
		reason := reason
		t.Run("known-success/"+expr, func(t *testing.T) {
			query, err := singleCaseQuery(expr, false)
			if err != nil {
				t.Fatal(err)
			}
			if output, err := runSpannerMyCLIQuery(query); err != nil {
				t.Fatalf("expected known emulator success (%s), but query failed: %v\nquery: %s\noutput:\n%s", reason, err, query, output)
			}
		})
	}
}

func requireSpannerMyCLI(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("spanner-mycli"); err != nil {
		t.Skipf("spanner-mycli not found: %v", err)
	}
}

func successBatchQuery(cases []emulatorParseExprCase) (string, error) {
	parts := make([]string, len(cases))
	for i, tc := range cases {
		expr, err := selectExprForEmulator(tc.expr, true)
		if err != nil {
			return "", err
		}
		parts[i] = fmt.Sprintf("%s AS e%d", expr, i+1)
	}
	return "SELECT " + strings.Join(parts, ", "), nil
}

func singleCaseQuery(expr string, expectSuccess bool) (string, error) {
	rendered, err := selectExprForEmulator(expr, expectSuccess)
	if err != nil {
		return "", err
	}
	return "SELECT " + rendered + " AS e1", nil
}

func describeCaseQuery(expr string, expectSuccess bool) (string, error) {
	rendered, err := selectExprForEmulator(expr, expectSuccess)
	if err != nil {
		return "", err
	}
	return "DESCRIBE SELECT " + rendered + " AS e1", nil
}

func selectExprForEmulator(expr string, expectSuccess bool) (string, error) {
	if expectSuccess {
		gcv, err := memebridge.ParseExpr("", expr)
		if err != nil {
			return "", fmt.Errorf("memebridge ParseExpr(%q): %w", expr, err)
		}
		if gcv.Type.GetCode() == sppb.TypeCode_STRUCT {
			return "[" + expr + "]", nil
		}
		return expr, nil
	}

	returnType, err := parseTopLevelReturnType(expr)
	if err != nil {
		return "", err
	}
	if returnType == sppb.TypeCode_STRUCT {
		return "[" + expr + "]", nil
	}
	return expr, nil
}

func parseTopLevelReturnType(expr string) (sppb.TypeCode, error) {
	parsed, err := memefish.ParseExpr("", expr)
	if err != nil {
		return sppb.TypeCode_TYPE_CODE_UNSPECIFIED, fmt.Errorf("memefish ParseExpr(%q): %w", expr, err)
	}
	return inferTopLevelTypeFromAST(parsed), nil
}

func inferTopLevelTypeFromAST(expr mfast.Expr) sppb.TypeCode {
	switch e := expr.(type) {
	case *mfast.ParenExpr:
		return inferTopLevelTypeFromAST(e.Expr)
	case *mfast.TypelessStructLiteral, *mfast.TupleStructLiteral, *mfast.TypedStructLiteral:
		return sppb.TypeCode_STRUCT
	case *mfast.CastExpr:
		return inferTypeCodeFromMemefishType(e.Type)
	case *mfast.ArrayLiteral:
		return sppb.TypeCode_ARRAY
	default:
		return sppb.TypeCode_TYPE_CODE_UNSPECIFIED
	}
}

func inferTypeCodeFromMemefishType(typ mfast.Type) sppb.TypeCode {
	switch t := typ.(type) {
	case *mfast.ArrayType:
		return sppb.TypeCode_ARRAY
	case *mfast.StructType:
		return sppb.TypeCode_STRUCT
	case *mfast.NamedType:
		if len(t.Path) == 1 && strings.EqualFold(t.Path[0].Name, "UUID") {
			return sppb.TypeCode_UUID
		}
		return sppb.TypeCode_TYPE_CODE_UNSPECIFIED
	case *mfast.SimpleType:
		switch t.Name {
		case mfast.BoolTypeName:
			return sppb.TypeCode_BOOL
		case mfast.Int64TypeName:
			return sppb.TypeCode_INT64
		case mfast.Float32TypeName:
			return sppb.TypeCode_FLOAT32
		case mfast.Float64TypeName:
			return sppb.TypeCode_FLOAT64
		case mfast.NumericTypeName:
			return sppb.TypeCode_NUMERIC
		case mfast.StringTypeName:
			return sppb.TypeCode_STRING
		case mfast.BytesTypeName:
			return sppb.TypeCode_BYTES
		case mfast.DateTypeName:
			return sppb.TypeCode_DATE
		case mfast.TimestampTypeName:
			return sppb.TypeCode_TIMESTAMP
		case mfast.JSONTypeName:
			return sppb.TypeCode_JSON
		case mfast.IntervalTypeName:
			return sppb.TypeCode_INTERVAL
		default:
			return sppb.TypeCode_TYPE_CODE_UNSPECIFIED
		}
	default:
		return sppb.TypeCode_TYPE_CODE_UNSPECIFIED
	}
}

func runSpannerMyCLIQuery(query string) (string, error) {
	cmd := exec.Command(
		"spanner-mycli",
		"--verbose",
		"--embedded-emulator",
		"--execute",
		query,
	)
	output, err := cmd.CombinedOutput()
	return string(output), err
}

func spannerTypeToGoogleSQL(typ *sppb.Type) string {
	if typ == nil {
		return "UNKNOWN"
	}

	switch typ.GetCode() {
	case sppb.TypeCode_BOOL:
		return "BOOL"
	case sppb.TypeCode_INT64:
		return "INT64"
	case sppb.TypeCode_FLOAT32:
		return "FLOAT32"
	case sppb.TypeCode_FLOAT64:
		return "FLOAT64"
	case sppb.TypeCode_NUMERIC:
		return "NUMERIC"
	case sppb.TypeCode_STRING:
		return "STRING"
	case sppb.TypeCode_BYTES:
		return "BYTES"
	case sppb.TypeCode_DATE:
		return "DATE"
	case sppb.TypeCode_TIMESTAMP:
		return "TIMESTAMP"
	case sppb.TypeCode_JSON:
		return "JSON"
	case sppb.TypeCode_UUID:
		return "UUID"
	case sppb.TypeCode_INTERVAL:
		return "INTERVAL"
	case sppb.TypeCode_ARRAY:
		return "ARRAY<" + spannerTypeToGoogleSQL(typ.GetArrayElementType()) + ">"
	case sppb.TypeCode_STRUCT:
		fields := typ.GetStructType().GetFields()
		parts := make([]string, 0, len(fields))
		for _, field := range fields {
			if field == nil {
				continue
			}
			fieldType := spannerTypeToGoogleSQL(field.Type)
			if field.Name == "" {
				parts = append(parts, fieldType)
				continue
			}
			parts = append(parts, field.Name+" "+fieldType)
		}
		return "STRUCT<" + strings.Join(parts, ", ") + ">"
	default:
		return typ.GetCode().String()
	}
}

func loadParseExprSuccessCases() ([]emulatorParseExprCase, error) {
	file, err := parseMemestrTestFile()
	if err != nil {
		return nil, err
	}

	var cases []emulatorParseExprCase
	for _, fnName := range []string{"TestParseExpr", "TestParseExpr_Numeric"} {
		inputs, err := extractTestsTableStrings(findFuncDecl(file, fnName))
		if err != nil {
			return nil, err
		}
		for _, input := range inputs {
			cases = append(cases, emulatorParseExprCase{name: input, expr: input})
		}
	}

	oneOff, err := extractParseExprCallString(findFuncDecl(file, "TestParseExpr_AllParenthesizedNullArrayInfersInt64"))
	if err != nil {
		return nil, err
	}
	cases = append(cases, emulatorParseExprCase{name: oneOff, expr: oneOff})
	return dedupeCases(cases), nil
}

func loadParseExprErrorCases() ([]emulatorParseExprCase, error) {
	file, err := parseMemestrTestFile()
	if err != nil {
		return nil, err
	}

	inputs, err := extractTestsTableStrings(findFuncDecl(file, "TestParseExpr_InvalidCastReturnsError"))
	if err != nil {
		return nil, err
	}

	cases := make([]emulatorParseExprCase, 0, len(inputs))
	for _, input := range inputs {
		cases = append(cases, emulatorParseExprCase{name: input, expr: input})
	}
	return dedupeCases(cases), nil
}

func dedupeCases(cases []emulatorParseExprCase) []emulatorParseExprCase {
	seen := make(map[string]struct{}, len(cases))
	result := make([]emulatorParseExprCase, 0, len(cases))
	for _, tc := range cases {
		if _, ok := seen[tc.expr]; ok {
			continue
		}
		seen[tc.expr] = struct{}{}
		result = append(result, tc)
	}
	return result
}

func filterCases(cases []emulatorParseExprCase, skip map[string]string) []emulatorParseExprCase {
	result := make([]emulatorParseExprCase, 0, len(cases))
	for _, tc := range cases {
		if _, ok := skip[tc.expr]; ok {
			continue
		}
		result = append(result, tc)
	}
	return result
}

func parseMemestrTestFile() (*goast.File, error) {
	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		return nil, fmt.Errorf("resolve current file path")
	}
	path := filepath.Join(filepath.Dir(currentFile), "memestr_to_sppb_value_test.go")
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, nil, 0)
	if err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	return file, nil
}

func findFuncDecl(file *goast.File, name string) *goast.FuncDecl {
	for _, decl := range file.Decls {
		fn, ok := decl.(*goast.FuncDecl)
		if ok && fn.Name != nil && fn.Name.Name == name {
			return fn
		}
	}
	return nil
}

func extractTestsTableStrings(fn *goast.FuncDecl) ([]string, error) {
	if fn == nil || fn.Body == nil {
		return nil, fmt.Errorf("test function not found")
	}
	for _, stmt := range fn.Body.List {
		assign, ok := stmt.(*goast.AssignStmt)
		if !ok || len(assign.Lhs) != 1 || len(assign.Rhs) != 1 {
			continue
		}
		ident, ok := assign.Lhs[0].(*goast.Ident)
		if !ok || ident.Name != "tests" {
			continue
		}
		lit, ok := assign.Rhs[0].(*goast.CompositeLit)
		if !ok {
			continue
		}
		var result []string
		for _, elt := range lit.Elts {
			value, ok := firstStringLiteral(elt)
			if !ok {
				continue
			}
			result = append(result, value)
		}
		if len(result) == 0 {
			return nil, fmt.Errorf("no string test cases found in %s", fn.Name.Name)
		}
		return result, nil
	}
	return nil, fmt.Errorf("tests table not found in %s", fn.Name.Name)
}

func extractParseExprCallString(fn *goast.FuncDecl) (string, error) {
	if fn == nil || fn.Body == nil {
		return "", fmt.Errorf("test function not found")
	}

	var result string
	goast.Inspect(fn.Body, func(n goast.Node) bool {
		call, ok := n.(*goast.CallExpr)
		if !ok || len(call.Args) < 2 {
			return true
		}
		sel, ok := call.Fun.(*goast.SelectorExpr)
		if !ok || sel.Sel == nil || sel.Sel.Name != "ParseExpr" {
			return true
		}
		value, ok := stringLiteralValue(call.Args[1])
		if !ok {
			return true
		}
		result = value
		return false
	})
	if result == "" {
		return "", fmt.Errorf("ParseExpr call string not found in %s", fn.Name.Name)
	}
	return result, nil
}

func firstStringLiteral(expr goast.Expr) (string, bool) {
	switch e := expr.(type) {
	case *goast.BasicLit:
		return stringLiteralValue(e)
	case *goast.CompositeLit:
		if len(e.Elts) == 0 {
			return "", false
		}
		return stringLiteralValueFromField(e.Elts[0])
	case *goast.KeyValueExpr:
		return stringLiteralValue(e.Value)
	default:
		return "", false
	}
}

func stringLiteralValueFromField(expr goast.Expr) (string, bool) {
	switch e := expr.(type) {
	case *goast.KeyValueExpr:
		return stringLiteralValue(e.Value)
	default:
		return stringLiteralValue(expr)
	}
}

func stringLiteralValue(expr goast.Expr) (string, bool) {
	lit, ok := expr.(*goast.BasicLit)
	if !ok || lit.Kind != token.STRING {
		return "", false
	}
	value, err := strconv.Unquote(lit.Value)
	if err != nil {
		return "", false
	}
	return value, true
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
