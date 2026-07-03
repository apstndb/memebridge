package memebridge_test

import (
	"fmt"

	"github.com/apstndb/memebridge"
	"github.com/cloudspannerecosystem/memefish"
)

func ExampleParseExpr() {
	gcv, err := memebridge.ParseExpr("", `CAST(42 AS STRING)`)
	if err != nil {
		panic(err)
	}
	fmt.Println(gcv.Type.GetCode(), gcv.Value.GetStringValue())
	// Output: STRING 42
}

func ExampleMemefishExprToGCV() {
	expr, err := memefish.ParseExpr("", `STRUCT(1 AS x, ["a", "b"] AS tags)`)
	if err != nil {
		panic(err)
	}
	gcv, err := memebridge.MemefishExprToGCV(expr)
	if err != nil {
		panic(err)
	}
	fmt.Println(gcv.Type.GetStructType().GetFields()[0].GetName())
	fmt.Println(gcv.Type.GetStructType().GetFields()[1].GetType().GetArrayElementType().GetCode())
	// Output: x
	// STRING
}

func ExampleMemefishExprToGCV_cast() {
	expr, err := memefish.ParseExpr("", `CAST(TRUE AS STRING)`)
	if err != nil {
		panic(err)
	}
	gcv, err := memebridge.MemefishExprToGCV(expr)
	if err != nil {
		panic(err)
	}
	fmt.Println(gcv.Type.GetCode(), gcv.Value.GetStringValue())
	// Output: STRING true
}

func ExampleMemefishExprToGCV_array() {
	expr, err := memefish.ParseExpr("", `ARRAY<INT64>[1, 2, 3]`)
	if err != nil {
		panic(err)
	}
	gcv, err := memebridge.MemefishExprToGCV(expr)
	if err != nil {
		panic(err)
	}
	fmt.Println(gcv.Type.GetCode(), gcv.Type.GetArrayElementType().GetCode())
	// Output: ARRAY INT64
}

func ExampleMemefishExprToGCV_pendingCommitTimestamp() {
	expr, err := memefish.ParseExpr("", `PENDING_COMMIT_TIMESTAMP()`)
	if err != nil {
		panic(err)
	}
	gcv, err := memebridge.MemefishExprToGCV(expr)
	if err != nil {
		panic(err)
	}
	fmt.Println(gcv.Type.GetCode())
	// Output: TIMESTAMP
}
