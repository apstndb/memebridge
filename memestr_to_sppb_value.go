package memebridge

import (
	"cloud.google.com/go/spanner"
	"github.com/cloudspannerecosystem/memefish"
)

func ParseExpr(filepath, s string) (spanner.GenericColumnValue, error) {
	expr, err := memefish.ParseExpr(filepath, s)
	if err != nil {
		return spanner.GenericColumnValue{}, err
	}

	return MemefishExprToGCV(expr)
}
