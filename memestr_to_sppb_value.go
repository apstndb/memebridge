package memebridge

import (
	"cloud.google.com/go/spanner"
	"github.com/cloudspannerecosystem/memefish"
)

// ParseExpr parses a GoogleSQL expression string and evaluates it to a
// GenericColumnValue. The filepath argument is passed to memefish for error
// positions only; an empty string is fine when positions are not needed.
func ParseExpr(filepath, s string) (spanner.GenericColumnValue, error) {
	expr, err := memefish.ParseExpr(filepath, s)
	if err != nil {
		return spanner.GenericColumnValue{}, err
	}

	return MemefishExprToGCV(expr)
}
