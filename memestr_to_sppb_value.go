package memebridge

import (
	"cloud.google.com/go/spanner"
	"github.com/cloudspannerecosystem/memefish"
)

// ParseExprToGCV parses a GoogleSQL expression string and evaluates it to a
// GenericColumnValue.
func ParseExprToGCV(expr string) (spanner.GenericColumnValue, error) {
	return ParseExprFile("", expr)
}

// ParseExprFile is like [ParseExprToGCV] but passes filename to memefish for
// error positions only.
func ParseExprFile(filename, expr string) (spanner.GenericColumnValue, error) {
	astExpr, err := memefish.ParseExpr(filename, expr)
	if err != nil {
		return spanner.GenericColumnValue{}, err
	}

	return MemefishExprToGCV(astExpr)
}
