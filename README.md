# memebridge

[![Go Reference](https://pkg.go.dev/badge/github.com/apstndb/memebridge.svg)](https://pkg.go.dev/github.com/apstndb/memebridge)

memebridge is a Go package to convert between [`memefish/ast.Type`](https://pkg.go.dev/github.com/cloudspannerecosystem/memefish/ast#Type) and [`spannerpb.Type`](https://pkg.go.dev/cloud.google.com/go/spanner/apiv1/spannerpb#Type), and to convert `memefish` expressions into `cloud.google.com/go/spanner.GenericColumnValue`.

## Compatibility

- `memebridge v0.5.0` requires `github.com/apstndb/spanvalue v0.2.x`.
- `memebridge v0.6.0` requires `github.com/apstndb/spanvalue v0.3.x`.
