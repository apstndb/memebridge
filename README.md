# memebridge

[![Go Reference](https://pkg.go.dev/badge/github.com/apstndb/memebridge.svg)](https://pkg.go.dev/github.com/apstndb/memebridge)

memebridge is a Go package to convert between [`memefish/ast.Type`](https://pkg.go.dev/github.com/cloudspannerecosystem/memefish/ast#Type) and [`spannerpb.Type`](https://pkg.go.dev/cloud.google.com/go/spanner/apiv1/spannerpb#Type), and to convert `memefish` expressions into `cloud.google.com/go/spanner.GenericColumnValue`.

The [`cliparams`](https://pkg.go.dev/github.com/apstndb/memebridge/cliparams) subpackage converts CLI-style query parameter assignments (`name:value` flags or already-split maps) into `spanner.GenericColumnValue` maps. It is shared by [spanner-mycli](https://github.com/apstndb/spanner-mycli) and [execspansql](https://github.com/apstndb/execspansql).

## Compatibility

- `memebridge v0.5.0` requires `github.com/apstndb/spanvalue v0.2.x`.
- `memebridge v0.6.0` requires `github.com/apstndb/spanvalue v0.3.x`.
- `memebridge v0.6.2` and later require Go 1.24+, `github.com/apstndb/spanvalue v0.8.x`, and `github.com/cloudspannerecosystem/memefish v0.7.x`.

Temporal casts use Cloud Spanner's default time zone, `America/Los_Angeles`.
By default, memebridge relies on the runtime's system zoneinfo; build with the `memebridge_tzdata` tag to embed Go's IANA tzdata for minimal runtimes that do not provide zoneinfo.
