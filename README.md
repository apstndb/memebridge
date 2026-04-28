# memebridge

[![Go Reference](https://pkg.go.dev/badge/github.com/apstndb/memebridge.svg)](https://pkg.go.dev/github.com/apstndb/memebridge)

memebridge is a Go package to convert between [`memefish/ast.Type`](https://pkg.go.dev/github.com/cloudspannerecosystem/memefish/ast#Type) and [`spannerpb.Type`](https://pkg.go.dev/cloud.google.com/go/spanner/apiv1/spannerpb#Type), and to convert `memefish` expressions into `cloud.google.com/go/spanner.GenericColumnValue`.

## Compatibility

- `memebridge v0.5.0` targets `github.com/apstndb/spanvalue v0.2.x`.
- The current development line after `v0.5.0` targets `github.com/apstndb/spanvalue v0.3.x`.

If you consume `memebridge` from another module, keep the `spanvalue` major/minor line aligned with the `memebridge` line you are using.

## `spanvalue v0.3` migration note

The `spanvalue v0.3` line removed several deprecated `gcvctor` aliases. If your downstream code still uses the older names, update them as follows:

| Old name | Replacement |
| --- | --- |
| `TypedNull` | `NullOf` |
| `SimpleTypedNull` | `NullFromCode` |
| `StringBasedValue` | `StringBasedValueFromCode` |
| `StructValue` | `StructValueOf` |

`memebridge` already follows the new `gcvctor` API surface expected by `spanvalue v0.3.x`.
