# Dependency Feedback Notes

Feedback gathered while building and operating `memebridge`. Tracks **upstream
API gaps** and **memebridge design policy** (e.g. forward compatibility,
`gcvctor` package boundary).

Does **not** duplicate migration runbooks — version upgrades and consumer
adoption are covered by upstream **release notes** and **godoc**, and by
memebridge **README** compatibility lines where this library's own semver
matters.

**Local prototypes** for open spanvalue API requests live in
[`gcv_helpers.go`](gcv_helpers.go) (unexported). GCV-returning requests target
`gcvctor`; wire/predicate requests target root `spanvalue` — see package boundary
below.

---

## Stack snapshot

### Published (`v0.6.2`, tag on `main`)

| Component | Version | Role in memebridge |
|-----------|---------|-------------------|
| Go | 1.24.0 | module minimum |
| [memefish](https://github.com/cloudspannerecosystem/memefish) | v0.7.0 | `ParseExpr`, `ParseType`, AST |
| [spanvalue](https://github.com/apstndb/spanvalue) / `gcvctor` | v0.8.2 | GCV construction |
| [spantype](https://github.com/apstndb/spantype) / `typector` | v0.3.13 | `spannerpb.Type` builders |
| `cloud.google.com/go/spanner` | v1.84.1 | `GenericColumnValue`, `spannerpb` |

`memebridge` imports **`gcvctor` only** from spanvalue (not `writer` or
`FormatConfig`). `cliparams` also uses `gcvctor` for typed NULL params.

---

## How the layers fit together

```
GoogleSQL text
    → memefish          ParseExpr / ParseType → ast.Expr, ast.Type
    → memebridge        AST → spannerpb.Type + GenericColumnValue; CAST/coercion
        uses spantype/typector for Type shapes
        uses spanvalue/gcvctor for GCV wire assembly
    → cliparams         CLI "name:value" → map[string]GenericColumnValue
    → Spanner client / server
```

| Layer | Owns | Does not own |
|-------|------|--------------|
| **memefish** | SQL syntax, literal AST | Spanner semantics, wire encoding |
| **spantype** | `spannerpb.Type` construction | Values, CAST rules |
| **spanvalue** | GCV construction, NULL/wire rules, formatting | Expression evaluation |
| **memebridge** | Literal evaluation, expected-type coercion, CAST/SAFE_CAST emulation | Parsing SQL outside memefish |

Semantic truth source: **Cloud Spanner** (and googlesql cast tables when
implementing CAST). Emulator disagreements are noted but not automatic grounds
to change behavior.

---

## Forward compatibility: unknown Spanner types

When `type.proto` gains a `TypeCode` before memefish, spanvalue, or memebridge
catch up, each layer should fail predictably. The goal is **explicit errors**,
not silent coercion and not `TYPE_CODE_UNSPECIFIED` as a stand-in for
"unsupported".

### Responsibility by layer

| Layer | When the type is unknown |
|-------|--------------------------|
| `spannerpb` (go mod bump) | New enum values compile; no runtime behavior |
| **memefish** | **Parse-time failure** for unknown type syntax or literal forms |
| **memebridge** | **Explicit error** on mapping, evaluation, or CAST — no guessing |
| **spanvalue / gcvctor** | Dedicated helpers may be missing; **scalar wire passthrough** may still work |
| **Spanner server** | Authoritative accept/reject |

```
type.proto (new TypeCode)
    → spannerpb
    → memefish (syntax / AST)           ← lag blocks text input
    → memebridge (semantics)            ← lag blocks evaluation / CAST
    → spanvalue (constructors / format) ← lag may allow transport-only paths
    → Spanner
```

Upper layers are stricter. Lower layers may be permissive only when shuttling
already-valid wire without interpreting it.

### memebridge policy

1. **Literals and CAST — fail closed**
   - Unknown scalar names → `unknown type` (`meme_to_sppb_type.go`).
   - Unhandled `ast.Expr` kinds → `ErrUnsupportedExpr` (`meme_to_sppb_value.go`).
   - Unhandled cast pairs → `ErrUnsupportedCast` (`cast.go`).
   - Never map unknown codes to a known scalar; never emit
     `TYPE_CODE_UNSPECIFIED` to mean unsupported.

2. **memefish lag — stop at parse**
   - Return `ParseType` / `ParseExpr` errors unchanged.
   - `cliparams` bare-type-as-NULL (`WithBareTypeAsNull`) fails the same way.
   - No ad hoc type-name parsing in memebridge.

3. **Proto ahead of memebridge maps**
   - After a `spannerpb` upgrade, `TypeCode_FOO` may compile while
     `ScalarTypeNameToTypeCodeMap` or expression handlers do not.
   - Prefer `unsupported Spanner type FOO` over generic cast errors or
     UNSPECIFIED.

4. **CAST / SAFE_CAST**
   - `CAST` errors when conversion rules are missing.
   - `SAFE_CAST` returns typed NULL when the target type is known but
     conversion fails or is unsupported (existing behavior).

5. **Bare `NULL` literal**
   - Typed as `INT64` NULL to match Spanner query-parameter default
     (`gcvctor.NullFromCode(INT64)`). This is a deliberate Spanner parity
     choice, not an unknown-type fallback.

6. **Optional future: narrow wire passthrough**
   - Only when memefish produced a literal, `spannerpb.Type` is fully
     specified, wire shape is documented, and no CAST/coercion is needed.
   - Would use `gcvctor.StringBasedValueOf`, not unchecked retyping.
   - CAST and coercion paths must not use this escape hatch.

7. **Versioning**
   - README compatibility table documents required dependency versions per
     memebridge release.
   - New Spanner types expect coordinated bumps: memefish (syntax) →
     memebridge (semantics) → spanvalue (constructors), as needed.

### Lag flavors

| Lag | What breaks | What may still work |
|-----|-------------|---------------------|
| **memefish** | Writing the new type/literal in SQL | Nothing upstream of parse |
| **memebridge** | Local evaluation, CAST, coercion | Parse-only tools; server-side execution |
| **spanvalue** | Convenient ctor/format | `StringBasedValueOf` for known string-wire scalars |
| **spantype** | Building complex `Type` trees | Types built manually from `spannerpb` |

ARRAY, STRUCT, PROTO, and ENUM generally need full typector/gcvctor support;
scalar passthrough is not a general solution.

### Anti-patterns (all layers)

- Unknown `TypeCode` → `TYPE_CODE_UNSPECIFIED`.
- Silent coercion to `INT64` or `STRING` (except the documented bare-`NULL` rule).
- Unchecked `WithType` on semantic paths to hide missing support.
- Assuming `go.mod` bump on `spannerpb` alone enables end-user SQL for a new type.

---

## `github.com/apstndb/spanvalue`

### Package boundary: `gcvctor` vs root `spanvalue`

**Policy (upstream request):** put APIs in **`gcvctor` only when the primary
result is `spanner.GenericColumnValue`**. Everything else belongs in the root
`spanvalue` package (or another non-gcvctor subpackage).

| Result | Package | Examples |
|--------|---------|----------|
| `GenericColumnValue` | **`gcvctor`** | `Int64Value`, `NullFromCode`, `WithType`, `NormalizeArrayElements` |
| `bool`, predicates | **`spanvalue`** | `IsNull`, `KnownTypeCode` (requested) |
| `*structpb.Value`, `[]*structpb.Value` | **`spanvalue`** | `ToProtoValue`, `ArrayWireValues` (requested) |
| formatted string / writer | **`writer`**, `protofmt` | out of scope for memebridge |

Rationale: `gcvctor` is the GCV constructor surface. Wire extraction, NULL
materialization for nested `structpb` assembly, and type-support predicates
operate on or below the GCV layer but do not *produce* a GCV — they should not
expand gcvctor's API surface.

Local prototypes in [`gcv_helpers.go`](gcv_helpers.go) follow the same split:
`gcvWith*` → gcvctor; `gcvToProtoValue` / `gcvArrayWireValues` → root
`spanvalue` (file name is historical).

### Consumer posture

- **gcvctor** for GCV construction; **root `spanvalue`** for `IsNull` and
  (if added) wire helpers.
- No `writer`, `dbsqlrows`, or `protofmt` in memebridge.
- `cliparams` uses `gcvctor.NullOf` / type helpers alongside memebridge.

### Resolved in v0.8.x (adopted in memebridge v0.6.2)

| Need | Upstream API | memebridge usage |
|------|--------------|------------------|
| NULL detection | `spanvalue.IsNull` | `isNullGCV` → `cast.go` |
| ARRAY NULL elements | `gcvctor.NormalizeArrayElements` | array literals, `coerceArrayElements` |
| Safe NUMERIC | `gcvctor.NumericValueChecked` | `cast.go`, array coercion |
| Validated temporal / UUID / interval strings | `DateStringValue`, `TimestampStringValue`, `IntervalStringValue`, `UUIDStringValue` | CAST, string-literal coercion |
| Scalar typed NULL | `gcvctor.NullFromCode` | bare `NULL` literal |
| Equivalent type retype | `gcvctor.WithEquivalentType` | identity CAST, ARRAY cast |
| Exact type match | `gcvctor.WithExactType` | expected-type coercion |

### Still open — requested upstream APIs

Tracked on github.com/apstndb/spanvalue (see Action tracker below).

#### `gcvctor` (result is `GenericColumnValue`)

| # | Requested API | Pain | Used in |
|---|---------------|------|---------|
| G1 | `WithType(typ, gcv) GCV` | Identity cast / coercion rewrites `Type` only; unchecked misuse is easy | `cast.go`, `meme_to_sppb_value.go` |
| G2 | `WithEquivalentType(typ, gcv) (GCV, error)` | **Resolved** in spanvalue v0.8.x | identity CAST, ARRAY cast |
| G3 | `WithExactType(typ, gcv) (GCV, error)` | **Resolved** in spanvalue v0.8.x | expected-type coercion |
| G4 | gcvctor package-doc recipe | `StringBasedValueFromCode` (wire preservation) vs validated `*StringValue` (CAST) | literal vs cast paths |
| G5 | Godoc cross-links to `spanvalue.IsNull` | NULL construction in gcvctor, detection in root | consumer imports |

#### Root `spanvalue` (result is not `GenericColumnValue`)

Prototypes: `gcvToProtoValue`, `gcvArrayWireValues`.

| # | Requested API | Pain | Used in |
|---|---------------|------|---------|
| S1 | `ToProtoValue(gcv) *structpb.Value` | ARRAY/STRUCT assembly must materialize `structpb.NullValue` | STRUCT CAST (`cast.go`) |
| S2 | `ArrayWireValues(elems []GCV) []*structpb.Value` | Repeated loop + NULL materialization for `ListValue` | permissive ARRAY fallback |
| S3 | `KnownTypeCode(code) bool` (or similar) | No shared "supported / constructible / formattable" predicate | forward-compat policy |
| S4 | Wire passthrough + `NullOf(nil)` docs | When to use `gcvctor.StringBasedValueOf`; UNSPECIFIED is not "unknown type" | forward-compat policy |

S1 and S2 share NULL rules with `spanvalue.IsNull`; they must not live in
`gcvctor` under the GCV-only policy.

### Out of scope for spanvalue

**Permissive ARRAY fallback** in memebridge (element types disagree with
declared element type but wire values are preserved) is intentional downstream
semantics, not a gcvctor builder responsibility.

---

## `github.com/apstndb/spantype`

No open API requests. memebridge uses **`typector`** only (`meme_to_sppb_type.go`,
tests, `cliparams` tests) to build `spannerpb.Type` values from memefish AST
types. When Spanner adds types, spantype/typector updates are usually needed in
the same release window as memebridge's type map.

---

## `github.com/cloudspannerecosystem/memefish`

### Documentation requests (unchanged)

Implementing semantic layers still depends on examples more than signatures:

1. **Nested literal examples** — typeless / tuple / typed STRUCT; ARRAY with
   and without explicit element types; nesting under outer expected types.
2. **AST shape for representative SQL** — especially typed inner STRUCT under
   typed outer STRUCT, ARRAY of STRUCT, typed ARRAY inside STRUCT fields.
3. **Example-oriented parser tests** downstream libraries can read as spec.

### Forward compatibility (memefish side)

When Spanner adds a type before memefish:

- **Expected:** `ParseType` / `ParseExpr` fail; memebridge never sees AST.
- **Useful upstream:** literal examples and release-note callouts when
  **expression** parser coverage changes (distinct from DDL/DML-only releases).
  Read memefish release notes for version-to-version parser deltas.

---

## Action tracker

| Item | Status |
|------|--------|
| spanvalue v0.8.x adoption | Released in memebridge v0.6.2 |
| memefish v0.7.0 bump | Released in memebridge v0.6.2 |
| `gcv_helpers.go` prototypes | Local only; delete when upstream APIs land |
| File spanvalue issues (G1, G4–G5, S1–S4) | Filed: [spanvalue#261](https://github.com/apstndb/spanvalue/issues/261), [spanvalue#262](https://github.com/apstndb/spanvalue/issues/262) |
| memebridge tracker issues from review | #35–#42 (see memebridge issues) |
| File memefish documentation issue | Not filed (memefish upstream) |
| gcvctor G2/G3 (`WithEquivalentType`, `WithExactType`) | Resolved in spanvalue v0.8.x |
