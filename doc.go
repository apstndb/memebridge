// Package memebridge converts memefish GoogleSQL AST nodes into Cloud Spanner
// types and values.
//
// The conversion pipeline is:
//
//	GoogleSQL text → memefish.ParseExpr / ParseType → ast.Expr / ast.Type
//	→ memebridge → spannerpb.Type + spanner.GenericColumnValue
//
// memebridge evaluates literal expressions (including CAST and SAFE_CAST),
// applies expected-type coercion for STRUCT fields and ARRAY elements, and
// maps memefish types to spannerpb.Type via spantype/typector. GCV wire
// assembly uses spanvalue/gcvctor.
//
// # Entry points
//
// ParseExpr parses a SQL expression string and returns a GenericColumnValue.
// MemefishExprToGCV converts an already-parsed ast.Expr. MemefishTypeToSpannerpbType
// maps ast.Type to spannerpb.Type.
//
// The cliparams subpackage parses CLI-style name:value parameter assignments.
//
// # Semantic source of truth
//
// Literal evaluation and CAST behavior aim to match Cloud Spanner (and
// googlesql cast tables). Temporal casts without an explicit time zone use
// America/Los_Angeles. Build with the memebridge_tzdata tag to embed IANA
// tzdata on minimal runtimes.
//
// # Special contracts
//
// PENDING_COMMIT_TIMESTAMP() is recognized case-insensitively and yields a
// TIMESTAMP GenericColumnValue whose string wire value is the placeholder
// "spanner.commit_timestamp()". Downstream Spanner clients interpret this
// sentinel; memebridge preserves it through TIMESTAMP→STRING casts.
//
// Array literals require elements to coerce to the declared or inferred element
// type by default. Use [WithLegacyArrayWirePassthrough] on [MemefishExprToGCV] or
// [ParseExpr] to restore pre-v0.7 behavior that preserves original element wire
// values when coercion fails. Strict coercion is always used on expected-type
// paths (typed STRUCT fields, ARRAY<T> annotations in typed contexts).
package memebridge
