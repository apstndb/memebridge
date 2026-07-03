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
// ParseExprToGCV parses a SQL expression string and returns a GenericColumnValue.
// ParseExprFile is the same with a filename for memefish error positions.
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
// Array literals use a permissive fallback when element values cannot be coerced
// to the declared or inferred element type: the resulting GCV is typed as
// ARRAY<T> but may retain original element wire values that do not match T.
// This preserves pre-v0.3 behavior for callers that defer validation to the
// server. Strict coercion is used on expected-type paths (typed STRUCT fields,
// ARRAY<T> annotations).
package memebridge
