package memebridge

// EvalOption configures expression evaluation.
type EvalOption func(*evalOptions)

type evalOptions struct {
	legacyArrayWirePassthrough bool
}

// WithLegacyArrayWirePassthrough restores pre-v0.7 behavior where ARRAY<T>
// literals preserve original element wire values when coercion to T fails.
// The default is strict coercion, which returns an error on incompatible elements.
func WithLegacyArrayWirePassthrough() EvalOption {
	return func(o *evalOptions) {
		o.legacyArrayWirePassthrough = true
	}
}

func applyEvalOptions(opts []EvalOption) evalOptions {
	var o evalOptions
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	return o
}
