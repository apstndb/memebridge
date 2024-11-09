package memebridge

import "spheric.cloud/xiter"

func tupledWithErr[T1, T2, R any](f func(T1, T2) (R, error)) func(xiter.Zipped[T1, T2]) (R, error) {
	return func(z xiter.Zipped[T1, T2]) (R, error) {
		return f(z.V1, z.V2)
	}
}
