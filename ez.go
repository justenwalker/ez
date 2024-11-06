// Copyright 2024, Justen Walker
// SPDX-License-Identifier: Apache-2.0

// Package ez declares a Stream interface and implementations,
// allowing data stream operations to be applied in a functional style.
package ez

import (
	"cmp"
	"errors"
	"iter"
	"math/rand/v2"
	"slices"
)

// ErrInfiniteSequence indicates that an operation requiring a finite Stream has been given an InfiniteStream.
// Certain operations like Collect and Reverse only work on Finite streams.
var ErrInfiniteSequence = errors.New("infinite sequence")

// IsFinite checks whether a given Stream is finite by asserting if it implements the Finite interface.
func IsFinite(stream any) bool {
	v, ok := stream.(Finite)
	return ok && v.Finite()
}

// AssertFinite asserts that the given stream is finite.
// NOTE: If stream is not finite, this function will panic with ErrInfiniteSequence.
func AssertFinite(stream any) {
	if !IsFinite(stream) {
		panic(ErrInfiniteSequence)
	}
}

// Finite is a marker interface to detect if a Stream is finite.
// A Stream must be finite in order for certain operations to be performed
// such as Collect, or Sort, or Shuffle.
type Finite interface {
	Finite() bool
}

// Collect returns a slice of all the elements collected from the stream.
// If the Stream implements `Collect() []D`, that implementation is used.
// Otherwise, this function collects all elements of the Stream's sequence.
// If the Stream implements `Size() int`, it can be used to reduce allocations, since the slice can be pre-allocated.
// NOTE: If stream is not finite, this function will panic with ErrInfiniteSequence.
func Collect[D any](stream Stream[D]) []D {
	AssertFinite(stream)
	type collector interface {
		Collect() []D
	}
	if c, ok := stream.(collector); ok {
		return c.Collect()
	}
	var collection []D
	type sizer interface {
		Size() int
	}
	if s, ok := stream.(sizer); ok {
		collection = make([]D, 0, s.Size())
	}
	for v := range stream.Seq() {
		collection = append(collection, v)
	}
	return collection
}

// Pipe returns a stream after the provided operations have been applied to it in sequence.
func Pipe[D any](stream Stream[D], pipeFunc ...func(iter Stream[D]) Stream[D]) Stream[D] {
	for _, pf := range pipeFunc {
		stream = pf(stream)
	}
	return stream
}

// Pipe2 returns a Stream2 after the provided operations have been applied to it in sequence.
func Pipe2[K any, V any](stream Stream2[K, V], pipeFunc ...func(iter Stream2[K, V]) Stream2[K, V]) Stream2[K, V] {
	for _, pf := range pipeFunc {
		stream = pf(stream)
	}
	return stream
}

// Shuffle returns a new Stream with data in a randomized order.
// NOTE: If stream is not finite, this function will panic with ErrInfiniteSequence.
func Shuffle[D any](stream Stream[D]) Stream[D] {
	values := Collect(stream)
	rand.Shuffle(len(values), func(i, j int) {
		values[i], values[j] = values[j], values[i]
	})
	return SliceStream(values)

}

// Shuffle2 returns a new Stream2 with data in a randomized order.
// NOTE: If stream is not finite, this function will panic with ErrInfiniteSequence.
func Shuffle2[L, R any](stream2 Stream2[L, R]) Stream2[L, R] {
	left, right := Collect2(stream2)
	rand.Shuffle(len(left), func(i, j int) {
		left[i], left[j] = left[j], left[i]
		right[i], right[j] = right[j], right[i]
	})
	return ZipSliceStream2(left, right)
}

// ShuffleWithRand returns a new Stream with data in a randomized order determined by the given random number generator.
// NOTE: If stream is not finite, this function will panic with ErrInfiniteSequence.
func ShuffleWithRand[D any](rng *rand.Rand) func(stream Stream[D]) Stream[D] {
	return func(stream Stream[D]) Stream[D] {
		values := Collect(stream)
		rng.Shuffle(len(values), func(i, j int) {
			values[i], values[j] = values[j], values[i]
		})
		return SliceStream(values)
	}
}

// Shuffle2WithRand returns a new Stream2 with data in a randomized order determined by the given random number generator.
// NOTE: If stream is not finite, this function will panic with ErrInfiniteSequence.
func Shuffle2WithRand[L, R any](rng *rand.Rand, stream2 Stream2[L, R]) Stream2[L, R] {
	left, right := Collect2(stream2)
	rng.Shuffle(len(left), func(i, j int) {
		left[i], left[j] = left[j], left[i]
		right[i], right[j] = right[j], right[i]
	})
	return ZipSliceStream2(left, right)
}

// Skip returns a new stream with the first n elements skipped.
func Skip[D any](n int) func(stream Stream[D]) Stream[D] {
	return func(stream Stream[D]) Stream[D] {
		seq := func(yield func(D) bool) {
			skip := n
			for v := range stream.Seq() {
				if skip > 0 {
					skip--
					continue
				}
				if !yield(v) {
					return
				}
			}
		}
		if sz := Size(stream); sz != -1 {
			if sz-n <= 0 {
				return SliceStream[D](nil)
			}
			return FiniteStreamWithSize(seq, sz-n)
		}
		if IsFinite(stream) {
			return FiniteStream(seq)
		}
		return InfiniteStream(seq)
	}
}

// Skip2 returns a new Stream2 with the first n elements skipped.
func Skip2[L any, R any](n int) func(stream2 Stream2[L, R]) Stream2[L, R] {
	return func(stream Stream2[L, R]) Stream2[L, R] {
		seq2 := func(yield func(L, R) bool) {
			skip := n
			for l, r := range stream.Seq2() {
				if skip > 0 {
					skip--
					continue
				}
				if !yield(l, r) {
					return
				}
			}
		}
		if sz := Size(stream); sz != -1 {
			if sz-n <= 0 {
				return ZipSliceStream2[L, R](nil, nil)
			}
			return FiniteStream2WithSize(seq2, sz-n)
		}
		if IsFinite(stream) {
			return FiniteStream2(seq2)
		}
		return InfiniteStream2(seq2)
	}
}

// Take returns a new stream which takes at most 'N' elements from the stream.
// Since take puts an upper bound on the elements returned by the stream, the returned
// Stream is guaranteed to be Finite.
func Take[D any](n int) func(stream Stream[D]) Stream[D] {
	return func(stream Stream[D]) Stream[D] {
		return &finiteSizeStream[D]{
			size: n,
			seq: func(yield func(D) bool) {
				take := n
				for v := range stream.Seq() {
					if take <= 0 {
						return
					}
					if !yield(v) {
						return
					}
					take--
				}
			},
		}
	}
}

// Take2 returns a new Stream2 which takes at most 'N' elements from the stream.
// Since take puts an upper bound on the elements returned by the stream, the returned
// Stream2 is guaranteed to be Finite.
func Take2[L, R any](n int) func(stream2 Stream2[L, R]) Stream2[L, R] {
	return func(stream Stream2[L, R]) Stream2[L, R] {
		return &finiteStream2WithSize[L, R]{
			size: n,
			seq2: func(yield func(L, R) bool) {
				take := n
				for l, r := range stream.Seq2() {
					if take <= 0 {
						return
					}
					if !yield(l, r) {
						return
					}
					take--
				}
			},
		}
	}
}

// Concat concatenates one or more streams together.
// If all the Streams given are Finite, the resulting concatenation is also Finite.
// If all the streams given have Size, then the resulting Stream is Finite, and also has a Size.
func Concat[D any](streams ...Stream[D]) Stream[D] {
	finite := true
	var size int
	hasSize := true
	for _, stream := range streams {
		if !IsFinite(stream) {
			finite = false
			hasSize = false
			break
		}
		n := Size(stream)
		if n == -1 {
			hasSize = false
			break
		}
		size += n
	}
	seq := func(yield func(D) bool) {
		for _, a := range streams {
			for v := range a.Seq() {
				if !yield(v) {
					return
				}
			}
		}
	}
	if hasSize {
		return FiniteStreamWithSize(seq, size)
	}
	if finite {
		return FiniteStream(seq)
	}
	return InfiniteStream(seq)
}

// Concat2 concatenates one or more streams together.
// If all the Streams given are Finite, the resulting concatenation is also Finite.
// If all the streams given have Size, then the resulting Stream is Finite, and also has a Size.
func Concat2[L, R any](streams ...Stream2[L, R]) Stream2[L, R] {
	finite := true
	var size int
	hasSize := true
	for _, stream := range streams {
		if !IsFinite(stream) {
			finite = false
			hasSize = false
			break
		}
		n := Size(stream)
		if n == -1 {
			hasSize = false
			break
		}
		size += n
	}
	seq2 := func(yield func(L, R) bool) {
		for _, a := range streams {
			for l, r := range a.Seq2() {
				if !yield(l, r) {
					return
				}
			}
		}
	}
	if hasSize {
		return FiniteStream2WithSize(seq2, size)
	}
	if finite {
		return FiniteStream2(seq2)
	}
	return InfiniteStream2(seq2)
}

// Reverse returns a new stream with the data elements in reverse order.
// NOTE: If stream is not finite, this function will panic with ErrInfiniteSequence.
func Reverse[D any](stream Stream[D]) Stream[D] {
	values := Collect(stream)
	slices.Reverse(values)
	return SliceStream(values)
}

// Reverse2 returns a new stream with the data elements in reverse order.
// NOTE: If stream is not finite, this function will panic with ErrInfiniteSequence.
func Reverse2[L, R any](stream Stream2[L, R]) Stream2[L, R] {
	left, right := Collect2(stream)
	slices.Reverse(left)
	slices.Reverse(right)
	return ZipSliceStream2(left, right)
}

// Ordered sorts Ordered types by their natural order, ascending.
func Ordered[D cmp.Ordered](stream Stream[D]) Stream[D] {
	return Sort(cmp.Compare[D])(stream)
}

// Sort returns a new stream with the data elements in sorted order.
// NOTE: If stream is not finite, this function will panic with ErrInfiniteSequence.
func Sort[D any](cmp func(a, b D) int) func(stream Stream[D]) Stream[D] {
	return func(stream Stream[D]) Stream[D] {
		values := Collect(stream)
		slices.SortFunc(values, cmp)
		return SliceStream(values)
	}
}

// Filter returns a new stream with all m for which the predicate returns true.
func Filter[D any](predicate func(v D) bool) func(stream Stream[D]) Stream[D] {
	return func(stream Stream[D]) Stream[D] {
		seq := func(yield func(D) bool) {
			for v := range stream.Seq() {
				if predicate(v) {
					if !yield(v) {
						return
					}
				}
			}
		}
		if IsFinite(stream) {
			return FiniteStream(seq)
		}
		return InfiniteStream(seq)
	}
}

// Filter2 returns a new stream with all m for which the predicate returns true.
func Filter2[L, R any](predicate func(l L, r R) bool) func(stream Stream2[L, R]) Stream2[L, R] {
	return func(stream Stream2[L, R]) Stream2[L, R] {
		seq2 := func(yield func(L, R) bool) {
			for l, r := range stream.Seq2() {
				if predicate(l, r) {
					if !yield(l, r) {
						return
					}
				}
			}
		}
		if IsFinite(stream) {
			return FiniteStream2(seq2)
		}
		return InfiniteStream2(seq2)
	}
}

// Map maps m in one stream to m in another stream of the same type.
func Map[D any](mapFn func(v D) D) func(stream Stream[D]) Stream[D] {
	return func(stream Stream[D]) Stream[D] {
		seq := func(yield func(D) bool) {
			for v := range stream.Seq() {
				if !yield(mapFn(v)) {
					return
				}
			}
		}
		if IsFinite(stream) {
			return FiniteStream(seq)
		}
		return InfiniteStream(seq)
	}
}

// Map2 maps m in one stream to m in another stream of the same type.
func Map2[L, R any](mapFn func(l L, r R) (L, R)) func(stream2 Stream2[L, R]) Stream2[L, R] {
	return func(stream2 Stream2[L, R]) Stream2[L, R] {
		seq2 := func(yield func(L, R) bool) {
			for l, r := range stream2.Seq2() {
				if !yield(mapFn(l, r)) {
					return
				}
			}
		}
		if IsFinite(stream2) {
			return FiniteStream2(seq2)
		}
		return InfiniteStream2(seq2)
	}
}

// Distinct returns a new stream containing only the distinct elements of the original stream by filtering out duplicates.
// NOTE: the memory consumed by this stream is unbounded if the Stream is not Finite.
func Distinct[D comparable](stream Stream[D]) Stream[D] {
	seen := make(map[D]struct{})
	return Filter(func(v D) bool {
		_, ok := seen[v]
		if ok {
			return false
		}
		seen[v] = struct{}{}
		return true
	})(stream)
}

// Convert converts a stream of type D to a stream of type R using the provided conversion function.
func Convert[D, R any](stream Stream[D], convertFunc func(v D) R) Stream[R] {
	seq := func(yield func(R) bool) {
		for v := range stream.Seq() {
			if !yield(convertFunc(v)) {
				return
			}
		}
	}
	if IsFinite(stream) {
		return FiniteStream(seq)
	}
	return InfiniteStream(seq)
}

// Convert2 converts a Stream2 of one type to a Stream2 of another type  using convertFunc.
func Convert2[L1 any, R1 any, L2 any, R2 any](stream2 Stream2[L1, R1], convertFunc func(v1 L1, v2 R1) (L2, R2)) Stream2[L2, R2] {
	seq := func(yield func(L2, R2) bool) {
		for l1, r1 := range stream2.Seq2() {
			l2, r2 := convertFunc(l1, r1)
			if !yield(l2, r2) {
				return
			}
		}
	}
	if IsFinite(stream2) {
		return FiniteStream2(seq)
	}
	return InfiniteStream2(seq)
}

// Size returns the maximum number of elements in the stream, if the stream has a Size method.
// It returns -1 if the Stream size is unknown; for instance: an InfiniteStream, or FiniteStream with no size.
func Size(stream any) int {
	type sizer interface {
		Size() int
	}
	if s, ok := stream.(sizer); ok {
		return s.Size()
	}
	return -1
}

// Reduce processes elements of a stream sequentially and applies an accumulator function to reduce them to a single value.
func Reduce[D any](stream Stream[D], initial D, accumFunc func(acc D, d D) D) D {
	for d := range stream.Seq() {
		initial = accumFunc(initial, d)
	}
	return initial
}

// Left extracts the left values from the given Stream2 and returns them as a Stream.
func Left[L any, R any](stream2 Stream2[L, R]) Stream[L] {
	return &seqStream[L]{
		seq: func(yield func(L) bool) {
			for k, _ := range stream2.Seq2() {
				if !yield(k) {
					return
				}
			}
		},
	}
}

// Right extracts the right values from a given Stream2 and returns them as a Stream.
func Right[L any, R any](stream2 Stream2[L, R]) Stream[R] {
	return &seqStream[R]{
		seq: func(yield func(R) bool) {
			for _, v := range stream2.Seq2() {
				if !yield(v) {
					return
				}
			}
		},
	}
}

func Collect2[L any, R any](stream2 Stream2[L, R]) (left []L, right []R) {
	AssertFinite(stream2)
	if sz := Size(stream2); sz != -1 {
		left = make([]L, 0, sz)
		right = make([]R, 0, sz)
	}
	for k, v := range stream2.Seq2() {
		left = append(left, k)
		right = append(right, v)
	}
	return left, right
}

// Zip returns a Stream2 produced by zipping a key and value stream together.
// If the both streams are infinite, the returns Stream2 is also infinite.
// If any of the given streams is finite, then a finite Stream2 will be returned
// having a length of the smallest size between the two streams.
func Zip[L any, R any](left Stream[L], right Stream[R]) Stream2[L, R] {
	seq2 := func(yield func(L, R) bool) {
		next, stop := iter.Pull(right.Seq())
		defer stop()
		for k := range left.Seq() {
			v, ok := next()
			if !ok {
				return
			}
			if !yield(k, v) {
				return
			}
		}
	}
	sz1, sz2 := Size(left), Size(right)
	if sz1 != -1 && sz2 != -1 { // both stream have a size
		// so the resulting stream is the lesser of the two sizes.
		FiniteStream2WithSize(seq2, min(sz1, sz2))
	}
	if IsFinite(left) || IsFinite(right) {
		// If either stream is finite, then the zipped stream is finite.
		return FiniteStream2(seq2)
	}
	// both streams are infinite
	return InfiniteStream2(seq2)
}

// Swap2 takes a Stream2 with left and right elements, and returns a new Stream2 with the elements swapped.
func Swap2[L, R any](stream2 Stream2[L, R]) Stream2[R, L] {
	seq2 := func(yield func(R, L) bool) {
		for k, v := range stream2.Seq2() {
			if !yield(v, k) {
				return
			}
		}
	}
	if sz := Size(stream2); sz != -1 {
		return FiniteStream2WithSize(seq2, sz)
	}
	if IsFinite(stream2) {
		return FiniteStream2(seq2)
	}
	return InfiniteStream2(seq2)
}

// ConvertToMap converts the Stream2 to a Map of L to R values.
func ConvertToMap[K comparable, V any](stream2 Stream2[K, V]) map[K]V {
	AssertFinite(stream2)
	m := make(map[K]V)
	for k, v := range stream2.Seq2() {
		m[k] = v
	}
	return m
}
