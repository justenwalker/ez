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
	"sort"
)

// ErrInfiniteSequence indicates that an operation requiring a finite Stream has been given an InfiniteStream.
// Certain operations like Collect and Reverse only work on Finite streams.
var ErrInfiniteSequence = errors.New("infinite sequence")

// Stream represents a sequence of elements supporting sequential and parallel aggregate operations.
type Stream[D any] interface {
	// Seq returns the internal data sequence.
	Seq() iter.Seq[D]
}

// IsFinite checks whether a given Stream is finite by asserting if it implements the Finite interface.
func IsFinite[D any](stream Stream[D]) bool {
	v, ok := stream.(Finite)
	return ok && v.Finite()
}

// AssertFinite asserts that the given stream is finite.
// NOTE: If stream is not finite, this function will panic with ErrInfiniteSequence.
func AssertFinite[D any](stream Stream[D]) Stream[D] {
	if !IsFinite[D](stream) {
		panic(ErrInfiniteSequence)
	}
	return stream
}

// Finite is a marker interface to detect if a Stream is finite.
// A Stream must be finite in order for certain operations to be performed
// such as Collect, or Sort, or Shuffle.
type Finite interface {
	Finite() bool
}

// AssumeFinite converts a given Stream into a Finite Stream.
// The caller guarantees that the stream given is finite.
// If the stream given is not finite, the behavior is undefined, but likely will result in a program hang/crash.
// If the given stream is already finite, ie: IsFinite(stream) == true, then this function returns its input unchanged.
func AssumeFinite[D any](stream Stream[D]) Stream[D] {
	if IsFinite[D](stream) {
		return stream
	}
	return &finiteSeqStream[D]{seq: stream.Seq()}
}

// SliceStream returns a Finite Stream backed by the given slice of data elements.
// Since slices are finite, the stream returned by this function is Finite.
// Since slices have a known size, the stream returned by this function has Size.
func SliceStream[D any](values []D) Stream[D] {
	return &sliceStream[D]{values: values}
}

// FiniteStream returns a Finite Stream backed by the given sequence.
// The sequence provided is assumed to be finite.
// If the sequence given is not finite, the behavior is undefined, but likely will result in a program hang/crash.
func FiniteStream[D any](seq iter.Seq[D]) Stream[D] {
	return &finiteSeqStream[D]{seq: seq}
}

// FiniteStreamWithSize returns a Finite Stream backed by the given sequence for which the size is known.
// The sequence provided is assumed to be finite and have a most number of data elements given.
// If the sequence given is not finite, the behavior is undefined.
func FiniteStreamWithSize[D any](seq iter.Seq[D], size int) Stream[D] {
	return &finiteSizeStream[D]{seq: seq, size: size}
}

// InfiniteStream returns a new Stream containing an infinite data sequence.
// This function is used when the underlying sequence does not have a predefined end.
func InfiniteStream[D any](seq iter.Seq[D]) Stream[D] {
	return &seqStream[D]{seq: seq}
}

// Collect returns a slice of all the elements collected from the stream.
// If the Stream implements `Collect() []D`, that implementation is used.
// Otherwise, this function collects all elements of the Stream's sequence.
// If the Stream implements `Size() int`, it can be used to reduce allocations, since the slice can be pre-allocated.
// NOTE: If stream is not finite, this function will panic with ErrInfiniteSequence.
func Collect[D any](stream Stream[D]) []D {
	stream = AssertFinite(stream)
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

// Shuffle returns a new Stream with data in a randomized order.
// NOTE: If stream is not finite, this function will panic with ErrInfiniteSequence.
func Shuffle[D any](stream Stream[D]) Stream[D] {
	values := Collect(stream)
	rand.Shuffle(len(values), func(i, j int) {
		values[i], values[j] = values[j], values[i]
	})
	return SliceStream(values)

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

// Skip returns a new stream with the first n elements skipped.
func Skip[D any](n int) func(stream Stream[D]) Stream[D] {
	return func(stream Stream[D]) Stream[D] {
		seq := func(yield func(D) bool) {
			for v := range stream.Seq() {
				n--
				if n >= 0 {
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

// Take returns a new stream which takes at most 'N' elements from the stream.
// Since take puts an upper bound on the elements returned by the stream, the returned
// Stream is guaranteed to be Finite.
func Take[D any](n int) func(stream Stream[D]) Stream[D] {
	return func(stream Stream[D]) Stream[D] {
		return &finiteSizeStream[D]{
			size: n,
			seq: func(yield func(D) bool) {
				for v := range stream.Seq() {
					if !yield(v) {
						return
					}
					n--
					if n <= 0 {
						return
					}
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

// Reverse returns a new stream with the data elements in reverse order.
// NOTE: If stream is not finite, this function will panic with ErrInfiniteSequence.
func Reverse[D any](stream Stream[D]) Stream[D] {
	values := Collect(stream)
	slices.Reverse(values)
	return SliceStream(values)
}

// SortOrdered sorts Ordered types by their natural order, ascending.
func SortOrdered[D cmp.Ordered](stream Stream[D]) Stream[D] {
	return Sort(cmp.Less[D])(stream)
}

// Sort returns a new stream with the data elements in sorted order.
// NOTE: If stream is not finite, this function will panic with ErrInfiniteSequence.
func Sort[D any](less func(a, b D) bool) func(stream Stream[D]) Stream[D] {
	return func(stream Stream[D]) Stream[D] {
		values := Collect(stream)
		sort.Slice(values, func(i, j int) bool {
			return less(values[i], values[j])
		})
		return SliceStream(values)
	}
}

// Filter returns a new stream with all values for which the predicate returns true.
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

// Map maps values in one stream to values in another stream of the same type.
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

// Size returns the maximum number of elements in the stream, if the stream has a Size method.
// It returns -1 if the Stream size is unknown; for instance: an InfiniteStream, or FiniteStream with no size.
func Size[D any](stream Stream[D]) int {
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

// seqStream implements Stream for a potentially infinite iter.Seq of any type.
type seqStream[D any] struct {
	seq iter.Seq[D]
}

// Seq returns the sequence of the current data stream.
func (it *seqStream[D]) Seq() iter.Seq[D] {
	return it.seq
}

// finiteSizeStream represents a finite stream with a known, fixed size.
// seq holds the sequence of elements in the stream.
// size denotes the number of elements in the finite stream.
type finiteSizeStream[D any] struct {
	seq  iter.Seq[D]
	size int
}

// Size returns the size of the finiteSizeStream.
func (it *finiteSizeStream[D]) Size() int {
	return it.size
}

// Finite returns true indicating that the stream has a finite size.
func (it *finiteSizeStream[D]) Finite() bool {
	return true
}

// Seq returns the internal sequence of the finite size stream.
func (it *finiteSizeStream[D]) Seq() iter.Seq[D] {
	return it.seq
}

// finiteSeqStream represents a finite sequence stream of elements of any type.
type finiteSeqStream[D any] struct {
	seq iter.Seq[D]
}

// Seq returns the underlying sequence of type iter.Seq[D] from the finiteSeqStream instance.
func (it *finiteSeqStream[D]) Seq() iter.Seq[D] {
	return it.seq
}

// Finite returns true indicating that the stream has a finite size.
func (it *finiteSeqStream[D]) Finite() bool {
	return true
}

// sliceStream represents a finite stream backed by a slice of data elements.
type sliceStream[D any] struct {
	values []D
}

// Finite returns true indicating that the stream has a finite size.
func (it *sliceStream[D]) Finite() bool {
	return true
}

// Size returns the number of elements in the sliceStream.
func (it *sliceStream[D]) Size() int {
	return len(it.values)
}

// Seq converts the backing slice []D to an iter.Seq[D]
func (it *sliceStream[D]) Seq() iter.Seq[D] {
	return func(yield func(D) bool) {
		for _, v := range it.values {
			if !yield(v) {
				return
			}
		}
	}
}

// Collect returns a slice containing all elements of the sliceStream.
func (it *sliceStream[D]) Collect() []D {
	vs := make([]D, len(it.values))
	copy(vs, it.values)
	return vs
}
