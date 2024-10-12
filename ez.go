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
// If it is not, this function panics with ErrInfiniteSequence
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
func AssumeFinite[D any](stream Stream[D]) Stream[D] {
	if IsFinite[D](stream) {
		return stream
	}
	return &finiteSeqStream[D]{seq: stream.Seq()}
}

// SliceStream returns a Finite Stream backed by the given slice of data elements.
// Since slices are finite, the stream returned by this function is also finite.
func SliceStream[D any](values []D) Stream[D] {
	return &sliceStream[D]{values: values}
}

// FiniteStream returns a Finite Stream backed by the given sequence.
// The sequence provided is assumed to be finite.
func FiniteStream[D any](seq iter.Seq[D]) Stream[D] {
	return &finiteSeqStream[D]{seq: seq}
}

// FiniteStreamWithSize returns a Finite Stream backed by the given sequence for which the size is known.
// The sequence provided is assumed to be finite and have the exact number of data elements given.
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
// NOTE: This function panics if the given stream is not Finite.
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
// NOTE: If the stream is not finite, this function will panic.
func Shuffle[D any](stream Stream[D]) Stream[D] {
	values := Collect(stream)
	rand.Shuffle(len(values), func(i, j int) {
		values[i], values[j] = values[j], values[i]
	})
	return SliceStream(values)

}

// ShuffleWithRand returns a new Stream with data in a randomized order determined by the given random number generator.
// NOTE: If the stream is not finite, this function will panic.
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
// Stream is guaranteed to be finite.
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
// If all the streams given has a Size, then the resulting Stream is Finite, and also has a Size.
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
// NOTE: if the given stream is not finite, this function will panic.
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
// NOTE: if the given stream is not Finite, this function will panic.
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

// Distinct returns a new Stream that filters out duplicate elements.
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

// Convert takes a stream of one type of data value D and converts it to a stream of another type R.
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

// Size returns the number of elements in the stream, if the stream has this capability.
// For Infinite Streams, and Finite streams for which the size is unknown; it returns -1.
func Size[D any](stream Stream[D]) int {
	type sizer interface {
		Size() int
	}
	if s, ok := stream.(sizer); ok {
		return s.Size()
	}
	return -1
}

// Reduce applies a reducing function to the elements of the Stream, starting with an initial value, and returns the result.
func Reduce[D any](stream Stream[D], initial D, reducer func(acc D, v D) D) D {
	for v := range stream.Seq() {
		initial = reducer(initial, v)
	}
	return initial
}

type seqStream[D any] struct {
	seq iter.Seq[D]
}

func (it *seqStream[D]) Seq() iter.Seq[D] {
	return it.seq
}

type finiteSizeStream[D any] struct {
	seq  iter.Seq[D]
	size int
}

func (it *finiteSizeStream[D]) Size() int {
	return it.size
}

func (it *finiteSizeStream[D]) Finite() bool {
	return true
}

func (it *finiteSizeStream[D]) Seq() iter.Seq[D] {
	return it.seq
}

type finiteSeqStream[D any] struct {
	seq iter.Seq[D]
}

func (it *finiteSeqStream[D]) Seq() iter.Seq[D] {
	return it.seq
}

func (it *finiteSeqStream[D]) Finite() bool {
	return true
}

type sliceStream[D any] struct {
	values []D
}

func (it *sliceStream[D]) Finite() bool {
	return true
}

func (it *sliceStream[D]) Size() int {
	return len(it.values)
}

func (it *sliceStream[D]) Seq() iter.Seq[D] {
	return func(yield func(D) bool) {
		for _, v := range it.values {
			if !yield(v) {
				return
			}
		}
	}
}

func (it *sliceStream[D]) Collect() []D {
	vs := make([]D, len(it.values))
	copy(vs, it.values)
	return vs
}
