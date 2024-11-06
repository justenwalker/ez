package ez

import (
	"cmp"
	"iter"
	"slices"
)

// Stream2 represents a generic interface for a stream of tuples.
type Stream2[L, R any] interface {
	Seq2() iter.Seq2[L, R]
}

// FiniteStream2 returns a Finite Stream2 backed by the given sequence.
// The sequence provided is assumed to be finite.
// If the sequence given is not finite, the behavior is undefined, but likely will result in a program hang/crash.
func FiniteStream2[L any, R any](seq2 iter.Seq2[L, R]) Stream2[L, R] {
	return &finiteStream2[L, R]{seq2: seq2}
}

// FiniteStream2WithSize returns a Finite Stream2 backed by the given sequence for which the size is known.
// The sequence provided is assumed to be finite and have a most number of data elements given.
// If the sequence given is not finite, the behavior is undefined.
func FiniteStream2WithSize[L any, R any](seq2 iter.Seq2[L, R], size int) Stream2[L, R] {
	return &finiteStream2WithSize[L, R]{seq2: seq2, size: size}
}

// InfiniteStream2 returns a new Stream2 containing an infinite data sequence.
func InfiniteStream2[L any, R any](seq2 iter.Seq2[L, R]) Stream2[L, R] {
	return &seqStream2[L, R]{seq2: seq2}
}

// AssumeFinite2 converts a given Stream2 into a Finite Stream2.
// The caller guarantees that the stream given is finite.
// If the stream given is not finite, the behavior is undefined, but likely will result in a program hang/crash.
// If the given stream is already finite, ie: IsFinite(stream) == true, then this function returns its input unchanged.
func AssumeFinite2[L, R any](stream Stream2[L, R]) Stream2[L, R] {
	if IsFinite(stream) {
		return stream
	}
	return &finiteStream2[L, R]{seq2: stream.Seq2()}
}

// ZipSliceStream2 converts the given pair of slices into a Stream2 by zipping them together.
// If the slices provided are not the same length, the stream returned will have the length of the smallest slice provided.
func ZipSliceStream2[L any, R any](left []L, right []R) Stream2[L, R] {
	size := min(len(left), len(right))
	if len(left) != len(right) { // make them equal
		left = left[:size]
		right = right[:size]
	}
	return &finiteStream2WithSize[L, R]{
		seq2: func(yield func(L, R) bool) {
			for i, l := range left {
				if !yield(l, right[i]) {
					return
				}
			}
		},
		size: size,
	}
}

// MapKVStream converts the given map into a Stream2.
// NOTE: the stream will produce items in an indeterminate order.
// For a well-defined ordering, use MapKVStreamOrdered or MapKVStreamSorted.
func MapKVStream[K comparable, V any](m map[K]V) Stream2[K, V] {
	return &mapStream[K, V]{inner: m}
}

// MapKVStreamOrdered converts the given map into a Stream2 using the natural order of the keys.
func MapKVStreamOrdered[K cmp.Ordered, V any](m map[K]V) Stream2[K, V] {
	return MapKVStreamSorted(m, cmp.Compare)
}

// MapKVStreamSorted converts the given map into a Stream2 using the provided key ordering.
func MapKVStreamSorted[K comparable, V any](m map[K]V, cmp func(a, b K) int) Stream2[K, V] {
	keys := make([]K, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.SortFunc(keys, cmp)
	return &finiteStream2WithSize[K, V]{
		seq2: func(yield func(K, V) bool) {
			for _, k := range keys {
				if !yield(k, m[k]) {
					return
				}
			}
		},
		size: len(m),
	}
}

// seqStream2 implements Stream2 for a potentially infinite iter.Seq2 of any type.
type seqStream2[L any, R any] struct {
	seq2 iter.Seq2[L, R]
}

// Seq2 returns the sequence of the current data stream.
func (it *seqStream2[L, R]) Seq2() iter.Seq2[L, R] {
	return it.seq2
}

// finiteSizeStream represents a finite stream with a known, fixed size.
// seq2 holds the sequence of elements in the stream.
// size denotes the number of elements in the finite stream.
type finiteStream2WithSize[L, R any] struct {
	seq2 iter.Seq2[L, R]
	size int
}

// Seq2 returns the internal sequence of the finite size stream.
func (it *finiteStream2WithSize[L, R]) Seq2() iter.Seq2[L, R] {
	return it.seq2
}

// Size returns the size of the finiteSizeStream.
func (it *finiteStream2WithSize[L, R]) Size() int {
	return it.size
}

// Finite returns true indicating that the stream has a finite size.
func (it *finiteStream2WithSize[L, R]) Finite() bool {
	return true
}

// finiteSeqStream represents a finite sequence stream of elements of any type.
type finiteStream2[L, R any] struct {
	seq2 iter.Seq2[L, R]
}

// Seq2 returns the underlying sequence of type iter.Seq2[L,R] from the finiteStream2 instance.
func (it *finiteStream2[L, R]) Seq2() iter.Seq2[L, R] {
	return it.seq2
}

// Finite returns true indicating that the stream has a finite size.
func (it *finiteStream2[L, R]) Finite() bool {
	return true
}

// mapStream represents a finite stream backed by a map of key/value pairs.
type mapStream[K comparable, V any] struct {
	inner map[K]V
}

// Finite returns true indicating that the stream has a finite size.
func (it *mapStream[K, V]) Finite() bool {
	return true
}

// Size returns the number of elements in the sliceStream.
func (it *mapStream[K, V]) Size() int {
	return len(it.inner)
}

func (it *mapStream[K, V]) Seq2() iter.Seq2[K, V] {
	return func(yield func(K, V) bool) {
		for k, v := range it.inner {
			if !yield(k, v) {
				return
			}
		}
	}
}
