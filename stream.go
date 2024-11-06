package ez

import "iter"

// Stream represents a generic interface for a stream of values.
type Stream[D any] interface {
	// Seq returns the internal data sequence.
	Seq() iter.Seq[D]
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

// AssumeFinite converts a given Stream into a Finite Stream.
// The caller guarantees that the stream given is finite.
// If the stream given is not finite, the behavior is undefined, but likely will result in a program hang/crash.
// If the given stream is already finite, ie: IsFinite(stream) == true, then this function returns its input unchanged.
func AssumeFinite[D any](stream Stream[D]) Stream[D] {
	if IsFinite(stream) {
		return stream
	}
	return &finiteSeqStream[D]{seq: stream.Seq()}
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

func (it *sliceStream[D]) Seq2() iter.Seq2[int, D] {
	return func(yield func(int, D) bool) {
		for i, v := range it.values {
			if !yield(i, v) {
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
