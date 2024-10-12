// Copyright 2024, Justen Walker
// SPDX-License-Identifier: Apache-2.0

package ez_test

import (
	"errors"
	"fmt"
	"github.com/justenwalker/ez"
	"math"
	"math/rand/v2"
	"slices"
	"testing"
)

// Example demonstrates the usage of various stream operations such as filtering, mapping, skipping, and taking elements from streams.
func Example() {
	// or convert a slice to a stream
	sliceStream := ez.SliceStream([]int{1, 2, 3, 4, 5, 6, 7})

	// Pipe stream operations together with the `Pipe` function
	intStream := ez.Pipe(fibonacci,
		ez.Filter(func(i int) bool { return i > 5 }),
		ez.Map(func(i int) int { return i / 2 }),
		ez.Skip[int](10),
		ez.Take[int](10),
		ez.Reverse[int],
	)

	// Convert from a stream of one type to another
	float64Stream := ez.Convert(intStream, func(i int) float64 {
		return math.Round(math.Sqrt(float64(i)))
	})

	// Collect values into a slice from a Finite Stream.
	values := ez.Collect(float64Stream)
	fmt.Println("values:", values)

	// Concatenate several streams together:
	concat := ez.Concat(
		oneToTen,
		sliceStream,
		ez.Pipe(fibonacci, ez.Take[int](10)),
	)

	// Reduce a stream to a single value
	sum := ez.Reduce(concat, 0, func(acc int, v int) int { return acc + v })
	fmt.Println("sum:", sum)
	// Output:
	// values: [194 152 120 94 74 58 46 36 28 22]
	// sum: 171
}

// Example_complex demonstrates complex stream transformations including filtering, mapping, and operations like shuffle,
// reverse, distinct, sortOrdered, and reduce using a sequence generated from the Fibonacci series.
func Example_complex() {
	rng := rand.New(rand.NewChaCha8([32]byte{}))
	stream := ez.Convert(
		ez.Pipe(fibonacci,
			ez.Filter(func(i int) bool {
				return i > 5
			}),
			ez.Map(func(i int) int {
				return i / 2
			}),
			ez.Skip[int](10),
			ez.Take[int](10),
			ez.ShuffleWithRand[int](rng),
			ez.Reverse[int],
		), func(i int) float64 {
			return math.Round(math.Sqrt(float64(i)) * math.Sin(float64(i)))
		})
	values := ez.Collect(stream)
	fmt.Println("values:", values)
	sorted := ez.Collect(ez.Pipe(ez.SliceStream(values), ez.SortOrdered[float64]))
	fmt.Println("sorted:", sorted)
	distinct := ez.Collect(ez.Pipe(ez.SliceStream(sorted), ez.Distinct[float64]))
	fmt.Println("distinct:", distinct)
	sum := ez.Reduce(stream, 0, func(a float64, b float64) float64 {
		return a + b
	})
	fmt.Println("sum:", sum)
	// Output:
	// values: [86 -34 -124 86 25 1 190 -26 5 58]
	// sorted: [-124 -34 -26 1 5 25 58 86 86 190]
	// distinct: [-124 -34 -26 1 5 25 58 86 190]
	// sum: 267
}

func TestMap(t *testing.T) {
	tests := []struct {
		name     string
		in       ez.Stream[int]
		mapper   func(int) float64
		expected []float64
	}{
		{
			name:     "square root",
			in:       ez.Pipe(fibonacci, ez.Take[int](5)),
			mapper:   func(i int) float64 { return math.Sqrt(float64(i)) },
			expected: []float64{0, 1, 1, 1.4142135623730951, 1.7320508075688772},
		},
		{
			name:     "double",
			in:       ez.Pipe(fibonacci, ez.Take[int](5)),
			mapper:   func(i int) float64 { return float64(i * 2) },
			expected: []float64{0, 2, 2, 4, 6},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ez.Collect(ez.Convert(tt.in, tt.mapper))
			for i := range result {
				if math.Abs(result[i]-tt.expected[i]) > 1e-9 {
					t.Errorf("expected %v, got %v", tt.expected, result)
				}
			}
		})
	}
}

func TestReverse(t *testing.T) {
	tests := []struct {
		name     string
		in       ez.Stream[int]
		expected []int
	}{
		{
			name:     "reverse",
			in:       ez.Pipe(fibonacci, ez.Take[int](5)),
			expected: []int{3, 2, 1, 1, 0},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ez.Collect(ez.Pipe(tt.in, ez.Reverse[int]))
			if slices.Compare(tt.expected, result) != 0 {
				t.Errorf("expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestShuffle(t *testing.T) {
	tests := []struct {
		name string
		in   ez.Stream[int]
	}{
		{
			name: "shuffle",
			in:   ez.Pipe(fibonacci, ez.Take[int](10)),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			before := ez.Collect(tt.in)
			result := ez.Collect(ez.Pipe(fibonacci, ez.Take[int](10), ez.Shuffle[int]))
			if slices.Compare(before, result) == 0 {
				t.Errorf("expected shuffled, but it is identical")
			}
		})
	}
}

func TestTake(t *testing.T) {
	tests := []struct {
		name     string
		n        int
		in       ez.Stream[int]
		expected []int
	}{
		{
			name:     "take_5",
			n:        5,
			in:       fibonacci,
			expected: []int{0, 1, 1, 2, 3},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ez.Collect(ez.Pipe(tt.in, ez.Take[int](tt.n)))
			if slices.Compare(tt.expected, result) != 0 {
				t.Errorf("expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestSkip(t *testing.T) {
	tests := []struct {
		name     string
		n        int
		in       ez.Stream[int]
		expected []int
	}{
		{
			name:     "skip_none",
			n:        0,
			in:       ez.Pipe(fibonacci, ez.Take[int](5)),
			expected: []int{0, 1, 1, 2, 3},
		},
		{
			name:     "slip_2",
			n:        2,
			in:       ez.Pipe(fibonacci, ez.Take[int](5)),
			expected: []int{1, 2, 3},
		},
		{
			name:     "slip_finite_5",
			n:        5,
			in:       oneToTen,
			expected: []int{6, 7, 8, 9, 10},
		},
		{
			name:     "skip_all_elements",
			n:        5,
			in:       ez.Pipe(fibonacci, ez.Take[int](5)),
			expected: []int{},
		},
		{
			name:     "overskip_all_elements",
			n:        10,
			in:       ez.Pipe(fibonacci, ez.Take[int](5)),
			expected: []int{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ez.Collect(ez.Pipe(tt.in, ez.Skip[int](tt.n)))
			if slices.Compare(tt.expected, result) != 0 {
				t.Errorf("expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestFilterApply(t *testing.T) {
	tests := []struct {
		name       string
		filterFunc func(int) bool
		applyFunc  func(int) int
		in         ez.Stream[int]
		expected   []int
	}{
		{
			name:       "filter and apply",
			filterFunc: func(i int) bool { return i > 5 },
			applyFunc:  func(i int) int { return i / 2 },
			in:         ez.Pipe(fibonacci, ez.Take[int](10)),
			expected:   []int{4, 6, 10, 17},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ez.Collect(ez.Pipe(tt.in, ez.Filter(tt.filterFunc), ez.Map(tt.applyFunc)))
			if slices.Compare(tt.expected, result) != 0 {
				t.Errorf("expected %v, got %v", tt.expected, result)
			}
		})
	}
}

var (
	oneToTen = ez.FiniteStream(func(yield func(int) bool) {
		for i := range 10 {
			if !yield(i + 1) {
				return
			}
		}
	})

	fibonacci = ez.InfiniteStream(func(yield func(int) bool) {
		a := 0
		if !yield(a) {
			return
		}
		b := 1
		for yield(b) {
			a, b = b, b+a
		}
	})
)

func TestFilter(t *testing.T) {
	tests := []struct {
		name     string
		in       ez.Stream[int]
		filter   func(int) bool
		expected []int
	}{
		{
			name:     "filter",
			in:       ez.Pipe(fibonacci, ez.Take[int](15)),
			filter:   func(i int) bool { return i > 5 && i < 200 },
			expected: []int{8, 13, 21, 34, 55, 89, 144},
		},
		{
			name:     "filter_single",
			in:       ez.Pipe(fibonacci, ez.Take[int](10)),
			filter:   func(i int) bool { return i < 1 },
			expected: []int{0},
		},
		{
			name:     "filter_none",
			in:       ez.Pipe(fibonacci, ez.Take[int](10)),
			filter:   func(i int) bool { return i > 1000 },
			expected: nil,
		},
		{
			name:     "filter_all",
			in:       ez.Pipe(fibonacci, ez.Take[int](10)),
			filter:   func(i int) bool { return i < 1000 },
			expected: []int{0, 1, 1, 2, 3, 5, 8, 13, 21, 34},
		},
		{
			name:     "filter_even",
			in:       ez.Pipe(fibonacci, ez.Take[int](10)),
			filter:   func(i int) bool { return i%2 == 0 },
			expected: []int{0, 2, 8, 34},
		},
		{
			name:     "filter_odd",
			in:       ez.Pipe(fibonacci, ez.Take[int](10)),
			filter:   func(i int) bool { return i%2 != 0 },
			expected: []int{1, 1, 3, 5, 13, 21},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ez.Collect(ez.Pipe(tt.in, ez.Filter(tt.filter)))
			if slices.Compare(tt.expected, result) != 0 {
				t.Errorf("expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestInfiniteStream(t *testing.T) {
	t.Run("collect_panics", func(t *testing.T) {
		defer expectPanicError(t, ez.ErrInfiniteSequence)
		ez.Collect(fibonacci)
	})
	t.Run("concat_infinite", func(t *testing.T) {
		defer expectPanicError(t, ez.ErrInfiniteSequence)
		ez.Collect(
			ez.Concat(
				fibonacci,
				fibonacci,
			),
		)
	})
	t.Run("map_infinite", func(t *testing.T) {
		defer expectPanicError(t, ez.ErrInfiniteSequence)
		ez.Collect(
			ez.Convert(fibonacci,
				func(v int) float64 {
					return float64(v)
				},
			),
		)
	})
}

func expectPanicError(t *testing.T, err error) {
	t.Helper()
	r := recover()
	if r == nil {
		t.Fatalf("expected panic")
	}
	panicErr, ok := r.(error)
	if !ok {
		t.Fatalf("expected panic error")
	}
	if !errors.Is(panicErr, err) {
		t.Fatalf("expected panic error %v, got %v", err, panicErr)
	}
}

func TestPipe(t *testing.T) {
	tests := []struct {
		name     string
		in       ez.Stream[int]
		pipes    []func(ez.Stream[int]) ez.Stream[int]
		expected []int
	}{
		{
			name:     "pipe_take",
			in:       fibonacci,
			pipes:    []func(ez.Stream[int]) ez.Stream[int]{ez.Take[int](5)},
			expected: []int{0, 1, 1, 2, 3},
		},
		{
			name:     "pipe_skip_take",
			in:       fibonacci,
			pipes:    []func(ez.Stream[int]) ez.Stream[int]{ez.Skip[int](2), ez.Take[int](5)},
			expected: []int{1, 2, 3, 5, 8},
		},
		{
			name:     "pipe_filter",
			in:       fibonacci,
			pipes:    []func(ez.Stream[int]) ez.Stream[int]{ez.Filter(func(i int) bool { return i%2 == 0 }), ez.Take[int](5)},
			expected: []int{0, 2, 8, 34, 144},
		},
		{
			name:     "pipe_filter_apply",
			in:       fibonacci,
			pipes:    []func(ez.Stream[int]) ez.Stream[int]{ez.Filter(func(i int) bool { return i > 5 }), ez.Map(func(i int) int { return i / 2 }), ez.Take[int](5)},
			expected: []int{4, 6, 10, 17, 27},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stream := ez.Pipe(tt.in, tt.pipes...)
			result := ez.Collect(stream)
			if slices.Compare(tt.expected, result) != 0 {
				t.Errorf("expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestConcat(t *testing.T) {
	tests := []struct {
		name     string
		streams  []ez.Stream[int]
		expected []int
	}{
		{
			name: "concat_two_sized_streams",
			streams: []ez.Stream[int]{
				ez.Pipe(fibonacci, ez.Take[int](3)),
				ez.Pipe(fibonacci, ez.Take[int](3)),
			},
			expected: []int{0, 1, 1, 0, 1, 1},
		},
		{
			name: "concat_two_unsized_streams",
			streams: []ez.Stream[int]{
				oneToTen,
				ez.Pipe(oneToTen, ez.Map[int](func(a int) int {
					return a + 10
				})),
			},
			expected: []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20},
		},
		{
			name: "concat_empty_with_non_empty",
			streams: []ez.Stream[int]{
				ez.SliceStream[int](nil),
				ez.Pipe(fibonacci, ez.Take[int](3)),
			},
			expected: []int{0, 1, 1},
		},
		{
			name: "concat_multiple_streams",
			streams: []ez.Stream[int]{
				ez.Pipe(fibonacci, ez.Take[int](2)),
				ez.Pipe(fibonacci, ez.Take[int](2)),
				ez.Pipe(fibonacci, ez.Take[int](2)),
			},
			expected: []int{0, 1, 0, 1, 0, 1},
		},
		{
			name: "concat_empty_streams",
			streams: []ez.Stream[int]{
				ez.SliceStream[int](nil),
				ez.SliceStream[int](nil),
			},
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			concatenated := ez.Concat(tt.streams...)
			result := ez.Collect(concatenated)
			if slices.Compare(tt.expected, result) != 0 {
				t.Errorf("expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestAssumeFinite(t *testing.T) {
	f1 := ez.AssumeFinite(fibonacci)
	if !ez.IsFinite(f1) {
		t.Errorf("expected AssumeFinite to produce a finite stream")
	}
	if f1 == fibonacci {
		t.Errorf("expected AssumeFinite to produce a stream NOT equal to the original")
	}
	f2 := ez.AssumeFinite(oneToTen)
	if !ez.IsFinite(f2) {
		t.Errorf("expected AssumeFinite to produce a finite stream")
	}
	if f2 != oneToTen {
		t.Errorf("expected AssumeFinite to produce a stream equal to the original")
	}
	sliceStream := ez.SliceStream([]int{1, 2, 3})
	f3 := ez.AssumeFinite(sliceStream)
	if !ez.IsFinite(f2) {
		t.Errorf("expected AssumeFinite to produce a finite stream")
	}
	if f3 != sliceStream {
		t.Errorf("expected AssumeFinite to produce a stream equal to the original")
	}
}

func TestBreak(t *testing.T) {
	tests := []struct {
		name     string
		stream   ez.Stream[int]
		expected []int
	}{
		{
			name:     "fibbonacci",
			stream:   fibonacci,
			expected: []int{0, 1, 1, 2, 3},
		},
		{
			name:     "one-to-ten",
			stream:   oneToTen,
			expected: []int{1, 2, 3, 4, 5},
		},
		{
			name:     "slice-of-ints",
			stream:   ez.SliceStream([]int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}),
			expected: []int{1, 2, 3, 4, 5},
		},
		{
			name: "concat-sequence",
			stream: ez.Concat(
				ez.Pipe(oneToTen, ez.Take[int](3)),
				ez.Pipe(oneToTen, ez.Take[int](3)),
			),
			expected: []int{1, 2, 3, 1, 2},
		},
		{
			name: "apply-sequence",
			stream: ez.Pipe(oneToTen, ez.Map(func(v int) int {
				return v * 2
			})),
			expected: []int{2, 4, 6, 8, 10},
		},
		{
			name: "map-sequence",
			stream: ez.Convert(ez.Convert(oneToTen, func(v int) float64 {
				return float64(v) * 0.5
			}), func(v float64) int {
				return int(v)
			}),
			expected: []int{0, 1, 1, 2, 2},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var result []int
			i := 0
			for v := range tt.stream.Seq() {
				result = append(result, v)
				i++
				if i >= 5 {
					break
				}
			}
			if slices.Compare(tt.expected, result) != 0 {
				t.Errorf("expected %v, got %v", tt.expected, result)
			}
		})
	}
}
