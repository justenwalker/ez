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

// Example Streams
var (
	countingNumbers = ez.InfiniteStream(func(yield func(int) bool) {
		i := 0
		for {
			if !yield(i) {
				return
			}
			i++
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
	fibonacciWithIndex = ez.InfiniteStream2(func(yield func(int, int) bool) {
		i := 0
		a := 0
		if !yield(i, a) {
			return
		}
		i++
		b := 1
		for yield(i, b) {
			a, b = b, b+a
			i++
		}
	})
	oneToTen = ez.FiniteStream(func(yield func(int) bool) {
		for i := range 10 {
			if !yield(i + 1) {
				return
			}
		}
	})
	oneToTenWithIndex = ez.FiniteStream2(func(yield func(int, int) bool) {
		for i := range 10 {
			if !yield(i, i+1) {
				return
			}
		}
	})
)

// Example demonstrates the usage of various stream operations such as filtering, mapping, skipping, and taking elements from streams.
func Example_stream() {
	// convert a slice to a stream
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

	// Collect m into a slice from a Finite Stream.
	values := ez.Collect(float64Stream)
	fmt.Println("m:", values)

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
	// m: [194 152 120 94 74 58 46 36 28 22]
	// sum: 171
}

// Example demonstrates the usage of various stream operations such as filtering, mapping, skipping, and taking elements from streams.
func Example_stream2() {
	zipStream2 := ez.Zip(
		ez.InfiniteStream(func(yield func(int) bool) {
			i := 0
			for {
				if !yield(i) {
					return
				}
				i++
			}
		}),
		fibonacci,
	)

	// Pipe stream operations together with the `Pipe` function
	intStream := ez.Pipe2(zipStream2,
		ez.Filter2(func(l, r int) bool { return r > 5 }),
		ez.Map2(func(l, r int) (int, int) { return l, r * 2 }),
		ez.Skip2[int, int](10),
		ez.Take2[int, int](10),
		ez.Reverse2[int, int],
	)

	// Convert from a stream of one type to another
	float64Stream := ez.Convert2(intStream, func(l, r int) (int, float64) {
		return l, math.Round(math.Sqrt(float64(r)))
	})

	// Collect m into a slice from a Finite Stream.
	leftValues, rightValues := ez.Collect2(float64Stream)
	fmt.Println("left:", leftValues)
	fmt.Println("right:", rightValues)

	// Concatenate several streams together:
	concat := ez.Concat2(
		ez.Pipe2(zipStream2, ez.Take2[int, int](3)),
		ez.Pipe2(zipStream2, ez.Skip2[int, int](3), ez.Take2[int, int](3)),
		ez.Pipe2(zipStream2, ez.Skip2[int, int](6), ez.Take2[int, int](3)),
	)

	// Reduce a stream to a single value
	leftSum := ez.Reduce(ez.Left(concat), 0, func(acc int, v int) int { return acc + v })
	fmt.Println("leftSum:", leftSum)
	rightSum := ez.Reduce(ez.Right(concat), 0, func(acc int, v int) int { return acc + v })
	fmt.Println("rightSum:", rightSum)
	// Output:
	// left: [25 24 23 22 21 20 19 18 17 16]
	// right: [387 305 239 188 148 116 91 72 57 44]
	// leftSum: 36
	// rightSum: 54
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
	fmt.Println("m:", values)
	sorted := ez.Collect(ez.Pipe(ez.SliceStream(values), ez.Ordered[float64]))
	fmt.Println("sorted:", sorted)
	distinct := ez.Collect(ez.Pipe(ez.SliceStream(sorted), ez.Distinct[float64]))
	fmt.Println("distinct:", distinct)
	sum := ez.Reduce(stream, 0, func(a float64, b float64) float64 {
		return a + b
	})
	fmt.Println("sum:", sum)
	// Output:
	// m: [86 -34 -124 86 25 1 190 -26 5 58]
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

func TestMap2(t *testing.T) {
	tests := []struct {
		name          string
		in            ez.Stream2[int, int]
		mapper        func(int, int) (int, float64)
		expectedLeft  []int
		expectedRight []float64
	}{
		{
			name: "add_and_square",
			in: ez.ZipSliceStream2(
				[]int{1, 2, 3},
				[]int{4, 5, 6},
			),
			mapper: func(a int, b int) (int, float64) {
				return a + b, float64((a + b) * (a + b))
			},
			expectedLeft:  []int{5, 7, 9},
			expectedRight: []float64{25, 49, 81},
		},
		{
			name: "subtract_and_double",
			in: ez.ZipSliceStream2(
				[]int{10, 20, 30},
				[]int{1, 2, 3},
			),
			mapper: func(a int, b int) (int, float64) {
				return a - b, float64((a - b) * 2)
			},
			expectedLeft:  []int{9, 18, 27},
			expectedRight: []float64{18, 36, 54},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resultLeft, resultRight := ez.Collect2(ez.Convert2(tt.in, tt.mapper))
			for i := range resultLeft {
				if resultLeft[i] != tt.expectedLeft[i] {
					t.Errorf("left: expected %v, got %v", tt.expectedLeft, resultLeft)
				}
			}
			for i := range resultRight {
				if math.Abs(resultRight[i]-tt.expectedRight[i]) > 1e-9 {
					t.Errorf("right: expected %v, got %v", tt.expectedRight, resultRight)
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

func TestShuffle2(t *testing.T) {
	tests := []struct {
		name string
		in   ez.Stream2[int, int]
	}{
		{
			name: "shuffle",
			in:   ez.Pipe2(fibonacciWithIndex, ez.Take2[int, int](10)),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l1, r1 := ez.Collect2(tt.in)
			l2, r2 := ez.Collect2(ez.Pipe2(fibonacciWithIndex, ez.Take2[int, int](10), ez.Shuffle2[int, int]))
			if slices.Compare(l1, l2) == 0 {
				t.Errorf("expected shuffled, but it is identical")
			}
			if slices.Compare(r1, r2) == 0 {
				t.Errorf("expected shuffled, but it is identical")
			}
		})
	}
}

func TestTake2(t *testing.T) {
	tests := []struct {
		name          string
		n             int
		in            ez.Stream2[int, int]
		expectedLeft  []int
		expectedRight []int
	}{
		{
			name: "take_2_pairs",
			n:    2,
			in: ez.ZipSliceStream2(
				[]int{1, 2, 3, 4},
				[]int{5, 6, 7, 8},
			),
			expectedLeft:  []int{1, 2},
			expectedRight: []int{5, 6},
		},
		{
			name: "take_1_pair",
			n:    1,
			in: ez.ZipSliceStream2(
				[]int{9, 10, 11},
				[]int{12, 13, 14},
			),
			expectedLeft:  []int{9},
			expectedRight: []int{12},
		},
		{
			name: "take_0_pairs",
			n:    0,
			in: ez.ZipSliceStream2(
				[]int{15, 16, 17},
				[]int{18, 19, 20},
			),
			expectedLeft:  []int{},
			expectedRight: []int{},
		},
		{
			name: "take_more_than_exists",
			n:    5,
			in: ez.ZipSliceStream2(
				[]int{21, 22},
				[]int{23, 24},
			),
			expectedLeft:  []int{21, 22},
			expectedRight: []int{23, 24},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resultLeft, resultRight := ez.Collect2(ez.Pipe2(tt.in, ez.Take2[int, int](tt.n)))
			if slices.Compare(tt.expectedLeft, resultLeft) != 0 {
				t.Errorf("left: expected %v, got %v", tt.expectedLeft, resultLeft)
			}
			if slices.Compare(tt.expectedRight, resultRight) != 0 {
				t.Errorf("right: expected %v, got %v", tt.expectedRight, resultRight)
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
			name:     "take_5_fibonacci",
			n:        5,
			in:       fibonacci,
			expected: []int{0, 1, 1, 2, 3},
		},
		{
			name:     "take_none",
			n:        0,
			in:       ez.SliceStream([]int{1, 2, 3, 4, 5}),
			expected: []int{},
		},
		{
			name:     "take_more_than_exists",
			n:        15,
			in:       ez.SliceStream([]int{1, 2, 3, 4, 5}),
			expected: []int{1, 2, 3, 4, 5},
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
			name:     "skip_2",
			n:        2,
			in:       ez.Pipe(fibonacci, ez.Take[int](5)),
			expected: []int{1, 2, 3},
		},
		{
			name:     "skip_finite_5",
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

func TestFilter(t *testing.T) {
	tests := []struct {
		name         string
		in           ez.Stream[int]
		filter       func(int) bool
		expected     []int
		expectSize   int
		expectFinite bool
	}{
		{
			name:         "filter_infinite",
			in:           fibonacci,
			filter:       func(i int) bool { return i > 5 && i < 10 },
			expected:     []int{8},
			expectSize:   -1,
			expectFinite: false,
		},
		{
			name:         "filter_finite",
			in:           ez.Pipe(oneToTen),
			filter:       func(i int) bool { return i > 5 && i < 10 },
			expected:     []int{6, 7, 8, 9},
			expectSize:   -1,
			expectFinite: true,
		},
		{
			name:         "filter_single",
			in:           oneToTen,
			filter:       func(i int) bool { return i <= 1 },
			expected:     []int{1},
			expectSize:   -1,
			expectFinite: true,
		},
		{
			name:         "filter_none",
			in:           oneToTen,
			filter:       func(i int) bool { return i <= 10 },
			expected:     []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
			expectSize:   -1,
			expectFinite: true,
		},
		{
			name:         "filter_all",
			in:           oneToTen,
			filter:       func(i int) bool { return i > 10 },
			expected:     nil,
			expectSize:   -1,
			expectFinite: true,
		},
		{
			name:         "filter_even",
			in:           oneToTen,
			filter:       func(i int) bool { return i%2 == 0 },
			expected:     []int{2, 4, 6, 8, 10},
			expectSize:   -1,
			expectFinite: true,
		},
		{
			name:         "filter_odd",
			in:           oneToTen,
			filter:       func(i int) bool { return i%2 != 0 },
			expected:     []int{1, 3, 5, 7, 9},
			expectSize:   -1,
			expectFinite: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filtered := ez.Pipe(tt.in, ez.Filter(tt.filter))
			size := ez.Size(filtered)
			finite := ez.IsFinite(filtered)
			if tt.expectSize != size {
				t.Errorf("expected size %v, got %v", tt.expectSize, size)
			}
			if tt.expectFinite != finite {
				t.Errorf("expected finite %v, got %v", tt.expectFinite, finite)
			}
			result := ez.Collect(ez.Pipe(tt.in, ez.Take[int](1000), ez.Filter(tt.filter)))
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

func TestConcat2(t *testing.T) {
	tests := []struct {
		name          string
		streams       []ez.Stream2[int, int]
		expectFinite  bool
		expectSize    int
		expectedLeft  []int
		expectedRight []int
	}{
		{
			name: "concat_two_unsized_streams",
			streams: []ez.Stream2[int, int]{
				fibonacciWithIndex,
				fibonacciWithIndex,
			},
			expectFinite:  false,
			expectSize:    -1,
			expectedLeft:  []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19},
			expectedRight: []int{0, 1, 1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233, 377, 610, 987, 1597, 2584, 4181},
		},
		{
			name: "concat_two_sized_streams",
			streams: []ez.Stream2[int, int]{
				oneToTenWithIndex,
				ez.Pipe2(oneToTenWithIndex, ez.Map2[int, int](func(l, r int) (int, int) {
					return l, r + 10
				})),
			},
			expectFinite:  true,
			expectSize:    -1,
			expectedLeft:  []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
			expectedRight: []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20},
		},
		{
			name: "concat_empty_with_non_empty",
			streams: []ez.Stream2[int, int]{
				ez.ZipSliceStream2[int, int](nil, nil),
				ez.Pipe2(fibonacciWithIndex, ez.Take2[int, int](3)),
			},
			expectFinite:  true,
			expectSize:    3,
			expectedLeft:  []int{0, 1, 2},
			expectedRight: []int{0, 1, 1},
		},
		{
			name: "concat_multiple_streams",
			streams: []ez.Stream2[int, int]{
				ez.Pipe2(fibonacciWithIndex, ez.Take2[int, int](3)),
				ez.Pipe2(fibonacciWithIndex, ez.Take2[int, int](3)),
				ez.Pipe2(fibonacciWithIndex, ez.Take2[int, int](3)),
			},
			expectFinite:  true,
			expectSize:    9,
			expectedLeft:  []int{0, 1, 2, 0, 1, 2, 0, 1, 2},
			expectedRight: []int{0, 1, 1, 0, 1, 1, 0, 1, 1},
		},
		{
			name: "concat_empty_streams",
			streams: []ez.Stream2[int, int]{
				ez.ZipSliceStream2[int, int](nil, nil),
				ez.ZipSliceStream2[int, int](nil, nil),
			},
			expectSize:    0,
			expectFinite:  true,
			expectedLeft:  nil,
			expectedRight: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			concat := ez.Concat2(tt.streams...)
			size := ez.Size(concat)
			finite := ez.IsFinite(concat)
			if tt.expectSize != size {
				t.Errorf("expected size %v, got %v", tt.expectSize, size)
			}
			if tt.expectFinite != finite {
				t.Errorf("expected finite %v, got %v", tt.expectFinite, finite)
			}
			resultLeft, resultRight := ez.Collect2(ez.Pipe2(concat, ez.Take2[int, int](20)))
			if slices.Compare(tt.expectedLeft, resultLeft) != 0 {
				t.Errorf("left: expected %v, got %v", tt.expectedLeft, resultLeft)
			}
			if slices.Compare(tt.expectedRight, resultRight) != 0 {
				t.Errorf("right: expected %v, got %v", tt.expectedRight, resultRight)
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

func TestAssumeFinite2(t *testing.T) {
	f1 := ez.AssumeFinite2(fibonacciWithIndex)
	if !ez.IsFinite(f1) {
		t.Errorf("expected AssumeFinite2 to produce a finite stream")
	}
	if f1 == fibonacciWithIndex {
		t.Errorf("expected AssumeFinite2 to produce a stream NOT equal to the original")
	}
	f2 := ez.AssumeFinite2(oneToTenWithIndex)
	if !ez.IsFinite(f2) {
		t.Errorf("expected AssumeFinite2 to produce a finite stream")
	}
	if f2 != oneToTenWithIndex {
		t.Errorf("expected AssumeFinite2 to produce a stream equal to the original")
	}
	mapStream := ez.MapKVStream(map[int]int{0: 1, 1: 2, 2: 3})
	f3 := ez.AssumeFinite2(mapStream)
	if !ez.IsFinite(f2) {
		t.Errorf("expected AssumeFinite to produce a finite stream")
	}
	if f3 != mapStream {
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
			name: "map-sequence",
			stream: ez.Pipe(oneToTen, ez.Map(func(v int) int {
				return v * 2
			})),
			expected: []int{2, 4, 6, 8, 10},
		},
		{
			name: "convert-sequence",
			stream: ez.Convert(ez.Convert(oneToTen, func(v int) float64 {
				return float64(v) * 0.5
			}), func(v float64) int {
				return int(v)
			}),
			expected: []int{0, 1, 1, 2, 2},
		},
		{
			name:     "left",
			stream:   ez.Left(oneToTenWithIndex),
			expected: []int{0, 1, 2, 3, 4},
		},
		{
			name:     "right",
			stream:   ez.Right(oneToTenWithIndex),
			expected: []int{1, 2, 3, 4, 5},
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

func TestBreak2(t *testing.T) {
	tests := []struct {
		name          string
		stream        ez.Stream2[int, int]
		expectedLeft  []int
		expectedRight []int
	}{
		{
			name:          "fibonacciWithIndex",
			stream:        fibonacciWithIndex,
			expectedLeft:  []int{0, 1, 2, 3, 4},
			expectedRight: []int{0, 1, 1, 2, 3},
		},
		{
			name:          "oneToTenWithIndex",
			stream:        oneToTenWithIndex,
			expectedLeft:  []int{0, 1, 2, 3, 4},
			expectedRight: []int{1, 2, 3, 4, 5},
		},
		{
			name: "slice-of-ints",
			stream: ez.ZipSliceStream2(
				[]int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
				[]int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
			),
			expectedLeft:  []int{0, 1, 2, 3, 4},
			expectedRight: []int{1, 2, 3, 4, 5},
		},
		{
			name: "concat-sequence",
			stream: ez.Concat2(
				ez.Pipe2(oneToTenWithIndex, ez.Take2[int, int](3)),
				ez.Pipe2(oneToTenWithIndex, ez.Take2[int, int](3)),
			),
			expectedLeft:  []int{0, 1, 2, 0, 1},
			expectedRight: []int{1, 2, 3, 1, 2},
		},
		{
			name: "apply-sequence",
			stream: ez.Pipe2(oneToTenWithIndex, ez.Map2(func(l, r int) (int, int) {
				return l, r * 2
			})),
			expectedLeft:  []int{0, 1, 2, 3, 4},
			expectedRight: []int{2, 4, 6, 8, 10},
		},
		{
			name: "map-sequence",
			stream: ez.Convert2(ez.Convert2(oneToTenWithIndex, func(l, r int) (int, float64) {
				return l, float64(r) * 0.5
			}), func(l int, r float64) (int, int) {
				return l, int(r)
			}),
			expectedLeft:  []int{0, 1, 2, 3, 4},
			expectedRight: []int{0, 1, 1, 2, 2},
		},
		{
			name:   "map-stream",
			stream: ez.MapKVStreamOrdered(map[int]int{0: 1, 1: 2, 2: 3, 3: 4, 4: 5, 5: 6, 6: 7, 7: 8, 8: 9, 9: 10}),

			expectedLeft:  []int{0, 1, 2, 3, 4},
			expectedRight: []int{1, 2, 3, 4, 5},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var resultLeft []int
			var resultRight []int
			i := 0
			for l, r := range tt.stream.Seq2() {
				resultLeft = append(resultLeft, l)
				resultRight = append(resultRight, r)
				i++
				if i >= 5 {
					break
				}
			}
			if slices.Compare(tt.expectedLeft, resultLeft) != 0 {
				t.Errorf("left: expected %v, got %v", tt.expectedLeft, resultLeft)
			}
			if slices.Compare(tt.expectedRight, resultRight) != 0 {
				t.Errorf("right: expected %v, got %v", tt.expectedRight, resultRight)
			}
		})
	}
}

func TestSwap2(t *testing.T) {
	tests := []struct {
		name          string
		in            ez.Stream2[int, int]
		expectedLeft  []int
		expectedRight []int
	}{
		{
			name: "swap_slice",
			in: ez.ZipSliceStream2[int, int](
				[]int{1, 2, 3},
				[]int{4, 5, 6},
			),
			expectedLeft:  []int{4, 5, 6},
			expectedRight: []int{1, 2, 3},
		},
		{
			name:          "swap_finite",
			in:            oneToTenWithIndex,
			expectedLeft:  []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
			expectedRight: []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			name:          "swap_infinite",
			in:            fibonacciWithIndex,
			expectedLeft:  []int{0, 1, 1, 2, 3, 5, 8, 13, 21, 34},
			expectedRight: []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			swapped := ez.Swap2(tt.in)
			resultLeft, resultRight := ez.Collect2(ez.Take2[int, int](10)(swapped))
			if slices.Compare(tt.expectedLeft, resultLeft) != 0 {
				t.Errorf("left: expected %v, got %v", tt.expectedLeft, resultLeft)
			}
			if slices.Compare(tt.expectedRight, resultRight) != 0 {
				t.Errorf("right: expected %v, got %v", tt.expectedRight, resultRight)
			}
		})
	}
}

func TestConvertToMap(t *testing.T) {
	tests := []struct {
		name     string
		in       ez.Stream2[int, int]
		expected map[int]int
	}{
		{
			name:     "empty",
			in:       ez.MapKVStream(map[int]int{}),
			expected: nil,
		},
		{
			name:     "items",
			in:       ez.MapKVStream(map[int]int{1: 1, 2: 2, 3: 3}),
			expected: map[int]int{1: 1, 2: 2, 3: 3},
		},
		{
			name:     "one-to-ten-with-index",
			in:       oneToTenWithIndex,
			expected: map[int]int{0: 1, 1: 2, 2: 3, 3: 4, 4: 5, 5: 6, 6: 7, 7: 8, 8: 9, 9: 10},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			converted := ez.ConvertToMap(tt.in)
			if !mapsEqual(converted, tt.expected) {
				t.Errorf("converted map does not equal expected: want=%v, got=%v", tt.expected, converted)
			}
		})
	}
}

func mapsEqual[K comparable, V comparable](expected map[K]V, actual map[K]V) bool {
	if len(expected) != len(actual) {
		return false
	}
	for key, val := range expected {
		if actualVal, exists := actual[key]; !exists || actualVal != val {
			return false
		}
	}
	return true
}

func TestMapKVStreamOrdered(t *testing.T) {
	orderedStream := ez.MapKVStreamOrdered(map[int]int{0: 1, 1: 2, 2: 3, 3: 4, 4: 5})
	var i int
	for l, r := range orderedStream.Seq2() {
		if l != i {
			t.Errorf("expected l=%d, got l=%d", i, l)
		}
		if r != i+1 {
			t.Errorf("expected r=%d, got r=%d", i+1, r)
		}
		i++
	}
}

func TestMapKVStreamSorted(t *testing.T) {
	orderedStream := ez.MapKVStreamSorted(map[int]int{0: 1, 1: 2, 2: 3, 3: 4, 4: 5}, func(a, b int) int {
		return a - b
	})
	var i int
	for l, r := range orderedStream.Seq2() {
		if l != i {
			t.Errorf("expected l=%d, got l=%d", i, l)
		}
		if r != i+1 {
			t.Errorf("expected r=%d, got r=%d", i+1, r)
		}
		i++
	}
}
