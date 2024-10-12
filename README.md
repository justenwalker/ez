# ez

`ez` makes it easy to chain together stream operations.

This library declares a `Stream` interface and implementations for finite, infinite sequences, as well as slices.

## Usage

Convert a Seq or a Slice to a `Stream`

```go
// Create an infinite stream of values
fibonacci := ez.InfiniteStream(func(yield func(int) bool) {
    a := 0
    if !yield(a) {
        return
    }
    b := 1
    for yield(b) {
        a, b = b, b+a
    }
})

// or create a finite stream
oneToTen := ez.FiniteStream(func(yield func(int) bool) {
    for i := range 10 {
        if !yield(i + 1) {
            return
        }
    }
})

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

// Concatenate several streams together:
concat := ez.Concat(
    oneToTen,
    sliceStream,
    ez.Pipe(fibonacci, ez.Take[int](10)),
)

// Reduce a stream to a single value
sum := ez.Reduce(concat, 0, func(acc int, v int) int { return acc + v })
```
