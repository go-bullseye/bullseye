/*
Package bullseye provides an implementation of a DataFrame using Apache Arrow.

Basics

The DataFrame is an immutable heterogeneous tabular data structure with labeled columns.
It stores it's raw bytes using a provided Arrow Allocator by using the fundamental data
structure of Array (columns), which holds a sequence of values of the same type. An array
consists of memory holding the data and an additional validity bitmap that indicates if
the corresponding entry in the array is valid (not null).

Any DataFrames created should be released using Release() to decrement the reference
and free up the memory managed by the Arrow implementation.

Getting Started

Look in the dataframe package to get started.
*/
package bullseye