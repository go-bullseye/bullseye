/*
Package iterator provides iterators for chunks and values.

Since Arrow can store chunks larger than tha max int64 (9223372036854775807) due to how it
store chunks, it's best to use iterators to iterate over chunks and their values.

There are generic ChunkIterator and ValueIterator implementations as well as specific
generated Arrow types for each of them, i.e. Float64ChunkIterator and Float64ValueIterator.

*/
package iterator
