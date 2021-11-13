package pump

import (
	ds "github.com/ipfs/go-datastore"
	dsq "github.com/ipfs/go-datastore/query"
)

type BlockInfo struct {
	Error error
	Key   ds.Key
	Entry dsq.Entry
}

// An Enumerator is able to enumerate the blocks from a source
type Enumerator interface {
	// TotalCount return the total number of existing blocks,
	// or -1 if unknown/unsupported.
	TotalCount() int

	// Keys emit in the given channel each Key existing in the source
	Keys(out chan<- BlockInfo) error
}

type Block struct {
	Error error
	Key   ds.Key
	Data  []byte
}

// A Collector is able to read a block from a source
type Collector interface {
	// Blocks read each CID from the input, retrieve the corresponding
	// block and emit it to the output
	Blocks(in <-chan BlockInfo, out chan<- Block) error
}

// A Drain is able to write a block to a destination
type Drain interface {
	Drain(block Block) error
}

type CountedDrain interface {
	Drain
	SuccessfulBlocksCount() uint64
}
