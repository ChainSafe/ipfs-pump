package pump

import (
	"crypto/rand"
	"fmt"
	"log"
	"sync"
	"sync/atomic"

	ds "github.com/ipfs/go-datastore"
	dshelp "github.com/ipfs/go-ipfs-ds-help"

	cid "github.com/ipfs/go-cid"
)

var _ Enumerator = &MockEnumerator{}
var _ Collector = &MockCollector{}
var _ Drain = &mockDrain{}

type MockEnumerator struct {
	blocks  *sync.Map
	count   int
	cidPref cid.Prefix
}

func newMockEnumerator(blocks *sync.Map, count int, cidPref cid.Prefix) *MockEnumerator {
	return &MockEnumerator{blocks: blocks, count: count, cidPref: cidPref}
}

func (m *MockEnumerator) TotalCount() int {
	return m.count
}

func (m *MockEnumerator) Keys(out chan<- BlockInfo) error {
	i := m.count

	go func() {
		defer close(out)

		for ; i > 0; i-- {
			data := make([]byte, 10000)

			_, err := rand.Read(data)
			if err != nil {
				log.Fatal(err)
			}

			c, err := m.cidPref.Sum(data)
			if err != nil {
				log.Fatal(err)
			}

			m.blocks.Store(c.String(), data)

			out <- BlockInfo{
				Key: dshelp.NewKeyFromBinary(c.Bytes()),
			}
		}
	}()

	return nil
}

type MockCollector struct {
	source *sync.Map
}

func NewMockCollector(source *sync.Map) *MockCollector {
	return &MockCollector{source: source}
}

func (m *MockCollector) Blocks(in <-chan BlockInfo, out chan<- Block) error {
	go func() {
		for info := range in {
			data, ok := m.source.Load(info.Key.String())
			if !ok {
				out <- Block{Key: info.Key, Error: fmt.Errorf("unknown block")}
				continue
			}

			out <- Block{
				Key:  info.Key,
				Data: data.([]byte),
			}
		}
		close(out)
	}()

	return nil
}

type mockDrain struct {
	Drained uint64
}

func newMockDrain() *mockDrain {
	return &mockDrain{}
}

func (m *mockDrain) Drain(block Block) error {
	atomic.AddUint64(&m.Drained, 1)
	return nil
}

type mockFailingDrain struct {
	Drained uint64

	// How many blocks we want the Drain() to simulate as failed
	BlocksToFail uint64
}

func newMockFailingDrain(blocksToFail uint64) *mockFailingDrain {
	return &mockFailingDrain{BlocksToFail: blocksToFail}
}

func (m *mockFailingDrain) Drain(block Block) error {
	atomic.AddUint64(&m.Drained, 1)

	if m.BlocksToFail > 0 {
		m.BlocksToFail--
		return fmt.Errorf("mocked s3 rate limit error, please slow down")
	}

	return nil
}

// mockCidPrefDrain has a Drain() function that verifies the CID coming from Enumerator is correctly deconstructed.
type mockCidPrefDrain struct {
	Drained uint64

	expCidPref cid.Prefix
}

func newMockCidPrefDrain(expCidPref cid.Prefix) *mockCidPrefDrain {
	return &mockCidPrefDrain{expCidPref: expCidPref}
}

func (m *mockCidPrefDrain) Drain(block Block) error {
	atomic.AddUint64(&m.Drained, 1)

	plainKey := ds.NewKey(block.Key.BaseNamespace())
	blockCID, err := dshelp.DsKeyToCidV1(plainKey, cid.DagProtobuf)
	if err != nil {
		return fmt.Errorf("error reconstructing cid from the key %s: %w", plainKey, err)
	}

	cidPref := blockCID.Prefix()

	if cidPref.Codec != m.expCidPref.Codec {
		return fmt.Errorf("expected codec %v, got %v", m.expCidPref.Codec, cidPref.Codec)
	}

	if cidPref.MhType != m.expCidPref.MhType {
		return fmt.Errorf("expected MH type %v, got %v", m.expCidPref.MhType, cidPref.MhType)
	}

	if cidPref.MhLength != m.expCidPref.MhLength {
		return fmt.Errorf("expected MH length %v, got %v", m.expCidPref.MhLength, cidPref.MhLength)
	}

	return nil
}
