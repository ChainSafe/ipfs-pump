package pump

import (
	"github.com/ipfs/go-cid"

	ds "github.com/ipfs/go-datastore"
	badger "github.com/ipfs/go-ds-badger"
	"github.com/pkg/errors"
)

type badgerPinCollector struct {
	dstore ds.Datastore
}

func NewBadgerPinCollector(path string) (Collector, error) {
	opts := badger.DefaultOptions
	opts.Options.ReadOnly = true
	ds, err := badger.NewDatastore(path, &opts)
	if err != nil {
		return nil, errors.Wrap(err, "Badger collector")
	}

	return &badgerPinCollector{ds}, nil
}

func (d *badgerPinCollector) Blocks(in <-chan BlockInfo, out chan<- Block) error {
	go func() {
		for info := range in {
			data, err := d.dstore.Get(info.Key)
			if err != nil {
				out <- Block{Key: info.Key, Error: errors.Wrap(err, "datastore collector")}
				continue
			}

			ns := info.Key.Namespaces()
			// This index indicates that DS namespaces is based on
			// /pins/index/cidRindex/<encoded-pin-cid>/<encoded-pin-id>
			// so we need to get forth section and decode it for CID
			magicIndex := 3
			c, err := cid.Parse(ns[magicIndex])
			if err != nil {
				out <- Block{Key: info.Key, Error: errors.Wrap(err, "error decoding cid from datastore")}
				continue
			}

			out <- Block{
				Key:  ds.NewKey(c.String()),
				Data: data,
			}
		}
		close(out)
	}()

	return nil
}