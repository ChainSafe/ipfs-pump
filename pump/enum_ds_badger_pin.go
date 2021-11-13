package pump

import (
	"log"

	ds "github.com/ipfs/go-datastore"
	dsq "github.com/ipfs/go-datastore/query"
	badger "github.com/ipfs/go-ds-badger"
	"github.com/pkg/errors"
)

type badgerPinEnum struct {
	dstore ds.Datastore
}

func NewBadgerPinEnumerator(path string) (Enumerator, error) {
	opts := badger.DefaultOptions
	opts.Options.ReadOnly = true
	ds, err := badger.NewDatastore(path, &opts)
	if err != nil {
		return nil, errors.Wrap(err, "Badger enumerator")
	}

	return &badgerPinEnum{ds}, nil
}

func (*badgerPinEnum) TotalCount() int {
	return -1
}

func (d *badgerPinEnum) Keys(out chan<- BlockInfo) error {
	// based on https://github.com/ipfs/go-ipfs-blockstore/blob/master/blockstore.go

	// KeysOnly, because that would be _a lot_ of data. Prefix: "/pins",
	q := dsq.Query{Prefix: "/pins/index/cidRindex", KeysOnly: true}
	res, err := d.dstore.Query(q)
	if err != nil {
		return errors.Wrap(err, "datastore enumerator")
	}

	go func() {
		defer func() {
			_ = res.Close() // ensure exit (signals early exit, too)
			close(out)
		}()

		for {
			e, ok := res.NextSync()
			if !ok {
				return
			}
			if e.Error != nil {
				log.Println(errors.Wrap(e.Error, "enumerating datastore"))
				return
			}

			out <- BlockInfo{
				Key:   ds.RawKey(e.Key),
				Entry: e.Entry,
			}
		}
	}()

	return nil
}
