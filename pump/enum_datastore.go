package pump

import (
	"log"

	ds "github.com/ipfs/go-datastore"
	dsq "github.com/ipfs/go-datastore/query"
	"github.com/pkg/errors"
)

var _ Enumerator = &DatastoreEnumerator{}

type DatastoreEnumerator struct {
	dstore    ds.Datastore
	keyPrefix string
}

func NewDatastoreEnumerator(dstore ds.Datastore, prefix ...string) *DatastoreEnumerator {
	pref := ""
	if len(prefix) >= 1 {
		pref = prefix[0]
	}
	return &DatastoreEnumerator{dstore: dstore, keyPrefix: pref}
}

func (*DatastoreEnumerator) TotalCount() int {
	return -1
}

func (d *DatastoreEnumerator) Keys(out chan<- BlockInfo) error {
	// based on https://github.com/ipfs/go-ipfs-blockstore/blob/master/blockstore.go
	// KeysOnly, because that would be _a lot_ of data.
	q := dsq.Query{KeysOnly: true}
	if len(d.keyPrefix) != 0 {
		q.Prefix = d.keyPrefix
	}

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
