package pump

import (
	ds "github.com/ipfs/go-datastore"
	"github.com/pkg/errors"
)

var _ Collector = &DatastoreCollector{}

type DatastoreCollector struct {
	dstore ds.Datastore
}

func NewDatastoreCollector(dstore ds.Datastore) *DatastoreCollector {
	return &DatastoreCollector{dstore: dstore}
}

func (d *DatastoreCollector) Blocks(in <-chan BlockInfo, out chan<- Block) error {
	go func() {
		for info := range in {
			data, err := d.dstore.Get(info.Key)
			if err != nil {
				out <- Block{Key: info.Key, Error: errors.Wrap(err, "badger datastore collector")}
				continue
			}

			out <- Block{
				Key:  info.Key,
				Data: data,
			}
		}
		close(out)
	}()

	return nil
}
