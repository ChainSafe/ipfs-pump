package pump

import (
	ds "github.com/ipfs/go-datastore"
	"github.com/pkg/errors"
)

var _ Drain = &DatastoreDrain{}

type DatastoreDrain struct {
	dstore ds.Datastore
}

func NewDatastoreDrain(dstore ds.Datastore) *DatastoreDrain {
	return &DatastoreDrain{dstore: dstore}
}

func (d *DatastoreDrain) Drain(block Block) error {
	err := d.dstore.Put(block.Key, block.Data)
	if err != nil {
		return errors.Wrap(err, "datastore drain")
	}
	return nil
}
