package pump

import (
	ds "github.com/ipfs/go-datastore"
	flatfs "github.com/ipfs/go-ds-flatfs"
	"github.com/pkg/errors"
)

type flatfsDatastore struct {
	dstore ds.Datastore
}

func NewFlatFSDrain(path string) (Drain, error) {
	ds, err := flatfs.Open(path, false)
	if err != nil {
		return nil, errors.Wrap(err, "FlatFS drain")
	}

	return &flatfsDatastore{dstore: ds}, nil
}

func (d *flatfsDatastore) Drain(block Block) error {
	plainKey := ds.NewKey(block.Key.BaseNamespace())
	err := d.dstore.Put(plainKey, block.Data)
	if err != nil {
		return errors.Wrap(err, "datastore drain")
	}
	return nil
}
