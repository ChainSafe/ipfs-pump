package pump

import (
	badger "github.com/ipfs/go-ds-badger"
	"github.com/pkg/errors"
)

func NewBadgerCollector(path string) (*DatastoreCollector, error) {
	opts := badger.DefaultOptions
	opts.Options.ReadOnly = true
	ds, err := badger.NewDatastore(path, &opts)
	if err != nil {
		return nil, errors.Wrap(err, "Badger collector")
	}

	return NewDatastoreCollector(ds), nil
}
