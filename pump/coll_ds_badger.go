package pump

import (
	badger "github.com/ipfs/go-ds-badger"
	"github.com/pkg/errors"
)

func NewBadgerCollector(path string) (*DatastoreCollector, error) {
	opts := badger.DefaultOptions
	// Readonly because we need enumerator and collector to have access to it
	opts.Options.ReadOnly = true
	// Completely disable GC because of ReadOnly access
	opts.GcInterval = 0
	datastore, err := badger.NewDatastore(path, &opts)
	if err != nil {
		return nil, errors.Wrap(err, "Badger collector")
	}

	return NewDatastoreCollector(datastore), nil
}
