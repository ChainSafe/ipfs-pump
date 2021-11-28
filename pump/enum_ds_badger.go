package pump

import (
	ds "github.com/ipfs/go-datastore"
	badger "github.com/ipfs/go-ds-badger"
	"github.com/pkg/errors"
)

type badgerEnum struct {
	dstore ds.Datastore
}

func NewBadgerEnumerator(params ...string) (Enumerator, error) {
	opts := badger.DefaultOptions
	// Readonly because we need enumerator and collector to have access to it
	opts.Options.ReadOnly = true
	// Completely disable GC because of ReadOnly access
	opts.GcInterval = 0
	var path string
	var keyPrefix string
	switch len(params) {
	case 0:
		return nil, errors.New("at least Badger DB location path needs to be provided")
	case 1:
		path = params[0]
	default:
		path = params[0]
		keyPrefix = params[1]
	}
	datastore, err := badger.NewDatastore(path, &opts)
	if err != nil {
		return nil, errors.Wrap(err, "Badger enumerator")
	}

	return NewDatastoreEnumerator(datastore, keyPrefix), nil
}
