package pump

import (
	"fmt"

	ds "github.com/ipfs/go-datastore"

	dshelp "github.com/ipfs/go-ipfs-ds-help"

	cid "github.com/ipfs/go-cid"
	shell "github.com/ipfs/go-ipfs-api"
	mh "github.com/multiformats/go-multihash"
)

var _ Drain = &APIDrain{}

type APIDrain struct {
	s *shell.Shell
}

func NewAPIDrain(URL string) *APIDrain {
	return &APIDrain{
		s: shell.NewShell(URL),
	}
}

func NewAPIDrainWithShell(shell *shell.Shell) *APIDrain {
	return &APIDrain{
		s: shell,
	}
}

func (a *APIDrain) Drain(block Block) error {
	_, err := a.s.BlockGet(block.Key.String())
	if err == nil {
		// Block was already migrated
		return nil
	}

	plainKey := ds.NewKey(block.Key.BaseNamespace())
	blockCID, err := dshelp.DsKeyToCidV1(plainKey, cid.DagProtobuf)
	if err != nil {
		return fmt.Errorf("CIDV1 reconstruction error for key '%s': '%w'", plainKey, err)
	}
	cidPref := blockCID.Prefix()
	blockPutCidRaw, err := a.s.BlockPut(block.Data, cid.CodecToStr[cidPref.Codec], mh.Codes[cidPref.MhType], cidPref.MhLength)
	if err != nil {
		return err
	}

	blockPutCid, err := cid.Parse(blockPutCidRaw)
	if err != nil {
		return err
	}
	blockPutCidPref := blockPutCid.Prefix()

	// We can't do `if blockPutCidRaw != blockCID.String()` because the CID v0 can mismatch CID v1 although
	// they would represent the same file, what we want to validate is their CID internals are matching (Codec + MH)
	if blockPutCidPref.Codec != cidPref.Codec {
		return fmt.Errorf("CID Codec mismatch between expected '%s', got '%s'", blockCID, blockPutCidRaw)
	}

	if blockPutCidPref.MhType != cidPref.MhType {
		return fmt.Errorf("CID MhType mismatch between expected '%s', got '%s'", blockCID, blockPutCidRaw)
	}

	if blockPutCidPref.MhLength != cidPref.MhLength {
		return fmt.Errorf("CID MhLength mismatch between expected '%s', got '%s'", blockCID, blockPutCidRaw)
	}

	return nil
}
