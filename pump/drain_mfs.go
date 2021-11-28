package pump

import (
	"context"
	"fmt"

	"github.com/ipfs/go-cid"
	shell "github.com/ipfs/go-ipfs-api"
	"github.com/multiformats/go-multiaddr"
	"github.com/pkg/errors"
)

type mfsDrainer struct {
	shell *shell.Shell
}

func NewMFSDrainer(addr string) (Drain, error) {
	// Gatekeep passed multiaddress
	ma, err := multiaddr.NewMultiaddr(addr)
	if err != nil {
		return nil, err
	}

	return &mfsDrainer{
		shell: shell.NewShell(ma.String()),
	}, nil
}

func (d *mfsDrainer) Drain(block Block) error {
	mfsCid, err := cid.Cast(block.Data)
	if err != nil {
		return err
	}

	ctx := context.Background()
	err = d.shell.FilesCp(ctx, fmt.Sprintf("/ipfs/%s", mfsCid.String()), "/")
	if err != nil {
		return errors.Wrap(err, "error copying MFS filesystem to destination node")
	}

	flChildren, err := d.shell.FilesLs(ctx, fmt.Sprintf("/%s", mfsCid.String()))
	if err != nil {
		return errors.Wrap(err, "error listing files in the copied cid-directory")
	}

	for _, ch := range flChildren {
		err = d.shell.FilesCp(ctx, fmt.Sprintf("/%s/%s", mfsCid.String(), ch.Name), "/")
		if err != nil {
			return errors.Wrap(err, "error moving copied directory to the root")
		}
	}

	err = d.shell.FilesRm(ctx, fmt.Sprintf("/%s", mfsCid.String()), true)
	if err != nil {
		return errors.Wrap(err, "error cleaning up the cid-directory")
	}

	return nil
}
