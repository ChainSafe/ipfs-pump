package pump

import (
	"context"
	"fmt"
	"time"

	"github.com/pkg/errors"

	"github.com/ipfs/interface-go-ipfs-core/path"

	client "github.com/ipfs/go-ipfs-http-client"
	iface "github.com/ipfs/interface-go-ipfs-core"
	"github.com/ipfs/interface-go-ipfs-core/options"
	"github.com/multiformats/go-multiaddr"
)

var _ Drain = &PinDrain{}

type PinDrain struct {
	pinner  iface.PinAPI
	checker iface.PinAPI
}

func NewPinDrain(pinner, checker string) (*PinDrain, error) {
	pinnerMA, err := multiaddr.NewMultiaddr(pinner)
	if err != nil {
		return nil, err
	}

	pinnerCli, err := client.NewApi(pinnerMA)
	if err != nil {
		return nil, err
	}

	chackerMA, err := multiaddr.NewMultiaddr(checker)
	if err != nil {
		return nil, err
	}

	checkerCli, err := client.NewApi(chackerMA)
	if err != nil {
		return nil, err
	}

	return &PinDrain{
		pinner:  pinnerCli.Pin(),
		checker: checkerCli.Pin(),
	}, nil
}

func (a *PinDrain) Drain(block Block) error {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()

	reason, isPinned, err := a.pinner.IsPinned(ctx, path.IpfsPath(block.CID))
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("error checking pin status in pinner for cid: %s", block.CID.String()))
	}

	if isPinned {
		return nil
	}

	reason, isPinned, err = a.checker.IsPinned(ctx, path.IpfsPath(block.CID))
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("error checking pin status in checker for cid: %s", block.CID.String()))
	}

	if !isPinned {
		// It was not pinned originally, so do nothing
		return nil
	}

	recursivePin := false
	if reason == "recursive" {
		recursivePin = true
	}

	err = a.pinner.Add(ctx, path.IpfsPath(block.CID), options.Pin.Recursive(recursivePin))
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("error pinning cid: %s", block.CID.String()))
	}
	return nil
}
