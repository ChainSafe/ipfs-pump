package pump

import (
	"context"
	"fmt"

	"github.com/ipfs/go-cid"

	client "github.com/ipfs/go-ipfs-http-client"
	iface "github.com/ipfs/interface-go-ipfs-core"
	"github.com/ipfs/interface-go-ipfs-core/options"
	"github.com/ipfs/interface-go-ipfs-core/path"
	"github.com/multiformats/go-multiaddr"
	"github.com/pkg/errors"
)

var _ Drain = &PinDrain{}

type PinDrain struct {
	pinner iface.PinAPI
	// checker iface.PinAPI
}

func NewPinDrain(pinner string) (*PinDrain, error) {
	pinnerMA, err := multiaddr.NewMultiaddr(pinner)
	if err != nil {
		return nil, err
	}

	pinnerCli, err := client.NewApi(pinnerMA)
	if err != nil {
		return nil, err
	}

	return &PinDrain{
		pinner: pinnerCli.Pin(),
	}, nil
}

func (a *PinDrain) Drain(block Block) error {
	c, err := cid.Decode(block.Key.BaseNamespace())
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("error decoding cid from block: %s", block.Key.String()))
	}

	err = a.pinner.Add(context.TODO(), path.IpfsPath(c), options.Pin.Recursive(true))
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("error pinning cid: %s", c.String()))
	}
	return nil
}
