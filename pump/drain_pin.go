package pump

import (
	"context"
	"fmt"
	"time"

	"github.com/ipfs/interface-go-ipfs-core/path"

	client "github.com/ipfs/go-ipfs-http-client"
	iface "github.com/ipfs/interface-go-ipfs-core"
	"github.com/ipfs/interface-go-ipfs-core/options"
	"github.com/multiformats/go-multiaddr"
)

var _ Drain = &PinDrain{}

type PinDrain struct {
	api iface.PinAPI
}

func NewPinDrain(URL string) (*PinDrain, error) {
	ma, err := multiaddr.NewMultiaddr(URL)
	if err != nil {
		return nil, err
	}

	httpCli, err := client.NewApi(ma)
	if err != nil {
		return nil, err
	}

	return &PinDrain{
		api: httpCli.Pin(),
	}, nil
}

func (a *PinDrain) Drain(block Block) error {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()

	err := a.api.Add(ctx, path.IpfsPath(block.CID), options.Pin.Recursive(false))
	if err != nil {
		return fmt.Errorf("error pinning cid: %s: %w", block.CID.String(), err)
	}
	return nil
}
