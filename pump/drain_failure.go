package pump

import "errors"

type failDrainer struct{}

func NewFailDrainer() (Drain, error) {
	return &failDrainer{}, nil
}

func (d *failDrainer) Drain(block Block) error {
	return errors.New("collecting blocks to drain")
}
