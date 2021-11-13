package pump

import (
	shell "github.com/ipfs/go-ipfs-api"
	"github.com/pkg/errors"
)

var _ Collector = &APICollector{}

type APICollector struct {
	URL string
}

func NewAPICollector(URL string) *APICollector {
	return &APICollector{URL: URL}
}

func (a *APICollector) Blocks(in <-chan BlockInfo, out chan<- Block) error {
	s := shell.NewShell(a.URL)

	_, _, err := s.Version()
	if err != nil {
		return errors.Wrap(err, "failed to reach the API")
	}

	go func() {
		for info := range in {
			data, err := s.BlockGet(info.Key.String())
			if err != nil {
				out <- Block{Key: info.Key, Error: err}
				continue
			}

			out <- Block{
				Key:  info.Key,
				Data: data,
			}
		}
		close(out)
	}()

	return nil
}
