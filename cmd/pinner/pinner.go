package main

import (
	"bufio"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"os"
	"sync"

	"github.com/INFURA/ipfs-pump/pump"
	"github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	apiURL = kingpin.Flag("api", "The multi-address for the pinning API").
		Default("").String()

	pinsDSKeys = kingpin.Flag("pins-keys", "The path to a file where all the pins datastore keys are reside").
			Default("").String()

	worker = kingpin.Flag("worker", "The number of concurrent worker to retrieve/push content").
		Default("1").Uint()
)

func main() {
	kingpin.Parse()

	switch {
	case len(*pinsDSKeys) == 0:
		log.Fatal(errors.New("no input files for pins was provided"))
	case len(*apiURL) == 0:
		log.Fatal(errors.New("no pinning API was provided"))
	case *worker < 1:
		log.Fatal(errors.New("at least one worker needs to be provided"))
	}

	file, err := os.OpenFile(*pinsDSKeys, os.O_RDONLY, fs.ModeType)
	if err != nil {
		log.Fatal(fmt.Errorf("error opening ds keys file: %w", err))
	}

	err = pinIt(file)
	if err != nil {
		log.Fatal(fmt.Errorf("error performing pinning: %w", err))
	}
}

func pinIt(file *os.File) error {
	pinChan := make(chan ds.Key, *worker)
	errChan := make(chan error)
	drainPin, err := pump.NewPinDrain(*apiURL)
	if err != nil {
		return err
	}

	var wgPin sync.WaitGroup
	for i := uint(0); i < *worker; i++ {
		wgPin.Add(1)
		go workerJob(drainPin, &wgPin, pinChan, errChan)
	}

	var wgErr sync.WaitGroup
	wgErr.Add(1)
	go func() {
		for err := range errChan {
			log.Printf("error pinning block: %s\n", err)
		}
		wgErr.Done()
	}()

	scanner := bufio.NewScanner(file)
	// optionally, resize scanner's capacity for lines over 64K, see next example
	for scanner.Scan() {
		pinChan <- ds.NewKey(scanner.Text())
	}
	close(pinChan)
	go func() {
		wgPin.Wait()
		close(errChan)
	}()

	if err = scanner.Err(); err != nil {
		log.Fatal(err)
	}

	wgErr.Wait()
	return nil
}

func workerJob(drain pump.Drain, wgPin *sync.WaitGroup, pinChan chan ds.Key, errChan chan error) {
	defer wgPin.Done()
	for key := range pinChan {
		// see enum_ds_badger_pin.go#Keys for more details why 3
		// generally we need to skip first 3 namespaces `/pins/index/cidRindex` of the key
		c, err := cid.Decode(key.Namespaces()[3])
		if err != nil {
			errChan <- err
			continue
		}
		err = drain.Drain(pump.Block{Key: ds.NewKey(c.String())})
		if err != nil {
			errChan <- err
		}
	}
}
