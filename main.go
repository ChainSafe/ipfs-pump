package main

import (
	"log"
	"os"
	"runtime"
	"runtime/pprof"
	"strings"

	"github.com/INFURA/ipfs-pump/pump"
	s3ds "github.com/ipfs/go-ds-s3"
	"gopkg.in/alecthomas/kingpin.v2"
)

const (
	EnumFile      = "file"
	EnumAPIPin    = "apipin"
	EnumFlatFS    = "flatfs"
	EnumBadger    = "badger"
	EnumBadgerPin = "badgerpin"
	EnumS3        = "s3"
)

const (
	CollAPI       = "api"
	CollFlatFS    = "flatfs"
	CollBadger    = "badger"
	CollBadgerPin = "badgerpin"
	CollS3        = "s3"
)

const (
	DrainAPI     = "api"
	DrainFlatFS  = "flatfs"
	DrainPin     = "pin"
	DrainBadger  = "badger"
	DrainS3      = "s3"
	DrainCollect = "col"
)

var (
	enumValues = []string{EnumFile, EnumAPIPin, EnumFlatFS, EnumBadger, EnumBadgerPin, EnumS3}
	enumArg    = kingpin.Arg("enum", "The source to enumerate the content. "+
		"Possible values are ["+strings.Join(enumValues, ",")+"].").
		Required().Enum(enumValues...)
	collValues = []string{CollAPI, CollFlatFS, CollBadger, CollBadgerPin, CollS3}
	collArg    = kingpin.Arg("coll", "The source to get the data blocks. "+
		"Possible values are ["+strings.Join(collValues, ",")+"].").
		Required().Enum(collValues...)
	drainValues = []string{DrainAPI, DrainCollect, DrainFlatFS, DrainBadger, DrainCollect, DrainS3}
	drainArg    = kingpin.Arg("drain", "The destination to copy to. "+
		"Possible values are ["+strings.Join(drainValues, ",")+"].").
		Required().Enum(drainValues...)

	worker = kingpin.Flag("worker", "The number of concurrent worker to retrieve/push content").
		Default("1").Uint()

	// pprof file paths
	pprofRamPath = kingpin.Flag("pprof-ram-path", "File path to RAM profiling data").
			Default("").String()
	pprofCpuPath = kingpin.Flag("pprof-cpu-path", "File path to CPU profiling data").
			Default("").String()

	failedBlocksPath = kingpin.Flag("failed-blocks-path", "The path to a file where all the failed CIDs should be written").Default("").String()

	enumFilePath    = kingpin.Flag("enum-file-path", "Enumerator "+EnumFile+": Path")
	enumFilePathVal = enumFilePath.String()

	enumAPIPinURL       = kingpin.Flag("enum-api-pin-url", "Enumerator "+EnumAPIPin+": API URL")
	enumAPIPinURLVal    = enumAPIPinURL.String()
	enumAPIPinStream    = kingpin.Flag("enum-api-pin-stream", "Enumerator "+EnumAPIPin+": Stream")
	enumAPIPinStreamVal = enumAPIPinStream.Bool()

	enumFlatFSPath    = kingpin.Flag("enum-flatfs-path", "Enumerator "+EnumFlatFS+": Path")
	enumFlatFSPathVal = enumFlatFSPath.String()

	enumBadgerPath    = kingpin.Flag("enum-badger-path", "Enumerator "+EnumBadger+": Path")
	enumBadgerPathVal = enumBadgerPath.String()

	enumBadgerPinPath    = kingpin.Flag("enum-badger-pin-path", "Enumerator "+EnumBadgerPin+": Path")
	enumBadgerPinPathVal = enumBadgerPinPath.String()

	enumS3Region          = kingpin.Flag("enum-s3-region", "Enumerator "+EnumS3+": Region")
	enumS3RegionVal       = enumS3Region.String()
	enumS3Bucket          = kingpin.Flag("enum-s3-bucket", "Enumerator "+EnumS3+": Bucket name")
	enumS3BucketVal       = enumS3Bucket.String()
	enumS3AccessKey       = kingpin.Flag("enum-s3-access-key", "Enumerator "+EnumS3+": Access key")
	enumS3AccessKeyVal    = enumS3AccessKey.String()
	enumS3SecretKey       = kingpin.Flag("enum-s3-secret-key", "Enumerator "+EnumS3+": Secret key")
	enumS3SecretKeyVal    = enumS3SecretKey.String()
	enumS3SessionToken    = kingpin.Flag("enum-s3-session-token", "Enumerator "+EnumS3+": Session token")
	enumS3SessionTokenVal = enumS3SessionToken.String()

	collAPIURL    = kingpin.Flag("coll-api-url", "Collector "+CollAPI+": API URL")
	collAPIURLVal = collAPIURL.String()

	collFlatFSPath    = kingpin.Flag("coll-flatfs-path", "Collector "+CollFlatFS+": Path")
	collFlatFSPathVal = collFlatFSPath.String()

	collBadgerPath    = kingpin.Flag("coll-badger-path", "Collector "+CollBadger+": Path")
	collBadgerPathVal = collBadgerPath.String()

	collBadgerPinPath    = kingpin.Flag("coll-badger-pin-path", "Collector "+CollBadgerPin+": Path")
	collBadgerPinPathVal = collBadgerPinPath.String()

	collS3Region          = kingpin.Flag("coll-s3-region", "Collector "+EnumS3+": Region")
	collS3RegionVal       = collS3Region.String()
	collS3Bucket          = kingpin.Flag("coll-s3-bucket", "Collector "+CollS3+": Bucket name")
	collS3BucketVal       = collS3Bucket.String()
	collS3AccessKey       = kingpin.Flag("coll-s3-access-key", "Collector "+CollS3+": Access key")
	collS3AccessKeyVal    = collS3AccessKey.String()
	collS3SecretKey       = kingpin.Flag("coll-s3-secret-key", "Collector "+CollS3+": Secret key")
	collS3SecretKeyVal    = collS3SecretKey.String()
	collS3SessionToken    = kingpin.Flag("coll-s3-session-token", "Collector "+CollS3+": Session token")
	collS3SessionTokenVal = collS3SessionToken.String()

	drainAPIURL    = kingpin.Flag("drain-api-url", "Drain "+DrainAPI+": API URL")
	drainAPIURLVal = drainAPIURL.String()

	drainFlatFSPath    = kingpin.Flag("drain-flatfs-path", "Drain "+DrainFlatFS+": Path")
	drainFlatFSPathVal = drainFlatFSPath.String()

	drainBadgerPath    = kingpin.Flag("drain-badger-path", "Drain "+DrainBadger+": Path")
	drainBadgerPathVal = drainBadgerPath.String()

	drainPinAPIURL    = kingpin.Flag("drain-pin-url", "Drain "+DrainPin+": API URL")
	drainPinAPIURLVal = drainPinAPIURL.String()

	drainS3Region          = kingpin.Flag("drain-s3-region", "Drain "+EnumS3+": Region")
	drainS3RegionVal       = drainS3Region.String()
	drainS3Bucket          = kingpin.Flag("drain-s3-bucket", "Drain "+DrainS3+": Bucket name")
	drainS3BucketVal       = drainS3Bucket.String()
	drainS3AccessKey       = kingpin.Flag("drain-s3-access-key", "Drain "+DrainS3+": Access key")
	drainS3AccessKeyVal    = drainS3AccessKey.String()
	drainS3SecretKey       = kingpin.Flag("drain-s3-secret-key", "Drain "+DrainS3+": Secret key")
	drainS3SecretKeyVal    = drainS3SecretKey.String()
	drainS3SessionToken    = kingpin.Flag("drain-s3-session-token", "Drain "+DrainS3+": Session token")
	drainS3SessionTokenVal = drainS3SessionToken.String()
)

func main() {
	kingpin.Parse()

	if len(*pprofCpuPath) != 0 {
		f, err := os.Create(*pprofCpuPath)
		if err != nil {
			log.Fatal("could not create CPU profile: ", err)
		}
		defer f.Close() // error handling omitted for example
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatal("could not start CPU profile: ", err)
		}
		defer pprof.StopCPUProfile()

		fgr, err := os.Create(*pprofCpuPath + ".goroutines")
		if err != nil {
			log.Fatal("could not create CPU profile: ", err)
		}
		defer fgr.Close() // error handling omitted for example

		grProf := pprof.Lookup("goroutine")
		if grProf != nil {
			if err := grProf.WriteTo(fgr, 0); err != nil {
				log.Fatal("could not start goroutines profile: ", err)
			}
		}
	}

	var enumerator pump.Enumerator
	var collector pump.Collector
	var drain pump.Drain
	var err error

	switch *enumArg {
	case EnumFile:
		requiredFlag(enumFilePath, *enumFilePathVal)
		var file *os.File
		file, err = os.Open(*enumFilePathVal)
		if err != nil {
			log.Fatal(err)
		}
		enumerator, err = pump.NewFileEnumerator(file)
	case EnumAPIPin:
		requiredFlag(enumAPIPinURL, *enumAPIPinURLVal)
		enumerator = pump.NewAPIPinEnumerator(*enumAPIPinURLVal, *enumAPIPinStreamVal)
	case EnumFlatFS:
		requiredFlag(enumFlatFSPath, *enumFlatFSPathVal)
		enumerator, err = pump.NewFlatFSEnumerator(*enumFlatFSPathVal)
	case EnumBadger:
		requiredFlag(enumBadgerPath, *enumBadgerPathVal)
		enumerator, err = pump.NewBadgerEnumerator(*enumBadgerPathVal)
	case EnumBadgerPin:
		requiredFlag(enumBadgerPinPath, *enumBadgerPinPathVal)
		enumerator, err = pump.NewBadgerPinEnumerator(*enumBadgerPinPathVal)
	case EnumS3:
		requiredFlag(enumS3Region, *enumS3RegionVal)
		requiredFlag(enumS3Bucket, *enumS3BucketVal)

		config := s3ds.Config{
			Region:       *enumS3RegionVal,
			Bucket:       *enumS3BucketVal,
			AccessKey:    *enumS3AccessKeyVal,
			SecretKey:    *enumS3SecretKeyVal,
			SessionToken: *enumS3SessionTokenVal,
		}

		enumerator, err = pump.NewS3Enumerator(config)
	}

	if err != nil {
		log.Fatal(err)
	}

	switch *collArg {
	case CollAPI:
		requiredFlag(collAPIURL, *collAPIURLVal)
		collector = pump.NewAPICollector(*collAPIURLVal)
	case CollFlatFS:
		requiredFlag(collFlatFSPath, *collFlatFSPathVal)
		collector, err = pump.NewFlatFSCollector(*collFlatFSPathVal)
	case CollBadger:
		requiredFlag(collBadgerPath, *collBadgerPathVal)
		collector, err = pump.NewBadgerCollector(*collBadgerPathVal)
	case CollBadgerPin:
		requiredFlag(collBadgerPinPath, *collBadgerPinPathVal)
		collector, err = pump.NewBadgerPinCollector(*collBadgerPinPathVal)
	case CollS3:
		requiredFlag(collS3Region, *collS3RegionVal)
		requiredFlag(collS3Bucket, *collS3BucketVal)

		config := s3ds.Config{
			Region:       *collS3RegionVal,
			Bucket:       *collS3BucketVal,
			AccessKey:    *collS3AccessKeyVal,
			SecretKey:    *collS3SecretKeyVal,
			SessionToken: *collS3SessionTokenVal,
		}

		collector, err = pump.NewS3Collector(config)
	}

	if err != nil {
		log.Fatal(err)
	}

	switch *drainArg {
	case DrainAPI:
		requiredFlag(drainAPIURL, *drainAPIURLVal)
		drain = pump.NewAPIDrain(*drainAPIURLVal)
	case DrainCollect:
		drain, err = pump.NewFailDrainer()
	case DrainFlatFS:
		requiredFlag(drainFlatFSPath, *drainFlatFSPathVal)
		drain, err = pump.NewFlatFSDrain(*drainFlatFSPathVal)
	case DrainBadger:
		requiredFlag(drainBadgerPath, *drainBadgerPathVal)
		drain, err = pump.NewBadgerDrain(*drainBadgerPathVal)
	case DrainPin:
		requiredFlag(drainPinAPIURL, *drainPinAPIURLVal)
		drain, err = pump.NewPinDrain(*drainPinAPIURLVal)
	case DrainS3:
		requiredFlag(drainS3Region, *drainS3RegionVal)
		requiredFlag(drainS3Bucket, *drainS3BucketVal)

		config := s3ds.Config{
			Region:       *drainS3RegionVal,
			Bucket:       *drainS3BucketVal,
			AccessKey:    *drainS3AccessKeyVal,
			SecretKey:    *drainS3SecretKeyVal,
			SessionToken: *drainS3SessionTokenVal,
		}

		drain, err = pump.NewS3Drain(config)
	}

	if err != nil {
		log.Fatal(err)
	}

	progressWriter := pump.NewProgressWriter()

	var failedBlocksWriter pump.FailedBlocksWriter
	if *failedBlocksPath == "" {
		failedBlocksWriter = pump.NewNullableFileEnumeratorWriter()
	} else {
		enumWriter, closeWriter, err := pump.NewFileEnumeratorWriter(*failedBlocksPath)
		if err != nil {
			log.Fatal(err)
		}
		failedBlocksWriter = enumWriter

		defer func() {
			err = closeWriter()
			if err != nil {
				log.Fatal(err)
			}
		}()
	}

	pump.PumpIt(enumerator, collector, drain, failedBlocksWriter, progressWriter, *worker)

	if *pprofRamPath != "" {
		f, err := os.Create(*pprofRamPath)
		if err != nil {
			log.Fatal("could not create memory profile: ", err)
		}
		defer f.Close() // error handling omitted for example
		runtime.GC()    // get up-to-date statistics
		if err := pprof.WriteHeapProfile(f); err != nil {
			log.Fatal("could not write memory profile: ", err)
		}
	}
}

func requiredFlag(flag *kingpin.FlagClause, val string) {
	if len(val) == 0 {
		log.Fatalf("flag %s is required", flag.Model().Name)
	}
}
