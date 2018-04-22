package main

import (
	"flag"
	"log"
	"os"
	"os/signal"

	"github.com/skycoin/cxo/intro/discovery/discovery/db"
	"github.com/skycoin/net/skycoin-messenger/factory"
)

const (
	Address    string = ":8080"
	DBPath     string = db.InMemory
	RandomSeed string = ":random:"
)

func main() {

	var (
		address        = Address
		dbPath         = DBPath
		seedConfigPath = RandomSeed

		help bool
	)

	flag.StringVar(&address,
		"a",
		address,
		"listening address")
	flag.StringVar(&dbPath,
		"db-path",
		dbPath,
		"path to SQLite3 database")
	flag.StringVar(&seedConfigPath,
		"seed-config",
		seedConfigPath,
		"seed config: path to file or ':random:'")
	flag.BoolVar(&help,
		"h",
		help,
		"show help")
	flag.Parse()

	if help == true {
		flag.PrintDefaults()
		return
	}

	var d, err = newDiscovery(dbPath, seedConfigPath)

	if err != nil {
		log.Fatal(err)
	}
	defer d.Close()

	// start discovery listener
	if err = d.m.Listen(address); err != nil {
		log.Fatal(err)
	}

	waitInterrupt() // wait for SIGINT
}

func waitInterrupt() {
	var sig = make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	<-sig
}

type Discovery struct {
	m  *factory.MessengerFactory
	db *db.DB
}

func newDiscovery(dbPath, seedConfigPath string) (d *Discovery, err error) {
	d = new(Discovery)

	d.m = factory.NewMessengerFactory()

	if d.db, err = db.New(dbPath); err != nil {
		d = nil
		return
	}

	var sc *factory.SeedConfig

	if seedConfigPath == RandomSeed {
		sc = factory.NewSeedConfig()
	} else {
		if sc, err = factory.ReadSeedConfig(seedConfigPath); err != nil {
			d = nil
			return
		}
	}

	if err = d.m.SetDefaultSeedConfig(sc); err != nil {
		d = nil
		return
	}

	// use SQLite3 DB to keep information in
	d.m.RegisterService = d.db.RegisterService
	d.m.UnRegisterService = d.db.UnRegisterService
	d.m.FindByAttributes = d.db.FindResultByAttrs
	d.m.FindByAttributesAndPaging = d.db.FindResultByAttrsAndPaging
	d.m.FindServiceAddresses = d.db.FindServiceAddresses

	return
}

// Close the Discovery
func (d *Discovery) Close() (err error) {
	d.m.Close()
	return d.db.Close()
}
