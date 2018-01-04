package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/skycoin/skycoin/src/cipher"

	"github.com/skycoin/cxo/cxoutils"
	"github.com/skycoin/cxo/node"
)

// defaults
const (
	cleaningInterval      time.Duration = 1 * time.Minute  // 1m
	staleObjectsThreshold int           = 16 * 1024 * 1204 // 16M
	historyLength         int           = 100              // 100 Root objects
)

func init() {
	log.SetFlags(log.Lshortfile)
}

func waitInterrupt() {
	var sig = make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	<-sig
}

func main() {

	var c = node.NewConfig()
	c.OnSubscribeRemote = acceptAllSubscriptions

	c.FromFlags()
	flag.Parse()

	var (
		n   *node.Node
		err error
	)

	// create and launch
	if n, err = node.NewNode(c); err != nil {
		log.Fatal(err)
	}
	defer n.Close()

	// remove stale obejcts and old Root objects
	var (
		wg           sync.WaitGroup
		stopCleaning = make(chan struct{})
	)

	wg.Add(1)
	go cleaning(&wg, stopCleaning, n)

	// waiting for SIGINT
	waitInterrupt()
	close(stopCleaning)
}

// accept all incoming subscriptions
func acceptAllSubscriptions(c *node.Conn, pk cipher.PubKey) (_ error) {
	if err := c.Node().Share(pk); err != nil {
		log.Fatal("DB failure:", err) // DB failure
	}
	return
}

func cleaning(wg *sync.WaitGroup, stopCleaning <-chan struct{}, n *node.Node) {
	defer wg.Done()

	var tk = time.NewTicker(cleaningInterval)
	defer tk.Stop()

	var tc = tk.C

	for {

		select {
		case <-stopCleaning:
			return
		case <-tc:
			clean(n)
		}

	}

}

func clean(n *node.Node) {

	var (
		c   = n.Container()
		err error
	)

	if err = cxoutils.RemoveRootObjects(c, historyLength); err != nil {
		log.Fatal(err)
	}

	var all, used = c.DB().CXDS().Amount()

	if all-used < staleObjectsThreshold {
		return
	}

	if err = cxoutils.RemoveObjects(c); err != nil {
		log.Fatal(err)
	}

}
