package node

import (
	"fmt"
	"testing"
	"time"

	"github.com/skycoin/skycoin/src/cipher"

	"github.com/skycoin/cxo/skyobject"
	"github.com/skycoin/cxo/skyobject/registry"

	"github.com/skycoin/cxo/node/msg"
)

func getBenchNode(prefix string, ft msg.Features) (n *Node) {

	var conf = getTestConfig(prefix)
	conf.Features = ft

	var err error
	if n, err = NewNode(conf); err != nil {
		panic(err)
	}

	return
}

func Benchmark_sendReceiveFeatures(b *testing.B) {

	for _, ft := range []msg.Features{
		0,
		msg.CreatedHashes,
		msg.CreatedObjects,
	} {
		b.Run(ft.String(), func(b *testing.B) {
			benchmarkSendReceiveFeatures(b, ft)
		})
	}

}

func benchmarkSendReceiveFeatures(b *testing.B, ft msg.Features) {

	var (
		fr           = make(chan *registry.Root, 100)
		onRootFilled = func(_ *Node, r *registry.Root) { fr <- r }

		sn = getBenchNode("sender", ft)

		rconf = getTestConfig("receiver")
	)

	rconf.Features = ft               // use features
	rconf.TCP.Listen = ""             // don't listen
	rconf.UDP.Listen = ""             // don't listen
	rconf.OnRootFilled = onRootFilled // callback
	rconf.OnRootReceived = func(c *Conn, r *registry.Root) (_ error) {
		//b.Logf("[%s] root received %s", c.String(), r.Short())
		return
	}
	rconf.OnFillingBreaks = func(n *Node, r *registry.Root, err error) {
		b.Logf("filling of %s breaks by %v", r.Short(), err)
	}

	var rn, err = NewNode(rconf)

	if err != nil {
		b.Fatal(err)
	}

	defer sn.Close()
	defer rn.Close()

	var pk, sk = cipher.GenerateKeyPair()

	assertNil(b, sn.Share(pk))
	assertNil(b, rn.Share(pk))

	if sn.TCP().Address() == "" {
		b.Fatal("blank listening address")
	}

	// connect the nodes between
	var c *Conn
	if c, err = rn.TCP().Connect(sn.TCP().Address()); err != nil {
		b.Fatal(err)
	}

	// subscribe the connection
	if err = c.Subscribe(pk); err != nil {
		b.Fatal(err)
	}

	var (
		reg = getTestRegistry()
		sc  = sn.Container()

		up *skyobject.Unpack
	)

	if up, err = sc.Unpack(sk, reg); err != nil {
		b.Fatal(err)
	}

	var (
		r  = new(registry.Root)
		rr *registry.Root // received Root
	)

	r.Nonce = 9021 // random
	r.Pub = pk     // set
	r.Descriptor = []byte("hey-ho!")

	var feed Feed

	r.Refs = append(r.Refs,
		dynamicByValue(b, up, "test.User", User{"Alice", 19, nil}),
		dynamicByValue(b, up, "test.Feed", feed),
	)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {

		if err = r.Refs[1].Value(up, &feed); err != nil {
			b.Fatal(err)
		}

		err = feed.Posts.AppendValues(up, Post{
			Head: fmt.Sprintf("Head #%d", i),
			Body: fmt.Sprintf("Body #%d", i),
			Time: time.Now().UnixNano(),
		})

		if err != nil {
			b.Fatal(err)
		}

		if err = r.Refs[1].SetValue(up, feed); err != nil {
			b.Fatal(err)
		}

		// save the Root
		if err := sc.Save(up, r); err != nil {
			b.Fatal(err)
		}

		if err = sn.Publish(r, up); err != nil {
			b.Fatal(err)
		}

		select {
		case rr = <-fr:
		case <-time.After(slow * 20):
			b.Fatal("slow")
		}

	}

	_ = rr

	b.ReportAllocs()

}

/*

# loopback

go test -cover -race -timeout=10m -benchtime=1m -bench .
goos: linux
goarch: amd64
pkg: github.com/skycoin/cxo/node
Benchmark_sendReceiveFeatures/no-4               500  233785181 ns/op  333138 B/op  5218 allocs/op
Benchmark_sendReceiveFeatures/created_hashes-4   300  260333048 ns/op  310440 B/op  4669 allocs/op
Benchmark_sendReceiveFeatures/created_objects-4  300  256745369 ns/op  303147 B/op  4528 allocs/op
PASS
coverage: 49.7% of statements
ok      github.com/skycoin/cxo/node     384.163s

# 3G

kostyarin@x556uq:~/go/src/github.com/skycoin/cxo/node$ go test -cover -race -timeout=10m -benchtime=1m -bench . -slow-timeout=1s
goos: linux
goarch: amd64
pkg: github.com/skycoin/cxo/node
Benchmark_sendReceiveFeatures/no-4                50  1947748574 ns/op  316114 B/op  5003 allocs/op
Benchmark_sendReceiveFeatures/created_hashes-4   100   837255638 ns/op  306308 B/op  4683 allocs/op
Benchmark_sendReceiveFeatures/created_objects-4  200   478563840 ns/op  300685 B/op  4500 allocs/op
PASS
coverage: 49.7% of statements
ok      github.com/skycoin/cxo/node     443.978s



*/
