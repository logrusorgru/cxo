package node

import (
	"os"
	"path/filepath"
	//"strings"
	"testing"

	// "github.com/skycoin/cxo/data"
	"github.com/skycoin/cxo/node/gnet"
	"github.com/skycoin/cxo/skyobject"
)

// func TestServer_Start(t *testing.T) {
// 	// Start() (err error)

// 	// 1. arbitrary address
// 	// 2. provided address

// 	t.Run("arbitrary", func(t *testing.T) {
// 		conf := newServerConfig()
// 		conf.Listen = ""

// 		s, err := newServerConf(conf)
// 		if err != nil {
// 			t.Fatal(err)
// 		}
// 		defer s.Close()

// 		if err := s.Start(); err != nil {
// 			t.Error(err)
// 		}
// 	})

// 	t.Run("provided", func(t *testing.T) {
// 		conf := newServerConfig()
// 		conf.Listen = "[::]:8987"

// 		s, err := newServerConf(conf)
// 		if err != nil {
// 			t.Fatal(err)
// 		}
// 		defer s.Close()

// 		if err := s.Start(); err != nil {
// 			if !strings.Contains(err.Error(), "address already in use") {
// 				t.Error(err)
// 			}
// 			return
// 		}

// 		if a := s.pool.Address(); a != "[::]:8987" {
// 			t.Error("wrong listening address:", a)
// 		}
// 	})

// }

func TestNewNode(t *testing.T) {
	// NewNode(sc NodeConfig) (s *Node, err error)

	// registry must be nil
	t.Run("registry", func(t *testing.T) {
		s, err := newNode(newNodeConfig(false))
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()

		if s.Container().CoreRegistry() != nil {
			t.Error("unexpected core registry")
		}
	})

	defer clean()

	// if datbase in memeory then Node doesn't
	// creates DataDir and datbase
	t.Run("memory db", func(t *testing.T) {
		clean()

		conf := newNodeConfig(false) // in-memory

		s, err := newNode(conf)
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()

		if _, err := os.Stat(conf.DataDir); err == nil {
			t.Error("unexpected data dir:", conf.DataDir)
			if _, err = os.Stat(conf.DBPath); err == nil {
				t.Error("unexpected db file:", conf.DBPath)
			} else if !os.IsNotExist(err) {
				t.Error("unexpected error")
			}
		} else if !os.IsNotExist(err) {
			t.Error("unexpected error:", err)
		}

	})

	// If database is not in memory, then
	// DataDir must be created even if DBPath
	// points to another directory
	t.Run("data dir", func(t *testing.T) {
		clean()

		conf := newNodeConfig(false)
		conf.InMemoryDB = false

		s, err := newNode(conf)
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()

		if _, err := os.Stat(conf.DataDir); err != nil {
			t.Error(err)
		} else {
			if _, err := os.Stat(conf.DBPath); err != nil {
				t.Error(err)
			}
		}

	})

	t.Run("dbpath", func(t *testing.T) {
		clean()

		conf := newNodeConfig(false)
		conf.InMemoryDB = false
		conf.DBPath = filepath.Join(testDataDir, "another.db")

		s, err := newNode(conf)
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()

		if _, err := os.Stat(conf.DataDir); err != nil {
			t.Error(err)
		} else {
			if _, err := os.Stat(conf.DBPath); err != nil {
				t.Error(err)
			}
		}

	})

}

func TestNewNodeReg(t *testing.T) {
	//sc NodeConfig, reg *skyobject.Registry) (s *Node, err error)

	// registry must be the same
	t.Run("registry", func(t *testing.T) {
		reg := skyobject.NewRegistry()

		s, err := newNodeReg(newNodeConfig(false), reg)
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()

		if s.Container().CoreRegistry() != reg {
			t.Error("wrong or missing registry")
		}
	})

	t.Run("invalid gnet.Config", func(t *testing.T) {
		conf := newNodeConfig(false)
		conf.Config.DialTimeout = -1000
		if s, err := newNode(conf); err == nil {
			t.Error("missing error")
			s.Close()
		}
	})

}

func TestNode_Start(t *testing.T) {

	t.Run("disable listener", func(t *testing.T) {
		s, err := newNode(newNodeConfig(false))
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()

		if err = s.Start(); err != nil {
			t.Fatal(err)
		}

		if s.pool.Address() != "" {
			t.Error("listens on")
		}
	})

	t.Run("enable listener", func(t *testing.T) {
		s, err := newNode(newNodeConfig(true))
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()

		if err = s.Start(); err != nil {
			t.Fatal(err)
		}

		if s.pool.Address() == "" {
			t.Error("doesn't listen on")
		}
	})

	t.Run("disable RPC listener", func(t *testing.T) {
		s, err := newNode(newNodeConfig(false))
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()

		if err = s.Start(); err != nil {
			t.Fatal(err)
		}

		if s.rpc != nil {
			t.Error("RPC was created")
		}
	})

	t.Run("enable RPC listener", func(t *testing.T) {
		conf := newNodeConfig(false)
		conf.EnableRPC = true

		s, err := newNode(conf)
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()

		if err = s.Start(); err != nil {
			t.Fatal(err)
		}

		if s.rpc == nil {
			t.Error("RPC wasn't created")
		}
	})
}

func TestNode_Close(t *testing.T) {

	t.Run("close listener", func(t *testing.T) {
		s, err := newNode(newNodeConfig(true))
		if err != nil {
			t.Fatal(err)
		}
		if err = s.Start(); err != nil {
			t.Fatal(err)
		}
		s.Close()

		if err = s.pool.Listen(""); err == nil {
			t.Error("missing error")
		} else if err != gnet.ErrClosed {
			t.Error("unexpected error:", err)
		}
	})

	t.Run("close RPC listener", func(t *testing.T) {
		conf := newNodeConfig(false)
		conf.EnableRPC = true

		s, err := newNode(conf)
		if err != nil {
			t.Fatal(err)
		}
		if err = s.Start(); err != nil {
			t.Fatal(err)
		}
		s.Close()

		// TODO: can block if not closed
		if c, err := s.rpc.l.Accept(); err == nil {
			t.Error("misisng error")
			c.Close()
		}
	})

	t.Run("twice", func(t *testing.T) {
		conf := newNodeConfig(true)
		conf.EnableRPC = true
		conf.InMemoryDB = false // force to use BoltDBs

		s, err := newNode(conf)
		if err != nil {
			t.Fatal(err)
		}
		if err = s.Start(); err != nil {
			t.Fatal(err)
		}
		if err = s.Close(); err != nil {
			t.Error(err)
		}
		if err = s.Close(); err != nil {
			t.Error(err)
		}
	})

}
