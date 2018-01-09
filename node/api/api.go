package api

const (
	ListenHTTP string = ":8872" // websockets

	WSPath   string = "ws"  // default websockets path
	HTTPPath string = "cxo" // default http path

	Pings time.Duration = 118 * time.Second
)

// HTTPConfig represents configurations of
// a HTTP and websockets networks
type HTTPConfig struct {
	// Listen is HTTP listening address. One of
	// WSPath and HTTPPath fields should not be
	// empty. Otherwise this config is invalid.
	// Both fields (WSPath and HTTPPath can contain
	// a path).
	Listen string

	// WSPath is path to handle websockets requests.
	// Use empty string to disable websockets.
	WSPath string
	// HTTPPath is path to handle HTTP GET requests
	// (to get JSON response). Use empty string to
	// disable this feature.
	HTTPPath string

	// Pings is time after which a ping message will
	// be sent through websockets conection if it's idle.
	// Set to zero to disabel pings
	Pings time.Duration

	// ResponseTimeout if response timeout for
	// websockets connections. Set to zero to disable
	// timeout
	ResponseTimeout time.Duration

	// TLSConfig contains TLS configurations
	TLSConfig
}

/*
	c.HTTP.Listen = ListenHTTP
	c.HTTP.WSPath = WSPath
	c.HTTP.HTTPPath = HTTPPath
	c.HTTP.ResponseTimeout = ResponseTimeout
	c.HTTP.Pings = Pings

		// websockets

	flag.StringVar(&c.HTTP.Listen,
		"http",
		c.HTTP.Listen,
		"http listening address")

	flag.StringVar(&c.HTTP.WSPath,
		"ws-path",
		c.HTTP.WSPath,
		"path to handle webscoekts requests")

	flag.StringVar(&c.HTTP.HTTPPath,
		"http-path",
		c.HTTP.HTTPPath,
		"path to handle http GET requests")

	flag.DurationVar(&c.HTTP.Pings,
		"ws-pings",
		c.HTTP.Pings,
		"ping idle websockets connections")

	flag.DurationVar(&c.HTTP.ResponseTimeout,
		"ws-response-timeout",
		c.HTTP.ResponseTimeout,
		"response timeout for websockets connections")

	c.HTTP.TLSConfig.FromFlags("ws", "websocket connections")

		if c.HTTP.Listen != "" {
		if c.HTTP.HTTPPath == "" && c.HTTP.WSPath == "" {
			return errors.New("HTTP configs: missing ws path of HTTP path")
		}
	}

		// should be already validated
	if c.HTTP.Listen != "" {
		err = c.HTTP.TLSConfig.Init()
	}

*/

//
// WS
//

// A WS represent websockets transport
type WS struct {
	// factory
	factory.FactoryCommonFields

	// back reference
	n *Node

	//
	mx   sync.Mutex
	s    *http.Server
	path string // handler path

	cs map[string]*Conn // address -> conn
}

func newWS(n *Node) (w *WS) {

	w = new(WS)
	w.FactoryCommonFields = factory.NewFactoryCommonFields()

	w.cs = make(map[string]*Conn)

}

// Listen on given address+path. If path is empty then
// root path used to handle websockets requests. It's
// possible to listen only once. For example
//
//     err = node.WS().Listen("127.0.0.1:8873/cxo")
//
// The address can contain leading http:// or https://
// scheme. But TLS depends on configs of the Node, not
// the scheme
func (w *WS) Listen(address string) (err error) {

	// don't listen twice

	w.mx.Lock()
	defer w.mx.Unlock()

	if w.s != nil {
		return ErrAlreadyListen
	}

	// parse and check the address

	switch {
	case strings.HasPrefix(address, "http://"):
	case strings.HasPrefix(address, "https://"):
	default:
		address = "http://" + address
	}

	var u url.URL
	if u, err = url.Parse(address); err != nil {
		return
	}

	if _, err = net.ResolveTCPAddr("tcp", u.Host); err != nil {
		return
	}

	if u.Path == "" {
		u.Path = "/"
	}

	// create http sever

	var s http.Server

	s.Addr = u.Host
	s.Handler = http.HandleFunc(u.Path, w.n.WebsocketsHandler)

	// TOOD (kostyarin): TLS

	// create logger of the http.Server

	var (
		out    io.Writer
		prefix string
	)

	if out = w.n.Config().Logger.Output; out == nil {
		out = os.Stderr
	}

	prefix = w.n.Config().Logger.Prefix + "[http] "

	s.ErrorLog = log.New(out, prefix, log.LstdFlags)

	w.s = &s
	w.path = u.Path

	go w.listenAndServe(&s)

	return

}

func (w *WS) listenAndServe(s *http.Server) {

	// TODO (kostyarin): TLS

	if err := s.ListenAndServe(); err != http.ErrServerClosed {
		s.ErrorLog.Print("ListenAndServe error:", err)
	}

}

// Address returns listening address + path that
// accepts incomig HTTP requests.If the WS doesn't
// listen the result is empty
func (w *WS) Address() (address string) {

	w.mx.Lock()
	defer w.mx.Unlock()

	if w.s == nil {
		return
	}

	address = w.s + w.path
	return
}

// Conenct to remote peer using websockets protocol.
func (w *WS) Connect(address string) (c *Conn, err error) {

	//

}

func NewNode() {

	// http + ws

	if conf.HTTP.Listen != "" {
		err = n.WS().Listen(conf.HTTP.Listen, conf.HTTP.WSPath,
			conf.HTTP.HTTPPath)

		if err != nil {
			return
		}
	}

}

func (n *Node) getWS() (w *WS) {
	n.mx.Lock()
	defer n.mx.Unlock()

	return n.ws // including HTTP
}

// WS returns websockets transport of the Node
func (n *Node) WS() (ws *WS) {

	n.mx.Lock()
	defer n.mx.Unlock()

	n.createWS()

	return n.ws
}

var websocketUpgrader = websocket.Upgrader{
	ReadBufferSize:  4096,
	WriteBufferSize: 4096,
	CheckOrigin: func(*http.Request) bool {
		return true // allow all
	},
}

// WebsocketsHandler can be used to handle HTTP requests and
// upgrade tehm to websockets. If you are planing to use websockets
// but want own HTTP-server with own path use this HandlerFunc
func (n *Node) WebsocketsHandler(w http.ResponseWriter, r *http.Request) {

	var wc, err = websocketUpgrader.Upgrade(w, r, nil)

	if err != nil {
		n.Print("[ERR] websockets handling error:", err)
		return
	}

	defer wc.Close()

	// TOOD

	/*


		func echo() {
			c, err := upgrader.Upgrade(w, r, nil)
			if err != nil {
				log.Print("upgrade:", err)
				return
			}
			defer c.Close()
			for {
				mt, message, err := c.ReadMessage()
				if err != nil {
					log.Println("read:", err)
					break
				}
				log.Printf("recv: %s", message)
				err = c.WriteMessage(mt, message)
				if err != nil {
					log.Println("write:", err)
					break
				}
			}
		}

	*/

}

// call under lock of the mx
func (n *Node) createWS() {

	if n.ws != nil {
		return // alrady created
	}

	n.ws = newWS(n)

}
