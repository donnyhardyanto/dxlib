package client

import (
	"context"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
	"golang.org/x/sync/errgroup"

	"github.com/donnyhardyanto/dxlib/core"
	utilsHttpClient "github.com/donnyhardyanto/dxlib/utils/http/client"
)

type DXWSClient struct {
	NameId string
}

type DXWSClientManager struct {
	Context           context.Context
	Cancel            context.CancelFunc
	WSClient          map[string]*DXWSClient
	ErrorGroup        *errgroup.Group
	ErrorGroupContext context.Context
}

var Manager DXWSClientManager

func init() {
	ctx, cancel := context.WithCancel(core.RootContext)
	Manager = DXWSClientManager{
		Context:  ctx,
		Cancel:   cancel,
		WSClient: map[string]*DXWSClient{},
	}
}

// NewDialer is websocket.DefaultDialer plus the process's outbound TLS
// settings: the same client certificate and trust source every HTTP client in
// the process uses, from the "http-client" configuration. With no such
// configuration TLSClientConfig is nil and the dialer is DefaultDialer in all
// but name, so a wss:// dial verifies against the system roots as it always
// did and a ws:// dial is unaffected either way.
//
// gorilla does its own handshake on the TCP connection and never offers ALPN,
// so a server that has HTTP/2 enabled -- as this library's does -- still sees
// an HTTP/1.1 upgrade from this dialer.
func NewDialer() *websocket.Dialer {
	return &websocket.Dialer{
		Proxy:            http.ProxyFromEnvironment,
		HandshakeTimeout: 45 * time.Second,
		TLSClientConfig:  utilsHttpClient.TLSClientConfig(),
	}
}

// Dial opens a WebSocket connection with NewDialer. A wss:// URL to a server
// that requires a client certificate works only when the "http-client"
// configuration carries one; the error otherwise is the server's
// bad-certificate alert, and the server's log names the reason.
func Dial(ctx context.Context, url string, requestHeader http.Header) (*websocket.Conn, *http.Response, error) {
	conn, resp, err := NewDialer().DialContext(ctx, url, requestHeader)
	if err != nil {
		utilsHttpClient.LogHandshakeFailure(nil, url, err)
	}
	return conn, resp, err
}
