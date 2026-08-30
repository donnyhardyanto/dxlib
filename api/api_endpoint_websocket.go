package api

import (
	"net/http"
	"runtime/debug"
	"sync"
	"time"

	"github.com/donnyhardyanto/dxlib/errors"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

// DXAPIEndPointWebSocketClient represents one connected WebSocket client.
type DXAPIEndPointWebSocketClient struct {
	Id   string
	Conn *websocket.Conn
	Send chan []byte
}

var wsUpgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

// handleWebSocket upgrades the HTTP connection to WebSocket and delegates
// to the endpoint's OnWSLoop. Called from routeHandler when EndPointType == EndPointTypeWS.
func (a *DXAPI) handleWebSocket(w http.ResponseWriter, r *http.Request, aepr *DXAPIEndPointRequest) {
	p := aepr.EndPoint

	// Run middlewares (auth, rate limit, etc.) before upgrading
	for _, mw := range p.Middlewares {
		if err := mw(aepr); err != nil {
			return
		}
		if aepr.ResponseHeaderSent {
			return
		}
	}

	// Optional pre-upgrade hook — useful for token validation before upgrading
	if p.OnExecute != nil {
		if err := p.OnExecute(aepr); err != nil {
			return
		}
		if aepr.ResponseHeaderSent {
			return
		}
	}

	// Upgrade HTTP → WebSocket
	conn, err := wsUpgrader.Upgrade(w, r, nil)
	if err != nil {
		aepr.Log.Errorf(err, "WS_UPGRADE_FAILED")
		return
	}
	aepr.ResponseHeaderSent = true // prevent further HTTP writes

	client := &DXAPIEndPointWebSocketClient{
		Id:   uuid.New().String(),
		Conn: conn,
		Send: make(chan []byte, 256),
	}
	aepr.WSClient = client

	p.wsRegister(client)
	defer p.wsUnregister(client)

	switch {
	case p.OnWSLoop != nil:
		// An endpoint that wants the whole lifecycle to itself still can.
		if err := p.OnWSLoop(aepr); err != nil {
			aepr.Log.Errorf(err, "WS_LOOP_ERROR")
		}
	case p.OnWSMessage != nil:
		p.runWSLoop(aepr)
	default:
		aepr.Log.Warn("WS_ENDPOINT_WITHOUT_HANDLER")
	}

	_ = conn.Close()
}

// runWSLoop is the lifecycle every WebSocket endpoint would otherwise write for
// itself: open, a write pump draining Send alongside a periodic tick, a read
// loop, and close on the way out however it ends.
func (p *DXAPIEndPoint) runWSLoop(aepr *DXAPIEndPointRequest) {
	client := aepr.WSClient

	if p.OnWSOpen != nil {
		if err := p.OnWSOpen(aepr); err != nil {
			aepr.Log.Errorf(err, "WS_OPEN_ERROR")
			return
		}
	}
	if p.OnWSClose != nil {
		defer p.OnWSClose(aepr)
	}

	writerDone := make(chan struct{})
	go func() {
		defer func() {
			if r := recover(); r != nil {
				aepr.Log.Errorf(errors.Errorf("%v", r), "WS_WRITE_PUMP_PANIC: %s", string(debug.Stack()))
			}
		}()
		interval := p.WSPeriodicInterval
		if interval <= 0 {
			interval = 30 * time.Second
		}
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case message, ok := <-client.Send:
				if !ok {
					return
				}
				if err := client.Conn.WriteMessage(websocket.TextMessage, message); err != nil {
					return
				}
			case <-ticker.C:
				// Runs on this goroutine, so a hook that writes the connection
				// itself is still serialised against the sends above.
				if p.OnWSPeriodic != nil {
					if err := p.OnWSPeriodic(aepr); err != nil {
						return
					}
				} else if err := client.Conn.WriteMessage(websocket.PingMessage, nil); err != nil {
					// With nothing of its own to send, a ping is what keeps an
					// idle connection from being reaped by whatever sits between.
					return
				}
			case <-writerDone:
				return
			case <-aepr.Context.Done():
				return
			}
		}
	}()
	defer close(writerDone)

	for {
		_, message, err := client.Conn.ReadMessage()
		if err != nil {
			return
		}
		response, err := p.OnWSMessage(aepr, message)
		if err != nil {
			aepr.Log.Errorf(err, "WS_MESSAGE_ERROR")
			continue
		}
		if len(response) > 0 {
			// Through Send, never straight to the connection: gorilla allows one
			// writer and the pump is it. Writing here would be a second.
			select {
			case client.Send <- response:
			case <-aepr.Context.Done():
				return
			}
		}
	}
}

// wsClientSet is the endpoint's connected clients. It is reached through a
// pointer so copies of an endpoint share one set and no lock is ever copied.
type wsClientSet struct {
	mu      sync.RWMutex
	clients map[*DXAPIEndPointWebSocketClient]bool
}

var wsClientSetMu sync.Mutex

func (p *DXAPIEndPoint) wsClientSetEnsure() *wsClientSet {
	wsClientSetMu.Lock()
	defer wsClientSetMu.Unlock()
	if p.wsClients == nil {
		p.wsClients = &wsClientSet{clients: map[*DXAPIEndPointWebSocketClient]bool{}}
	}
	return p.wsClients
}

func (p *DXAPIEndPoint) wsRegister(client *DXAPIEndPointWebSocketClient) {
	set := p.wsClientSetEnsure()
	set.mu.Lock()
	defer set.mu.Unlock()
	set.clients[client] = true
}

func (p *DXAPIEndPoint) wsUnregister(client *DXAPIEndPointWebSocketClient) {
	set := p.wsClientSetEnsure()
	set.mu.Lock()
	defer set.mu.Unlock()
	delete(set.clients, client)
	close(client.Send)
}

// WSClients returns the endpoint's currently connected clients.
func (p *DXAPIEndPoint) WSClients() []*DXAPIEndPointWebSocketClient {
	set := p.wsClientSetEnsure()
	set.mu.RLock()
	defer set.mu.RUnlock()
	clients := make([]*DXAPIEndPointWebSocketClient, 0, len(set.clients))
	for client := range set.clients {
		clients = append(clients, client)
	}
	return clients
}

// WSBroadcast queues a message for every connected client, skipping any whose
// buffer is already full rather than blocking the caller on a slow reader.
//
// The read lock is held across the sends. wsUnregister closes Send under the
// write lock, and a closed channel is ready rather than blocked, so a send
// racing an unregister would take the default arm on a live client and panic on
// a closed one. Holding the lock is what keeps the channel open for the send.
func (p *DXAPIEndPoint) WSBroadcast(message []byte) (delivered int) {
	set := p.wsClientSetEnsure()
	set.mu.RLock()
	defer set.mu.RUnlock()
	for client := range set.clients {
		select {
		case client.Send <- message:
			delivered++
		default:
		}
	}
	return delivered
}
