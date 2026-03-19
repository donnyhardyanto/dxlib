package api

import (
	"net/http"

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

	if p.OnWSLoop != nil {
		if err := p.OnWSLoop(aepr); err != nil {
			aepr.Log.Errorf(err, "WS_LOOP_ERROR")
		}
	}

	_ = conn.Close()
}
