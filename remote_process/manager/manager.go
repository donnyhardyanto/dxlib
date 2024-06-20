package manager

import (
	"context"
	"github.com/go-redis/redis/v8"
	"github.com/gofiber/contrib/websocket"
	"github.com/gofiber/fiber/v2"
	"log"
)

type DXRemoteProcessClient struct {
	Owner        *DXRemoteProcessManager
	NameId       string
	IsConfigured bool
	Address      string
	Connection   *redis.Ring
	Connected    bool
	Context      context.Context
}

type DXRemoteProcessManagerInstance struct {
	Owner   *DXRemoteProcessManager
	Address string
}

type DXRemoteProcessManager struct {
	Clients          map[string]*DXRemoteProcessClient
	ManagerInstances map[string]*DXRemoteProcessManagerInstance
}

func (m *DXRemoteProcessManagerInstance) Execute() {
	f := fiber.New()

	f.Use("/ws", func(c *fiber.Ctx) error {
		// IsWebSocketUpgrade returns true if the client
		// requested upgrade to the WebSocket protocol.
		if websocket.IsWebSocketUpgrade(c) {
			c.Locals("allowed", true)
			return c.Next()
		}
		return fiber.ErrUpgradeRequired
	})

	f.Get("/ws/:id", websocket.New(func(c *websocket.Conn) {
		// c.Locals is added to the *websocket.Conn
		log.Println(c.Locals("allowed"))  // true
		log.Println(c.Params("id"))       // 123
		log.Println(c.Query("v"))         // 1.0
		log.Println(c.Cookies("session")) // ""

		// websocket.Conn bindings https://pkg.go.dev/github.com/fasthttp/websocket?tab=doc#pkg-index
		var (
			mt  int
			msg []byte
			err error
		)
		for {
			if mt, msg, err = c.ReadMessage(); err != nil {
				log.Println("read:", err)
				break
			}
			log.Printf("recv: %s", msg)

			if err = c.WriteMessage(mt, msg); err != nil {
				log.Println("write:", err)
				break
			}
		}

	}))

	log.Fatal(f.Listen(":3000"))
}

var Manager DXRemoteProcessManager

func init() {
	Manager = DXRemoteProcessManager{Clients: map[string]*DXRemoteProcessClient{}}
}
