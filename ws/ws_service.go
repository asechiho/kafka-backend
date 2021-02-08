package ws

import (
	"bytes"
	"context"
	"fmt"
	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"
	"github.com/google/uuid"
	"log"
	"net"
	"net/http"
)

type WsService struct {
	connections map[uuid.UUID]net.Conn
}

func (wsService *WsService) handleSocket(writer http.ResponseWriter, request *http.Request, process func() ([]byte, error)) {
	var (
		conn net.Conn
		err  error
	)

	if conn, err = wsService.initConnection(writer, request); err != nil {
		log.Print(err.Error())
	}

	go func() {
		for {

			response, err := process()
			if err != nil {
				logAndClose(err, conn)
				return
			}

			err = wsutil.WriteServerMessage(conn, ws.OpText, response)
			if err != nil {
				logAndClose(err, conn)
				return
			}
		}
	}()
}

func logAndClose(err error, conn net.Conn) {
	log.Print(err.Error())
	log.Print("Close connection...")
	conn.Close()
}

func jsonError(err error) []byte {
	return bytes.NewBufferString(fmt.Sprintf(`{"error": "%s"}`, err.Error())).Bytes()
}

func (wsService *WsService) initConnection(writer http.ResponseWriter, request *http.Request) (net.Conn, error) {
	conn, _, _, err := ws.UpgradeHTTP(request, writer)
	if err != nil {
		return nil, err
	}

	id := uuid.New()
	log.Printf("Create '%s' connection", id.String())
	wsService.connections[id] = conn

	return conn, nil
}

func (wsService *WsService) terminateConnections(ctx context.Context) {
	go func() {
		<-ctx.Done()
		log.Print("Terminate socket")

		for id, conn := range wsService.connections {
			log.Printf("Close '%s' connection", id.String())
			conn.Close()
		}
	}()
}
