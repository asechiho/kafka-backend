package ws

import (
	"bytes"
	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"kafka-backned/config"
	"net"
	"net/http"
)

type WsService struct {
	configure   *config.Configure `di.inject:"appConfigure"`
	connections map[uuid.UUID]net.Conn
}

func (wsService *WsService) Read(writer http.ResponseWriter, request *http.Request) {
	wsService.handleSocket(writer, request, func() ([]byte, error) {
		return bytes.NewBufferString(`{"test": "test"}`).Bytes(), nil
	})
}

func (wsService *WsService) TerminateConnections() {
	go func() {
		<-wsService.configure.Context.Done()
		log.Info("Terminate socket")

		for id, conn := range wsService.connections {
			log.Infof("Close '%s' connection", id.String())
			conn.Close()
		}
	}()
}

func (wsService *WsService) handleSocket(writer http.ResponseWriter, request *http.Request, process func() ([]byte, error)) {
	var (
		conn     net.Conn
		response []byte
		err      error
	)

	if conn, err = wsService.initConnection(writer, request); err != nil {
		log.Warnf("Error init connection for '%s': %s", request.URL, err.Error())
		log.Warn(err.Error())
		return
	}

	go func() {
		for {
			select {
			case <-wsService.configure.Context.Done():
				return
			default:
				if response, err = process(); err != nil {
					logAndClose(err, conn)
					return
				}
			}

			select {
			case <-wsService.configure.Context.Done():
				return
			default:
				if err = wsutil.WriteServerMessage(conn, ws.OpText, response); err != nil {
					logAndClose(err, conn)
					return
				}
			}
		}
	}()
}

func logAndClose(err error, conn net.Conn) {
	log.Warn(err.Error())
	log.Info("Close connection...")
	conn.Close()
}

func (wsService *WsService) initConnection(writer http.ResponseWriter, request *http.Request) (net.Conn, error) {
	log.Debugf("Upgrade connection for request: %s", request.RequestURI)

	conn, _, _, err := ws.UpgradeHTTP(request, writer)
	if err != nil {
		log.Error("Upgrade connection error")
		return nil, err
	}

	id := uuid.New()
	log.Infof("Create '%s' connection", id.String())
	if wsService.connections == nil {
		wsService.connections = map[uuid.UUID]net.Conn{}
	}
	wsService.connections[id] = conn

	return conn, nil
}
