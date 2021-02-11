package ws

import (
	"encoding/json"
	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"
	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
	log "github.com/sirupsen/logrus"
	"kafka-backned/config"
	"kafka-backned/provider"
	"kafka-backned/store"
	"net"
	"net/http"
)

type WsService struct {
	configure   *config.Configure     `di.inject:"appConfigure"`
	providerSvc *provider.Provider    `di.inject:"providerService"`
	storeSvc    *store.RethinkService `di.inject:"storeService"`
	connections map[uuid.UUID]net.Conn
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

func (wsService *WsService) Serve(writer http.ResponseWriter, request *http.Request) {
	var (
		conn net.Conn
		err  error
	)

	if conn, err = wsService.initConnection(writer, request); err != nil {
		log.Warnf("Error init connection for '%s': %s", request.URL, err.Error())
		log.Warn(err.Error())
		return
	}

	go func() {
		msgChan := wsService.storeSvc.Messages()
		wsCommandChan := wsService.handleInput(conn)
		go wsService.handleOutput(conn, wsCommandChan, msgChan)
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

func (wsService *WsService) handleInput(conn net.Conn) <-chan MessageRequest {
	var (
		wsCommandChan = make(chan MessageRequest)
		request       MessageRequest
	)

	go func() {
		for {
			select {
			case <-wsService.configure.Context.Done():
				close(wsCommandChan)
				return
			default:
				msg, _, _ := wsutil.ReadClientData(conn)
				log.Info(string(msg))

				_ = json.Unmarshal(msg, &request)
				wsCommandChan <- request
			}
		}
	}()

	return wsCommandChan
}

func (wsService *WsService) handleOutput(conn net.Conn, wsCommandChan <-chan MessageRequest, msgChan <-chan store.Message) {
	var (
		response []byte
		cmd      = MessageRequest{}
		ok       bool
	)

	wsCmdChan := make(chan kafka.Message, 1)
	changeTopicChan := make(chan string, 1)
	requestChan := make(chan string, 1)

	go wsService.providerSvc.Serve(wsCmdChan, changeTopicChan, requestChan)
	go wsService.storeSvc.Serve(wsCmdChan)

	for {
		select {
		case <-wsService.configure.Context.Done():
			close(changeTopicChan)
			return

		case cmd, ok = <-wsCommandChan:
			if !ok {
				return
			}

			switch cmd.Command {
			case WsCommandTypeTopics:
				requestChan <- (&cmd.Command).String()
			case WsCommandTypeMessages:
				changeTopicChan <- cmd.Filter.Value
			}

		case message, ok := <-msgChan:
			if !ok {
				return
			}

			//todo:
			var headers = map[string]string{}
			_ = json.Unmarshal(message.Headers, &headers)

			var body = map[string]string{}
			_ = json.Unmarshal(message.Message, &body)

			response, _ = json.Marshal(provider.Message{
				Topic:       message.Topic,
				Headers:     headers,
				Offset:      message.Offset,
				Partition:   message.Partition,
				Timestamp:   message.Timestamp,
				At:          message.At.Format("2021-02-09T22:37:55"),
				PayloadSize: message.Size,
				Payload:     body,
			})

			if err := wsutil.WriteServerMessage(conn, ws.OpText, response); err != nil {
				logAndClose(err, conn)
				return
			}
		}
	}
}
