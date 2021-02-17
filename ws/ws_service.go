package ws

import (
	"context"
	"encoding/json"
	"kafka-backned/config"
	"kafka-backned/provider"
	"kafka-backned/store"
	"net"
	"net/http"

	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

type WsService struct {
	configure         *config.Configure     `di.inject:"appConfigure"`
	providerSvc       *provider.Provider    `di.inject:"providerService"`
	storeSvc          *store.RethinkService `di.inject:"storeService"`
	connections       map[uuid.UUID]net.Conn
	serveContextClose context.CancelFunc
}

func (wsService *WsService) Close() error {
	log.Info("Terminate socket")

	for id := range wsService.connections {
		wsService.closeSocket(id)
	}

	return nil
}

func (wsService *WsService) Serve(writer http.ResponseWriter, request *http.Request) {
	var (
		id           uuid.UUID
		err          error
		serveContext context.Context
	)

	if len(wsService.connections) == 0 {
		serveContext, wsService.serveContextClose = context.WithCancel(context.Background())
		storeMsgChan := make(chan store.Message)

		log.Info("Serve provider and store")
		wsService.providerSvc.Serve(serveContext, storeMsgChan)
		wsService.storeSvc.Serve(serveContext, storeMsgChan)
	}

	if id, err = wsService.initConnection(writer, request); err != nil {
		log.Warnf("Error init connection for '%s': %s", request.URL, err.Error())
		log.Warn(err.Error())
		return
	}

	wsSocketContext, wsSocketCancel := context.WithCancel(context.Background())
	wsCmdReqChan := wsService.handleInput(id, wsSocketCancel)
	wsService.handleOutput(id, wsCmdReqChan, wsSocketContext)
}

func (wsService *WsService) initConnection(writer http.ResponseWriter, request *http.Request) (uuid.UUID, error) {
	log.Debugf("Upgrade connection for request: %s", request.RequestURI)

	conn, _, _, err := ws.UpgradeHTTP(request, writer)
	if err != nil {
		log.Error("Upgrade connection error")
		return uuid.UUID{}, err
	}

	id := uuid.New()
	log.Infof("Create '%s' connection", id.String())
	if wsService.connections == nil {
		wsService.connections = map[uuid.UUID]net.Conn{}
	}

	wsService.connections[id] = conn
	return id, nil
}

func (wsService *WsService) handleInput(id uuid.UUID, socketCancel context.CancelFunc) <-chan MessageRequest {
	var (
		wsCommandChan = make(chan MessageRequest)
		request       MessageRequest
	)

	go func() {
		for {
			select {
			case <-wsService.configure.GlobalContext.Done():
				close(wsCommandChan)
				return
			default:
				msg, opCode, _ := wsutil.ReadClientData(wsService.connections[id])
				log.Infof("Get msg from client '%s': %s", id, string(msg))

				if opCode == ws.OpClose || opCode == ws.OpContinuation {
					log.Infof("Get closed command for connection '%s'", id)
					socketCancel()
					return
				}

				_ = json.Unmarshal(msg, &request)
				wsCommandChan <- request
			}
		}
	}()

	return wsCommandChan
}

//storeMsgChan - ok (get msg chan for socket)
//wsCmdRequestChan - ok (socket requests)
func (wsService *WsService) handleOutput(id uuid.UUID, wsCmdReqChan <-chan MessageRequest, wsSocketContext context.Context) {
	go func() {
		startTopicChan := make(chan interface{}, 1)
		filterChan := make(chan func(message store.Message) bool, 1)

		wsMsgChan := wsService.storeSvc.Messages(wsSocketContext, filterChan)
		wsTopicChan := wsService.storeSvc.Topics(wsSocketContext, startTopicChan)
		defer wsService.closeSocket(id)

		for {
			select {
			case <-wsService.configure.GlobalContext.Done():
				return

			case <-wsSocketContext.Done():
				return

			case message, ok := <-wsMsgChan:
				if !ok {
					log.Debug("Message channel was closed")
					return
				}

				log.Debugf("Get message from channel: %s", toJson(message))
				if err := wsutil.WriteServerMessage(wsService.connections[id], ws.OpText, toJson(ConvertToWsMessage(message))); err != nil {
					log.Errorf("WsSocket: failed to write message to '%s'. Err: %s", id, err.Error())
					return
				}

			case msg, ok := <-wsTopicChan:
				if !ok {
					log.Debug("Topic channel was closed")
					return
				}

				log.Debugf("Get topics from channel: %s", toJson(msg))
				if err := wsutil.WriteServerMessage(wsService.connections[id], ws.OpText, toJson(ConvertToWsTopic(msg))); err != nil {
					log.Errorf("WsSocket: failed to write message to '%s'. Err: %s", id, err.Error())
					return
				}

			case cmd, ok := <-wsCmdReqChan:
				if !ok {
					log.Debug("Ws Command Request channel was closed")
					return
				}
				log.Debugf("Ws Command Request channel has msg: %s", cmd)

				switch cmd.Command {
				case WsCommandTypeTopics:
					startTopicChan <- 0
				case WsCommandTypeMessages:
					filterChan <- EvaluateFilter(cmd)
				}
			}
		}
	}()
}

func (wsService *WsService) closeSocket(id uuid.UUID) {
	log.Infof("Close '%s' connection", id)

	if con, ok := wsService.connections[id]; ok {
		con.Close()
		delete(wsService.connections, id)
	}

	if len(wsService.connections) == 0 {
		wsService.serveContextClose()
	}
}

func toJson(message interface{}) []byte {
	res, err := json.Marshal(message)
	if err != nil {
		return []byte("")
	}
	return res
}
