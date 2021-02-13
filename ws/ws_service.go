package ws

import (
	"encoding/json"
	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"kafka-backned/config"
	"kafka-backned/provider"
	"kafka-backned/store"
	"net"
	"net/http"
)

type WsService struct {
	configure    *config.Configure     `di.inject:"appConfigure"`
	providerSvc  *provider.Provider    `di.inject:"providerService"`
	storeSvc     *store.RethinkService `di.inject:"storeService"`
	connections  map[uuid.UUID]net.Conn
	done         chan interface{}
	storeMsgChan chan store.Message
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
		id  uuid.UUID
		err error
	)

	if len(wsService.connections) == 0 {
		wsService.done = make(chan interface{})
		wsService.storeMsgChan = make(chan store.Message)

		wsService.providerSvc.Serve(wsService.storeMsgChan, wsService.done)
		wsService.storeSvc.Serve(wsService.storeMsgChan)
	}

	if id, err = wsService.initConnection(writer, request); err != nil {
		log.Warnf("Error init connection for '%s': %s", request.URL, err.Error())
		log.Warn(err.Error())
		return
	}

	go func() {
		wsCmdReqChan := wsService.handleInput(id)
		wsService.handleOutput(id, wsCmdReqChan)
	}()
}

func (wsService *WsService) logAndClose(err error, id uuid.UUID) {
	log.Warn(err.Error())
	log.Info("Close connection...")
	wsService.getConnection(id).Close()
	delete(wsService.connections, id)

	if len(wsService.connections) == 0 {
		wsService.done <- true
		close(wsService.storeMsgChan)
	}
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

func (wsService *WsService) handleInput(id uuid.UUID) <-chan MessageRequest {
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
				msg, _, _ := wsutil.ReadClientData(wsService.getConnection(id))
				log.Infof("Get msg from client '%s': %s", id, string(msg))

				_ = json.Unmarshal(msg, &request)
				wsCommandChan <- request
			}
		}
	}()

	return wsCommandChan
}

//storeMsgChan - ok (get msg chan for socket)
//wsCmdRequestChan - ok (socket requests)
func (wsService *WsService) handleOutput(id uuid.UUID, wsCmdReqChan <-chan MessageRequest) {
	go func() {
		startTopicChan := make(chan interface{}, 1)
		filterChan := make(chan map[string]interface{}, 1)

		wsMsgChan := wsService.storeSvc.Messages(filterChan)
		wsTopicChan := wsService.storeSvc.Topics(startTopicChan)

		for {
			select {
			case <-wsService.configure.Context.Done():
				return

			case msg, ok := <-wsTopicChan:
				if !ok {
					log.Debug("Topic channel was closed")
					return
				}

				log.Debugf("Get topics from channel: %s", toJson(msg))
				if err := wsutil.WriteServerMessage(wsService.getConnection(id), ws.OpText, toJson(ConvertToWsTopic(msg))); err != nil {
					wsService.logAndClose(err, id)
					return
				}

			case message, ok := <-wsMsgChan:
				if !ok {
					log.Debug("Message channel was closed")
					return
				}

				log.Debugf("Get message from channel: %s", toJson(message))
				if err := wsutil.WriteServerMessage(wsService.getConnection(id), ws.OpText, toJson(ConvertToWsMessage(message))); err != nil {
					wsService.logAndClose(err, id)
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

func (wsService *WsService) getConnection(id uuid.UUID) net.Conn {
	return wsService.connections[id]
}

func toJson(message interface{}) []byte {
	res, err := json.Marshal(message)
	if err != nil {
		return []byte("")
	}
	return res
}
