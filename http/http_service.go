package http

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"
	"kafka-backned/kaf"
	"log"
	"net"
	"net/http"
)

type Service interface {
	ListTopics(writer http.ResponseWriter, request *http.Request)
	ReadMessages(writer http.ResponseWriter, request *http.Request)
}

type HttpService struct {
	kafSvc *kaf.KafkaService `di.inject:"kafkaService"`
}

func (self *HttpService) ListTopics(writer http.ResponseWriter, request *http.Request) {
	var (
		message  []string
		response []byte
		err      error
	)

	if message, err = self.kafSvc.ListTopics(); err != nil {
		log.Print(err.Error())
	}

	if response, err = json.Marshal(message); err != nil {
		response = jsonError(err)
	}

	if _, err = writer.Write(response); err != nil {
		log.Fatal(err.Error())
	}
}

func (self *HttpService) ReadMessages(writer http.ResponseWriter, request *http.Request) {
	var (
		messages []kaf.Message
		response []byte
		err      error
	)

	var process = func() ([]byte, error) {
		var topic = request.URL.Query().Get("topic")

		if messages, err = self.kafSvc.ReadMessages(topic); err != nil {
			return nil, err
		}

		if response, err = json.Marshal(messages); err != nil {
			response = jsonError(err)
		}
		return response, nil
	}

	handleServerSentEvent(writer, request, process)
}

func handleServerSentEvent(writer http.ResponseWriter, request *http.Request, process func() ([]byte, error)) {
	conn, _, _, err := ws.UpgradeHTTP(request, writer)
	if err != nil {
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
