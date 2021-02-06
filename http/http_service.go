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

func ListTopics(writer http.ResponseWriter, request *http.Request) {
	var (
		message  []string
		response []byte
		err      error
	)

	if message, err = kaf.ListTopics(); err != nil {
		log.Printf(err.Error())
	}

	if response, err = json.Marshal(message); err != nil {
		response = jsonError(err)
	}

	if _, err = writer.Write(response); err != nil {
		log.Fatal(err.Error())
	}
}

func ReadMessages(writer http.ResponseWriter, request *http.Request) {
	var (
		message  []kaf.Message
		response []byte
		offset   = 0
		err      error
	)

	var process = func() ([]byte, error) {
		var topic = request.URL.Query().Get("topic")

		if message, err = kaf.ReadMessages(topic, offset); err != nil {
			return nil, err
		}

		offset = offset + len(message)
		if response, err = json.Marshal(message); err != nil {
			response = jsonError(err)
		}
		return response, nil
	}

	handleServerSentEvent(writer, request, process)
}

func handleServerSentEvent(writer http.ResponseWriter, request *http.Request, process func() ([]byte, error)) {
	conn, _, _, err := ws.UpgradeHTTP(request, writer)
	if err != nil {
		log.Printf(err.Error())
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
	log.Printf(err.Error())
	log.Printf("Close connection...")
	conn.Close()
}

func jsonError(err error) []byte {
	return bytes.NewBufferString(fmt.Sprintf(`{"error": "%s"}`, err.Error())).Bytes()
}
