package store

import (
	"bytes"
	"encoding/json"
	"github.com/segmentio/kafka-go"
	log "github.com/sirupsen/logrus"
	"time"
)

type Message struct {
	Topic     string    `rethinkdb:"topic"`
	Headers   []byte    `rethinkdb:"headers"`
	Offset    int64     `rethinkdb:"offset"`
	Partition int       `rethinkdb:"partition"`
	Timestamp int64     `rethinkdb:"timestamp"`
	At        time.Time `rethinkdb:"at"`
	Size      int       `rethinkdb:"size"`
	Message   []byte    `rethinkdb:"message"`
}

type Changes struct {
	OldValue Message `rethinkdb:"old_val"`
	NewValue Message `rethinkdb:"new_val"`
}

func New(msg kafka.Message) Message {
	var (
		dbHeaders []byte
		err       error
	)

	keyValue := map[string]string{}
	for _, header := range msg.Headers {
		keyValue[header.Key] = string(header.Value)
	}

	if dbHeaders, err = json.Marshal(keyValue); err != nil {
		log.Warnf("Marshal header error: %s", err.Error())
		dbHeaders = []byte(`{}`)
	}

	return Message{
		Topic:     msg.Topic,
		Headers:   dbHeaders,
		Offset:    msg.Offset,
		Partition: msg.Partition,
		Timestamp: msg.Time.Unix(),
		At:        msg.Time,
		Size:      len(msg.Value),
		Message:   bytes.NewBufferString(string(msg.Value)).Bytes(),
	}
}
