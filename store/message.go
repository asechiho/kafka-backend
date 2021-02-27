package store

import (
	"bytes"
	"encoding/json"
	log "github.com/sirupsen/logrus"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
	"strconv"
	"time"
)

type Message struct {
	Topic     string    `rethinkdb:"topic"`
	Headers   []byte    `rethinkdb:"headers"`
	Offset    int64     `rethinkdb:"offset"`
	Partition int32     `rethinkdb:"partition"`
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
		offset    int64
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

	if offset, err = strconv.ParseInt(msg.TopicPartition.Offset.String(), 10, 64); err != nil {
		log.Warnf("Offset parse error: %s", err.Error())
		offset = 0
	}

	return Message{
		Topic:     *msg.TopicPartition.Topic,
		Headers:   dbHeaders,
		Offset:    offset,
		Partition: msg.TopicPartition.Partition,
		Timestamp: msg.Timestamp.Unix(),
		At:        msg.Timestamp,
		Size:      len(msg.Value),
		Message:   bytes.NewBufferString(string(msg.Value)).Bytes(),
	}
}
