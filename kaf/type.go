package kaf

import (
	"encoding/base64"
	"github.com/segmentio/kafka-go"
)

type Message struct {
	Topic       string            "json:\"topic\""
	Headers     map[string]string "json:\"headers\""
	Offset      int64             "json:\"offset\""
	Partition   int               "json:\"partition\""
	Timestamp   int64             "json:\"timestamp\""
	At          string            "json:\"at\""
	PayloadSize int               "json:\"payloadSize\""
	Payload     string            "json:\"message\""
}

func New(msg kafka.Message) Message {
	var (
		headers = map[string]string{}
	)

	for _, header := range msg.Headers {
		headers[header.Key] = string(header.Value)
	}

	return Message{
		Topic:       msg.Topic,
		Headers:     headers,
		Offset:      msg.Offset,
		Partition:   msg.Partition,
		Timestamp:   msg.Time.Unix(),
		At:          msg.Time.Format("2021-02-05T10:02:40.617Z"),
		PayloadSize: len(msg.Value),
		Payload:     string(msg.Value),
	}
}

func decodeValue(value []byte) string {
	return base64.StdEncoding.EncodeToString(value)
}
