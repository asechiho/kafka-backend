package kaf

import (
	"bytes"
	"encoding/json"
	"kafka-backned/repository"
	"log"
	"time"
)

type DTOTransformer struct{}

func (self DTOTransformer) ConvertMessageToDb(message Message) repository.Message {
	headers, err := json.Marshal(message.Headers)
	if err != nil {
		headers = []byte{}
	}

	at, err := time.Parse(TimeLayout, message.At)
	if err != nil {
		at = time.Time{}
	}

	return repository.Message{
		Topic:     message.Topic,
		Headers:   headers,
		Offset:    message.Offset,
		Partition: message.Partition,
		Timestamp: message.Timestamp,
		At:        at,
		Size:      message.PayloadSize,
		Message:   bytes.NewBufferString(message.Payload).Bytes(),
	}
}

func (self DTOTransformer) ConvertMessageToKafka(message repository.Message) Message {
	var (
		headers = map[string]string{}
		err     error
	)
	if err = json.Unmarshal(message.Headers, &headers); err != nil {
		log.Printf("Unmarsal header %s error: %s", string(message.Headers), err.Error())
	}

	return Message{
		Topic:       message.Topic,
		Headers:     headers,
		Offset:      message.Offset,
		Partition:   message.Partition,
		Timestamp:   message.Timestamp,
		At:          message.At.Format(TimeLayout),
		PayloadSize: message.Size,
		Payload:     string(message.Message),
	}
}
