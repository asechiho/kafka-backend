package ws

import (
	"encoding/json"
	"kafka-backned/store"
	"time"
)

func ConvertToWsMessage(message store.Message) Messages {
	var headers = map[string]string{}
	_ = json.Unmarshal(message.Headers, &headers)

	var body = map[string]string{}
	_ = json.Unmarshal(message.Message, &body)

	return Messages{
		Message: Message{
			Topic:       message.Topic,
			Headers:     headers,
			Offset:      message.Offset,
			Partition:   message.Partition,
			Timestamp:   message.Timestamp,
			At:          message.At.Format(time.RFC3339),
			PayloadSize: message.Size,
			Payload:     body,
		},
	}
}

func ConvertToWsTopic(message store.Message) Topic {
	return Topic{
		Topic: Message{
			Topic: message.Topic,
		},
	}
}

//todo: implement filter (message fields)
func EvaluateFilter(request MessageRequest) func(message store.Message) bool {
	if len(request.Filters) > 0 {
		val := request.Filters[0].Value
		return func(message store.Message) bool {
			return message.Topic == val
		}
	}
	return func(message store.Message) bool {
		return true
	}
}
