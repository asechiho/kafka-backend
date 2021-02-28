package ws

import (
	"encoding/json"
	"kafka-backned/store"
	"strconv"
	"strings"
	"time"
)

func ConvertToWsMessage(message store.Message) Messages {
	var headers = map[string]string{}
	_ = json.Unmarshal(message.Headers, &headers)

	var body = map[string]interface{}{}
	_ = json.Unmarshal(message.Message, &body)

	return Messages{
		Message: Message{
			Topic:       message.Topic,
			Headers:     headers,
			Offset:      strconv.FormatInt(message.Offset, 10),
			Partition:   string(message.Partition),
			Timestamp:   strconv.FormatInt(message.Timestamp, 10),
			At:          message.At.Format(time.RFC3339),
			PayloadSize: strconv.Itoa(message.Size),
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

func ConvertToStoreFilter(request MessageRequest) store.Filter {
	if len(request.Filters) == 0 {
		return store.Filter{}
	}

	return store.Filter{
		FieldName:  request.Filters[0].Param,
		FieldValue: request.Filters[0].Value,
		Comparator: func(left interface{}, right interface{}) bool {
			return strings.EqualFold(left.(string), right.(string))
		},
	}
}
