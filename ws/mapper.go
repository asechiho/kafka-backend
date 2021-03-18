package ws

import (
	"encoding/json"
	"kafka-backned/store"
	"strconv"
	"strings"
	"time"
)

var messageFilterFields = map[string]string{
	"topic":     "string",
	"offset":    "int",
	"partition": "int",
	"timestamp": "int",
	"at":        "string",
	"size":      "int",
}

func ConvertToWsMessage(message store.Message) Messages {
	var headers = map[string]string{}
	_ = json.Unmarshal(message.Headers, &headers)

	var body = map[string]interface{}{}
	_ = json.Unmarshal(message.Message, &body)

	return Messages{
		Message: Message{
			Topic:       message.Topic,
			Headers:     headers,
			Offset:      strconv.FormatInt(int64(message.Offset), 10),
			Partition:   string(rune(message.Partition)),
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

func ConvertToStoreFilter(request MessageRequest) (result store.Filters) {
	var (
		comparator Comparator
		types      string
		ok         bool
	)

	if len(request.Filters) == 0 {
		return store.Filters{}
	}

	for _, filter := range request.Filters {
		if filter.Param == "topic" {
			result.Topic = filter.Value
		}

		comparator = StringComparator{filter.Operator}
		if types, ok = messageFilterFields[strings.ToLower(filter.Param)]; ok {
			comparator = New(filter.Operator, types)
		}

		result.Filters = append(result.Filters, store.Filter{
			FieldName:  filter.Param,
			FieldValue: filter.Value,
			Comparator: comparator,
		})
	}
	return
}
