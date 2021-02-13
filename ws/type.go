package ws

//go:generate go-enum -f=$GOFILE --marshal
//ENUM(
//topics
//messages
//)
type WsCommandType uint

//ENUM(
//eq
//ne
//gt
//ge
//lt
//le
//)
type OperatorType uint

type Filter struct {
	Param    string       `json:"parameter"`
	Operator OperatorType `json:"operator"`
	Value    string       `json:"value"`
}

type MessageRequest struct {
	Command WsCommandType `json:"request"`
	Filters []Filter      `json:"filters,omitempty"`
}

type Message struct {
	Topic       string            `json:"topic"`
	Headers     map[string]string `json:"headers"`
	Offset      int64             `json:"offset"`
	Partition   int32             `json:"partition"`
	Timestamp   int64             `json:"timestamp"`
	At          string            `json:"at"`
	PayloadSize int               `json:"payloadSize"`
	Payload     map[string]string `json:"message"`
}

type Topic struct {
	Messages Message `json:"topic"`
}

type Messages struct {
	Message Message `json:"message"`
}
