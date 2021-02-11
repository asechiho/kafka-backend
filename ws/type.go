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
	Command WsCommandType `json:"command"`
	Filter  Filter        `json:"filter,omitempty"`
}
