package ws

import (
	"encoding/json"
	"fmt"
	"kafka-backned/provider"
)

type WsCommandType int

const (
	Topics WsCommandType = iota
	Messages
)

var WsCommandTypeToString = map[WsCommandType]string{
	Topics:   "topics",
	Messages: "messages",
}

var WsCommandTypeFromString = map[string]WsCommandType{
	"topics":   Topics,
	"messages": Messages,
}

func (cmd WsCommandType) String() string {
	if s, ok := WsCommandTypeToString[cmd]; ok {
		return s
	}
	return "unknown"
}

func (cmd WsCommandType) MarshalJSON() ([]byte, error) {
	if s, ok := WsCommandTypeToString[cmd]; ok {
		return json.Marshal(s)
	}
	return nil, fmt.Errorf("unknown type %d", cmd)
}

func (cmd *WsCommandType) UnmarshalJSON(value []byte) error {
	var s string
	if err := json.Unmarshal(value, &s); err != nil {
		return err
	}

	var v WsCommandType
	var ok bool
	if v, ok = WsCommandTypeFromString[s]; !ok {
		return fmt.Errorf("unknown type %s", s)
	}

	*cmd = v
	return nil
}

type MessageRequest struct {
	Command WsCommandType    `json:"command"`
	Message provider.Message `json:"message,omitempty"`
}
