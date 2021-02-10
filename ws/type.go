package ws

type WsCommand int

const (
	Topics WsCommand = iota
	Messages
)

func (d WsCommand) String() string {
	return [...]string{"topics", "messages"}[d]
}

func ValueOf(value string) WsCommand {
	if value == "topics" {
		return Topics
	}
	return Messages
}
