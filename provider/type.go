package provider

type Message struct {
	Topic       string            `json:"topic"`
	Headers     map[string]string `json:"headers"`
	Offset      int64             `json:"offset"`
	Partition   int               `json:"partition"`
	Timestamp   int64             `json:"timestamp"`
	At          string            `json:"at"`
	PayloadSize int               `json:"payloadSize"`
	Payload     map[string]string `json:"message"`
}
