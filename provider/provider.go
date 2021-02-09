package provider

import (
	"github.com/prometheus/common/log"
	"github.com/segmentio/kafka-go"
	"kafka-backned/config"
)

type Provider struct {
	config *config.Configure `di.inject:"appConfigure"`
}

func (provider *Provider) Serve(topic string, c chan kafka.Message) {
	var (
		message kafka.Message
		err     error
	)

	go func() {
		reader := kafka.NewReader(kafka.ReaderConfig{
			Brokers:  []string{provider.config.Config.Brokers},
			Topic:    topic,
			MinBytes: 10e3, // 10KB
			MaxBytes: 10e6, // 10MB
		})
		defer reader.Close()

		for {
			select {
			case <-provider.config.Context.Done():
				return
			default:
				if message, err = reader.ReadMessage(provider.config.Context); err != nil {
					log.Warnf("Kafka read message: ", err.Error())
				}
				c <- message
			}
		}
	}()
}
