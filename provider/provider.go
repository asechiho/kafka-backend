package provider

import (
	"github.com/segmentio/kafka-go"
	"kafka-backned/config"
)

type Provider struct {
	config *config.Config `di.inject:"appConfig"`
}

func (provider *Provider) Serve(topic string) error {
	var (
		reader *kafka.Reader
	)

	reader = kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{provider.config.Brokers},
		Topic:    topic,
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
	})

	go func() {
		defer reader.Close()

		//todo: add read and write messages

	}()

	return nil
}
