package provider

import (
	"github.com/prometheus/common/log"
	"github.com/segmentio/kafka-go"
	"kafka-backned/config"
)

type Provider struct {
	config *config.Configure `di.inject:"appConfigure"`
}

func (provider *Provider) Serve(c chan kafka.Message, topicChan <-chan string, requestChan <-chan string) {
	var (
		message kafka.Message
		err     error
		reader  *kafka.Reader
	)

	go func() {

		for {
			select {
			case <-provider.config.Context.Done():
				reader.Close()
				close(c)
				return

			case request := <-requestChan:
				if request == "topics" {
					provider.Topics(c)
				}

			case topicName := <-topicChan:
				if reader == nil {
					reader = kafka.NewReader(kafka.ReaderConfig{
						Brokers:  []string{provider.config.Config.Brokers},
						Topic:    topicName,
						MinBytes: 10e3, // 10KB
						MaxBytes: 10e6, // 10MB
					})
				}

				if reader.Config().Topic == topicName {
					continue
				}

				reader.Close()
				reader = kafka.NewReader(kafka.ReaderConfig{
					Brokers:  []string{provider.config.Config.Brokers},
					Topic:    topicName,
					MinBytes: 10e3, // 10KB
					MaxBytes: 10e6, // 10MB
				})

			default:
				if reader != nil {
					if message, err = reader.ReadMessage(provider.config.Context); err != nil {
						log.Warnf("Kafka read message: %s", err.Error())
					}
					c <- message
				}
			}
		}
	}()
}

func (provider *Provider) Topics(c chan kafka.Message) {
	conn, err := kafka.Dial("tcp", provider.config.Config.Brokers)
	if err != nil {
		log.Warn(err.Error())
	}
	defer conn.Close()

	partitions, err := conn.ReadPartitions()
	if err != nil {
		log.Warn(err.Error())
	}

	for _, v := range partitions {
		if v.Topic == "__consumer_offsets" {
			continue
		}

		c <- kafka.Message{
			Topic: v.Topic,
		}
	}
}
