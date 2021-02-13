package provider

import (
	"github.com/prometheus/common/log"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
	"kafka-backned/config"
	"kafka-backned/store"
)

type Provider struct {
	config *config.Configure `di.inject:"appConfigure"`
}

func (provider *Provider) Serve(c chan store.Message, done <-chan interface{}) {
	var (
		consumer *kafka.Consumer
		err      error
		topics   []string
		message  *kafka.Message
	)

	go func() {
		if consumer, err = kafka.NewConsumer(&kafka.ConfigMap{
			//todo: groupId -> env|var
			"bootstrap.servers": provider.config.Config.Brokers,
			"group.id":          "kafka-ui-messages-fetch",
			"auto.offset.reset": "smallest",
		}); err != nil {
			log.Errorf("Failed connection to kafka: %s", err.Error())
			close(c)
			return
		}

		if topics, err = provider.topics(consumer); err != nil {
			log.Errorf("Couldn't get topics: %s", err.Error())
			close(c)
			return
		}

		for {
			_ = consumer.SubscribeTopics(topics, nil)

			select {
			case <-provider.config.Context.Done():
				provider.close(consumer, c)
				return

			case <-done:
				//todo: investigate
				provider.close(consumer, c)
				return

			default:
				if message, err = consumer.ReadMessage(-1); err != nil {
					log.Warnf("Kafka read message: %s", err.Error())
				}
				c <- store.New(*message)
			}
		}
	}()
}

func (provider *Provider) close(consumer *kafka.Consumer, c chan store.Message) {
	consumer.Close()
	close(c)
}

func (provider *Provider) topics(consumer *kafka.Consumer) (topics []string, err error) {
	meta, err := consumer.GetMetadata(nil, true, 3000)
	if err != nil {
		return
	}

	for _, v := range meta.Topics {
		if v.Topic == "__consumer_offsets" {
			continue
		}
		topics = append(topics, v.Topic)
	}
	return
}
