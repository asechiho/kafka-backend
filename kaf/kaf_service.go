package kaf

import (
	"context"
	"github.com/segmentio/kafka-go"
	"kafka-backned/repository"
	"time"
)

type Service interface {
	ListTopics() ([]string, error)
	ReadMessages(topic string, offset int) ([]Message, error)
}

type KafkaService struct {
	repSvc         *repository.RepositoryService `di.inject:"repositoryService"`
	dtoTransformer *DTOTransformer               `di.inject:"dtoTransformer"`
}

func (self *KafkaService) ListTopics() ([]string, error) {
	var (
		conn       *kafka.Conn
		partitions []kafka.Partition
		topics     []string
		err        error
	)

	//todo: add flags
	if conn, err = kafka.Dial("tcp", "localhost:9092"); err != nil {
		return nil, err
	}
	defer conn.Close()

	if partitions, err = conn.ReadPartitions(); err != nil {
		return nil, err
	}

	for _, p := range partitions {
		if "__consumer_offsets" == p.Topic {
			continue
		}
		topics = append(topics, p.Topic)
	}

	return topics, nil
}

func (self *KafkaService) ReadMessages(topic string) ([]Message, error) {
	var (
		reader      *kafka.Reader
		err         error
		response    []Message
		kafResponse []repository.Message
		message     kafka.Message
	)

	//todo: add flags
	reader = kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{"localhost:9092"},
		Topic:    topic,
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
	})
	defer reader.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if kafResponse, err = self.repSvc.List(topic); err != nil {
		return nil, err
	}

	// todo: optimize
	for _, value := range kafResponse {
		response = append(response, self.dtoTransformer.ConvertMessageToKafka(value))
	}
	reader.SetOffset(int64(len(kafResponse)))

	for {
		if message, err = reader.ReadMessage(ctx); err != nil {
			break
		}

		msg := New(message)
		if _, err = self.repSvc.Insert(topic, self.dtoTransformer.ConvertMessageToDb(msg)); err != nil {
			return nil, err
		}
		response = append(response, msg)
	}

	return response, nil
}
