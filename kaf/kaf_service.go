package kaf

import (
	"context"
	"github.com/segmentio/kafka-go"
	"time"
)

func ListTopics() ([]string, error) {
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

func ReadMessages(topic string, offset int) ([]Message, error) {
	var (
		reader   *kafka.Reader
		err      error
		response []Message
		message  kafka.Message
	)

	//todo: add flags
	reader = kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{"localhost:9092"},
		Topic:    topic,
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
	})
	defer reader.Close()

	reader.SetOffset(int64(offset))

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	for {
		if message, err = reader.ReadMessage(ctx); err != nil {
			break
		}
		response = append(response, New(message))
	}

	return response, nil
}
