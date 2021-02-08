package store

import (
	"kafka-backned/provider"
)

type Service interface {
	Topics() <-chan provider.Message
	Messages() <-chan provider.Message
}

type RethinkService struct {
}

func (rethinkService *RethinkService) Topics() <-chan provider.Message {
	return nil
}

func (rethinkService *RethinkService) Messages() <-chan provider.Message {
	return nil
}
