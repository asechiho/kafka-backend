package config

import (
	"context"
	"github.com/heetch/confita"
	"github.com/heetch/confita/backend/env"
	"github.com/heetch/confita/backend/flags"
	log "github.com/sirupsen/logrus"
)

//todo: add config?
type Config struct {
	Brokers   string `config:"kafka-brokers" envconfig:"kafka-brokers"`
	GroupId   string `config:"kafka-group-id" envconfig:"kafka-group-id"`
	DbAddress string `config:"db-addr" envconfig:"db-addr"`
}

func (config *Config) Defaults() *Config {
	config.Brokers = "127.0.0.1:9092"
	config.GroupId = "kafka-ui-messages-fetch"
	config.DbAddress = "127.0.0.1:28015"
	return config
}

type Configure struct {
	GlobalContext    context.Context `di.inject:"appContext"`
	Config           *Config         `di.inject:"appConfig"`
	serveMessageChan chan interface{}
}

func (configure *Configure) ServeReadChannel() <-chan interface{} {
	return configure.serveMessageChan
}

func (configure *Configure) ServeWriteChannel() chan interface{} {
	return configure.serveMessageChan
}

func (configure *Configure) LoadConfig() (cfg *Configure, err error) {
	configure.serveMessageChan = make(chan interface{})

	if err = confita.NewLoader(flags.NewBackend(), env.NewBackend()).Load(context.Background(), configure.Config); err != nil {
		log.Warn("Error load config")
		return configure, err
	}

	return configure, nil
}
