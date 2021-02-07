package main

import (
	"github.com/goioc/di"
	"kafka-backned/http"
	"kafka-backned/kaf"
	"kafka-backned/repository"
	"reflect"
)

func init() {
	_, _ = di.RegisterBean("kafkaService", reflect.TypeOf((*kaf.KafkaService)(nil)))
	_, _ = di.RegisterBean("httpService", reflect.TypeOf((*http.HttpService)(nil)))
	_, _ = di.RegisterBean("dtoTransformer", reflect.TypeOf((*kaf.DTOTransformer)(nil)))
	_, _ = di.RegisterBean("repositoryService", reflect.TypeOf(repository.New()))
	_ = di.InitializeContainer()
}
