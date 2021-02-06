package main

import (
	"github.com/goioc/di"
	"kafka-backned/http"
	"kafka-backned/kaf"
	"reflect"
)

func init() {
	_, _ = di.RegisterBean("kafkaService", reflect.TypeOf((*kaf.KafkaService)(nil)))
	_, _ = di.RegisterBean("httpService", reflect.TypeOf((*http.HttpService)(nil)))
	_ = di.InitializeContainer()
}
