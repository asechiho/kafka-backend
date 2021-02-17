package main

import (
	"context"
	"github.com/goioc/di"
	log "github.com/sirupsen/logrus"
	"kafka-backned/application"
	"kafka-backned/config"
	"kafka-backned/provider"
	"kafka-backned/store"
	"kafka-backned/ws"
	"os"
	"reflect"
)

func main() {
	var (
		ctx    context.Context
		cancel context.CancelFunc
		err    error
	)
	logInit()

	ctx, cancel = context.WithCancel(context.Background())
	app := initContainers(ctx, cancel)

	if _, err = di.GetInstance("appConfigure").(*config.Configure).LoadConfig(); err != nil {
		log.Error(err.Error())
	}

	if err = app.Run(); err != nil {
		os.Exit(1)
	}
	os.Exit(0)
}

func initContainers(ctx context.Context, cancel context.CancelFunc) *application.Application {
	_, _ = di.RegisterBeanInstance("appContext", ctx)
	_, _ = di.RegisterBeanInstance("appConfig", new(config.Config).Defaults())
	_, _ = di.RegisterBean("appConfigure", reflect.TypeOf((*config.Configure)(nil)))
	_, _ = di.RegisterBean("wsService", reflect.TypeOf((*ws.WsService)(nil)))
	_, _ = di.RegisterBean("providerService", reflect.TypeOf((*provider.Provider)(nil)))
	_, _ = di.RegisterBean("storeService", reflect.TypeOf((*store.RethinkService)(nil)))
	_ = di.InitializeContainer()

	if err := di.GetInstance("storeService").(*store.RethinkService).InitializeContext(); err != nil {
		log.Fatalf("Db error: %s", err.Error())
	}

	return application.New(cancel)
}

func logInit() {
	log.SetFormatter(&log.JSONFormatter{})
	log.SetOutput(os.Stdout)
	log.SetLevel(log.InfoLevel)
}
