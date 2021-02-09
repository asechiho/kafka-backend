package main

import (
	"context"
	"github.com/goioc/di"
	log "github.com/sirupsen/logrus"
	"kafka-backned/config"
	"kafka-backned/provider"
	"kafka-backned/store"
	"kafka-backned/ws"
	"net/http"
	"os"
	"os/signal"
	"reflect"
	"syscall"
)

func main() {
	var (
		ctx    context.Context
		cancel context.CancelFunc
		err    error
	)
	logInit()

	ctx, cancel = context.WithCancel(context.Background())
	initContainers(ctx)

	if _, err = di.GetInstance("appConfigure").(*config.Configure).LoadConfig(); err != nil {
		log.Error(err.Error())
	}

	go listenTerminate(cancel)

	http.HandleFunc("/", di.GetInstance("wsService").(*ws.WsService).Read)
	log.Fatal(http.ListenAndServe(":8888", nil))
}

func listenTerminate(close context.CancelFunc) {
	var (
		stopChan = make(chan os.Signal, 1)
		signals  = []os.Signal{syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT}
	)
	signal.Notify(stopChan, signals...)

	log.Debug((<-stopChan).String())
	close()
	signal.Stop(stopChan)
}

func initContainers(ctx context.Context) {
	_, _ = di.RegisterBeanInstance("appContext", ctx)
	_, _ = di.RegisterBeanInstance("appConfig", new(config.Config).Defaults())
	_, _ = di.RegisterBean("appConfigure", reflect.TypeOf((*config.Configure)(nil)))
	_, _ = di.RegisterBean("wsService", reflect.TypeOf((*ws.WsService)(nil)))
	_, _ = di.RegisterBean("providerService", reflect.TypeOf((*provider.Provider)(nil)))
	_, _ = di.RegisterBean("storeService", reflect.TypeOf((*store.RethinkService)(nil)))
	_ = di.InitializeContainer()

	di.GetInstance("wsService").(*ws.WsService).TerminateConnections()
	if err := di.GetInstance("storeService").(*store.RethinkService).InitializeContext(); err != nil {
		log.Fatalf("Db error: %s", err.Error())
	}
}

func logInit() {
	log.SetFormatter(&log.JSONFormatter{})
	log.SetOutput(os.Stdout)
	log.SetLevel(log.InfoLevel)
}
