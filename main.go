package main

import (
	"context"
	"github.com/goioc/di"
	"kafka-backned/config"
	"log"
	"net/http"
	"os"
	"os/signal"
	"reflect"
	"syscall"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	di.RegisterBeanInstance("appConfig", new(config.Config).Defaults())
	di.RegisterBeanInstance("appContext", ctx)
	di.RegisterBean("appConfigure", reflect.TypeOf((*config.Configure)(nil)))
	di.InitializeContainer()

	go listenTerminate(cancel)

	log.Fatal(http.ListenAndServe(":8888", nil))
}

func listenTerminate(close context.CancelFunc) {
	var (
		stopChan = make(chan os.Signal, 1)
		signals  = []os.Signal{syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT}
	)
	signal.Notify(stopChan, signals...)

	log.Print((<-stopChan).String())
	close()
	signal.Stop(stopChan)
}
