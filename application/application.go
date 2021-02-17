package application

import (
	"context"
	"github.com/goioc/di"
	log "github.com/sirupsen/logrus"
	"kafka-backned/ws"
	"net/http"
	"os"
	"os/signal"
	"syscall"
)

type Application struct {
	cancel context.CancelFunc
	socket *ws.WsService `di.inject:"wsService"`
}

func New(cancel context.CancelFunc) *Application {
	return &Application{
		cancel: cancel,
		socket: di.GetInstance("wsService").(*ws.WsService),
	}
}

func (application *Application) Run() error {
	http.HandleFunc("/", application.socket.Serve)
	go http.ListenAndServe(":9002", nil)
	return application.waitTerminate()
}

func (application *Application) Close() error {
	if err := application.socket.Close(); err != nil {
		return err
	}
	return nil
}

func (application *Application) waitTerminate() error {
	var (
		stopChan = make(chan os.Signal, 1)
		signals  = []os.Signal{syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT}
	)
	signal.Notify(stopChan, signals...)

	log.Infof("Wait terminate signal")
	log.Infof("Signal: %s", (<-stopChan).String())

	application.cancel()
	if err := application.Close(); err != nil {
		log.Warnf("Terminate error: %s", err.Error())
		return err
	}

	signal.Stop(stopChan)
	close(stopChan)
	return nil
}
