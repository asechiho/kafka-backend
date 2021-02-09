package store

import (
	"github.com/segmentio/kafka-go"
	log "github.com/sirupsen/logrus"
	rethink "gopkg.in/rethinkdb/rethinkdb-go.v6"
	"kafka-backned/config"
)

const dbName = "topics"
const tableName = "message"

type Service interface {
	Topics() <-chan Message
	Messages() <-chan Message
	MessagePusher(<-chan Message)
}

type RethinkService struct {
	configure   *config.Configure `di.inject:"appConfigure"`
	isDbCreated bool
}

func (rethinkService *RethinkService) Topics() <-chan Message {
	return nil
}

func (rethinkService *RethinkService) Messages() <-chan Message {
	var (
		cursor  *rethink.Cursor
		err     error
		changes Changes
		msgChan = make(chan Message, 1)
	)

	go func() {
		session := rethinkService.connect()
		defer session.Close()

		for {
			select {
			case <-rethinkService.configure.Context.Done():
				log.Info("Close rethinkDb connection")
				return
			default:
				if cursor, err = rethink.Table(tableName).Changes().Run(session); err != nil {
					log.Error(err.Error())
				}

				for cursor.Next(&changes) {
					msgChan <- changes.NewValue
				}
			}
		}
	}()

	return msgChan
}

func (rethinkService *RethinkService) Serve(c <-chan kafka.Message) {
	go func() {
		session := rethinkService.connect()
		defer session.Close()

		for {
			select {
			case <-rethinkService.configure.Context.Done():
				log.Info("Close rethinkDb connection")
				return
			case msg := <-c:
				_, err := rethink.Table(tableName).Insert(New(msg)).RunWrite(session)
				if err != nil {
					log.Warnf("Insert message error: %s", err.Error())
				}
			default:
			}
		}
	}()
}

func (rethinkService *RethinkService) InitializeContext() error {
	var (
		isContains bool
		dbResponse *rethink.Cursor
		err        error
		session    *rethink.Session
	)
	session = rethinkService.connect()
	if dbResponse, err = rethink.DBList().Contains(dbName).Run(session); err != nil {
		return err
	}

	dbResponse.Next(&isContains)
	if !isContains {
		if _, err = rethink.DBCreate(dbName).Run(session); err != nil {
			return err
		}
	}
	rethinkService.isDbCreated = true

	if dbResponse, err = rethink.DB(dbName).TableList().Contains(tableName).Run(session); err != nil {
		return err
	}

	dbResponse.Next(&isContains)
	if !isContains {
		if _, err = rethink.DB(dbName).TableCreate(tableName).Run(session); err != nil {
			return err
		}
	}

	return nil
}

func (rethinkService *RethinkService) connect() *rethink.Session {
	var (
		session *rethink.Session
		err     error
	)

	connectOpts := rethink.ConnectOpts{
		Address: rethinkService.configure.Config.DbAddress,
	}

	if rethinkService.isDbCreated {
		connectOpts.Database = dbName
	}

	if session, err = rethink.Connect(connectOpts); err != nil {
		log.Fatalf("Open rethinkDb connection error: %s", err.Error())
	}

	return session
}
