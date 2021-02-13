package store

import (
	log "github.com/sirupsen/logrus"
	rethink "gopkg.in/rethinkdb/rethinkdb-go.v6"
	"kafka-backned/config"
)

const (
	dbName    = "topics"
	tableName = "message"
	index     = "topic"
)

type Service interface {
	Topics() <-chan Message
	Messages() <-chan Message
	Serve(<-chan Message)
}

type RethinkService struct {
	configure   *config.Configure `di.inject:"appConfigure"`
	isDbCreated bool
}

func (rethinkService *RethinkService) Topics(startChan <-chan interface{}) <-chan Message {
	var (
		cursor  *rethink.Cursor
		err     error
		msgChan = make(chan Message, 1)
		msg     Message
	)

	go func() {
		session := rethinkService.connect()
		defer session.Close()

		for {
			select {
			case <-rethinkService.configure.Context.Done():
				log.Info("Close rethinkDb connection")
				close(msgChan)
				return
			case <-startChan:
				//todo:
				log.Debug("Start topics channel")
				if cursor, err = rethink.Table(tableName).Pluck(index).Distinct().Run(session); err != nil {
					log.Error(err.Error())
				}

				for cursor.Next(&msg) {
					msgChan <- msg
				}
			}
		}
	}()

	return msgChan
}

func (rethinkService *RethinkService) Messages(filterChan <-chan map[string]interface{}) <-chan Message {
	var (
		cursor  *rethink.Cursor
		err     error
		changes Changes
		msgChan = make(chan Message, 1)
	)

	go func() {
		term := rethink.Table(tableName).Changes().Limit(1)
		session := rethinkService.connect()
		defer session.Close()

		for {
			select {
			case <-rethinkService.configure.Context.Done():
				log.Info("Close rethinkDb connection")
				close(msgChan)
				return

			case curFilter := <-filterChan:
				//todo: filter implement. Changes ?
				log.Debugf("get filters: %s", curFilter)
				term = rethink.Table(tableName).Changes().Limit(1).Filter(curFilter)

			default:
				if cursor, err = term.Run(session); err != nil {
					log.Errorf("RethinkDb get changes error: %s", err.Error())
				}

				for cursor.Next(&changes) {
					msgChan <- changes.NewValue
				}
			}
		}
	}()

	return msgChan
}

func (rethinkService *RethinkService) Serve(c <-chan Message) {
	go func() {
		session := rethinkService.connect()
		defer session.Close()

		for {
			select {
			case <-rethinkService.configure.Context.Done():
				log.Info("Close rethinkDb connection")
				return
			case msg, ok := <-c:
				if !ok {
					return
				}
				_, err := rethink.Table(tableName).Insert(msg).RunWrite(session)
				if err != nil {
					log.Warnf("Insert message error: %s", err.Error())
				}
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
		if err = rethink.DBCreate(dbName).Exec(session); err != nil {
			return err
		}
	}
	rethinkService.isDbCreated = true

	if dbResponse, err = rethink.DB(dbName).TableList().Contains(tableName).Run(session); err != nil {
		return err
	}

	dbResponse.Next(&isContains)
	if !isContains {
		if err = rethink.DB(dbName).TableCreate(tableName).Exec(session); err != nil {
			return err
		}
	}

	if dbResponse, err = rethink.DB(dbName).Table(tableName).IndexList().Contains(index).Run(session); err != nil {
		return err
	}

	dbResponse.Next(&isContains)
	if !isContains {
		if err = rethink.DB(dbName).Table(tableName).IndexCreate(index).Exec(session); err != nil {
			return err
		}
		_ = rethink.DB(dbName).Table(tableName).IndexWait().Exec(session)
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
