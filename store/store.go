package store

import (
	"context"
	"github.com/google/uuid"
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
	configure      *config.Configure `di.inject:"appConfigure"`
	connectionPool map[uuid.UUID]*rethink.Session
}

func (rethinkService *RethinkService) Topics(socketContext context.Context, startChan <-chan interface{}) <-chan Message {
	var (
		cursor  *rethink.Cursor
		err     error
		msgChan = make(chan Message, 1)
		topic   string
	)

	go func() {
		id := rethinkService.connect(true)
		defer rethinkService.close(id)

		termTopics := rethink.Table(tableName).Distinct(rethink.DistinctOpts{
			Index: index,
		})

		for {
			select {
			case <-socketContext.Done():
				log.Info("Close rethinkDb connection for read topics")
				close(msgChan)
				return

			case <-rethinkService.configure.GlobalContext.Done():
				log.Info("Close rethinkDb connection for read topics")
				close(msgChan)
				return

				//todo: refactor
			case <-startChan:
				if cursor, err = termTopics.Run(rethinkService.connectionPool[id]); err != nil {
					log.Error(err.Error())
				}

				for cursor.Next(&topic) {
					msgChan <- Message{Topic: topic}
				}
			}
		}
	}()

	return msgChan
}

func (rethinkService *RethinkService) Messages(socketContext context.Context, filterChan <-chan func(message Message) bool) <-chan Message {
	var (
		cursor    *rethink.Cursor
		err       error
		changes   Changes
		msgChan   = make(chan Message, 1)
		curFilter func(message Message) bool
	)

	go func() {
		term := rethink.Table(tableName).Changes().Limit(1)
		id := rethinkService.connect(true)
		defer rethinkService.close(id)

		go rethinkService.getLastMessages(id, rethink.Table(tableName).OrderBy(rethink.Desc("timestamp")), msgChan, nil)

		for {
			select {
			case <-socketContext.Done():
				log.Info("Close rethinkDb connection for read messages")
				close(msgChan)
				return

			case <-rethinkService.configure.GlobalContext.Done():
				log.Info("Close rethinkDb connection for read messages")
				close(msgChan)
				return

			case curFilter := <-filterChan:
				//todo: filter implement. Changes ?
				log.Debugf("get filters: %s", curFilter)
				rethinkService.getLastMessages(id, rethink.Table(tableName).OrderBy(rethink.Desc("timestamp")), msgChan, curFilter)

			default:
				if cursor, err = term.Run(rethinkService.connectionPool[id]); err != nil {
					log.Errorf("RethinkDb get changes error: %s", err.Error())
					continue
				}

				for cursor.Next(&changes) {
					if curFilter == nil || curFilter(changes.NewValue) {
						msgChan <- changes.NewValue
					}
				}
			}
		}
	}()

	return msgChan
}

func (rethinkService *RethinkService) Serve(ctx context.Context, storeMsgChan <-chan Message) {
	go func() {
		id := rethinkService.connect(true)
		defer rethinkService.close(id)

		for {
			select {
			case <-ctx.Done():
				return

			case <-rethinkService.configure.GlobalContext.Done():
				log.Info("Close rethinkDb connection")
				return

			case msg, ok := <-storeMsgChan:
				if !ok {
					return
				}

				err := rethink.Table(tableName).Insert(msg).Exec(rethinkService.connectionPool[id])
				if err != nil {
					log.Warnf("Insert message error: %s", err.Error())
				}
			}
		}
	}()
}

func (rethinkService *RethinkService) InitializeContext() (err error) {
	// Create DB
	rethinkService.connectionPool = make(map[uuid.UUID]*rethink.Session)

	id := rethinkService.connect(false)
	err = rethinkService.executeCreateIfAbsent(rethink.DBList().Contains(dbName), rethink.DBCreate(dbName), id)
	rethinkService.close(id)

	// Create Table And Index
	id = rethinkService.connect(true)
	err = rethinkService.executeCreateIfAbsent(rethink.TableList().Contains(tableName), rethink.TableCreate(tableName), id)
	err = rethinkService.executeCreateIfAbsent(rethink.Table(tableName).IndexList().Contains(index), rethink.Table(tableName).IndexCreate(index), id)

	_ = rethink.Table(tableName).IndexWait().Exec(rethinkService.connectionPool[id])
	rethinkService.close(id)

	return
}

func (rethinkService *RethinkService) connect(isDbCreated bool) uuid.UUID {
	var (
		session *rethink.Session
		err     error
	)

	connectOpts := rethink.ConnectOpts{
		Address: rethinkService.configure.Config.DbAddress,
	}

	if isDbCreated {
		connectOpts.Database = dbName
	}

	if session, err = rethink.Connect(connectOpts); err != nil {
		log.Fatalf("Open rethinkDb connection error: %s", err.Error())
	}

	id := uuid.New()
	rethinkService.connectionPool[id] = session

	return id
}

func (rethinkService *RethinkService) close(id uuid.UUID) {
	rethinkService.connectionPool[id].Close()
	delete(rethinkService.connectionPool, id)
}

func (rethinkService *RethinkService) executeCreateIfAbsent(listTerm rethink.Term, createTerm rethink.Term, id uuid.UUID) error {
	var (
		isContains bool
		dbResponse *rethink.Cursor
		err        error
	)

	if dbResponse, err = listTerm.Run(rethinkService.connectionPool[id]); err != nil {
		return err
	}

	dbResponse.Next(&isContains)
	if !isContains {
		if err = createTerm.Exec(rethinkService.connectionPool[id]); err != nil {
			return err
		}
	}

	return nil
}

func (rethinkService *RethinkService) getLastMessages(id uuid.UUID, term rethink.Term, msgChan chan Message, filter func(message Message) bool) {
	cursor, err := term.Limit(20).Run(rethinkService.connectionPool[id])
	if err != nil {
		log.Warnf("Get desc error: %s", err.Error())
		return
	}

	var msg Message
	for cursor.Next(&msg) {
		if filter == nil || filter(msg) {
			msgChan <- msg
		}
	}
}
