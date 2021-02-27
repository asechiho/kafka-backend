package store

import (
	"context"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	rethink "gopkg.in/rethinkdb/rethinkdb-go.v6"
	"kafka-backned/config"
)

const (
	dbName       = "topics"
	tableName    = "message"
	index        = "topic"
	NewTopicChan = "topicChan"
)

type Service interface {
	Serve()
	Stop()
}

type RethinkService struct {
	configure      *config.Configure `di.inject:"appConfigure"`
	connectionPool map[uuid.UUID]*rethink.Session
	topics         []string
	newTopicChan   chan string
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
			case topic := <-rethinkService.newTopicChan:
				msgChan <- Message{Topic: topic}

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

		rethinkService.getLastMessages(id, msgChan, nil)

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
				//log.Debugf("get filters: %s", curFilter)
				rethinkService.getLastMessages(id, msgChan, curFilter)

			default:
				if cursor, err = term.Run(rethinkService.connectionPool[id]); err != nil {
					log.Errorf("RethinkDb get changes error: %s", err.Error())
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

func (rethinkService *RethinkService) Serve() {
	if err := rethinkService.InitializeContext(); err != nil {
		log.Fatalf("Db error: %s", err.Error())
	}

	go func() {
		id := rethinkService.connect(true)
		defer rethinkService.close(id)

		// init start topics
		var topic string
		cursor, _ := rethink.Table(tableName).Distinct(rethink.DistinctOpts{
			Index: index,
		}).Run(rethinkService.connectionPool[id])

		for cursor.Next(&topic) {
			rethinkService.topics = append(rethinkService.topics, topic)
		}

		for {
			select {
			case <-rethinkService.configure.GlobalContext.Done():
				return

			case msg, ok := <-rethinkService.configure.ServeReadChannel():
				if !ok {
					return
				}

				go rethinkService.appendTopic(msg.(Message).Topic)

				err := rethink.Table(tableName).Insert(msg.(Message)).Exec(rethinkService.connectionPool[id])
				if err != nil {
					log.Warnf("Insert message error: %s", err.Error())
				}
			}
		}
	}()
}

func (rethinkService *RethinkService) Stop() {
}

func (rethinkService *RethinkService) InitializeContext() error {
	var err error
	// Create DB
	rethinkService.connectionPool = make(map[uuid.UUID]*rethink.Session)

	id := rethinkService.connect(false)
	if err = rethinkService.executeCreateIfAbsent(rethink.DBList().Contains(dbName), rethink.DBCreate(dbName), id); err != nil {
		return err
	}

	rethinkService.close(id)

	// Create Table And Index
	id = rethinkService.connect(true)
	if err = rethinkService.executeCreateIfAbsent(rethink.TableList().Contains(tableName), rethink.TableCreate(tableName), id); err != nil {
		return err
	}

	if err = rethinkService.executeCreateIfAbsent(rethink.Table(tableName).IndexList().Contains(index), rethink.Table(tableName).IndexCreate(index), id); err != nil {
		return err
	}

	_ = rethink.Table(tableName).IndexWait().Exec(rethinkService.connectionPool[id])
	rethinkService.close(id)

	return nil
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

func (rethinkService *RethinkService) getLastMessages(id uuid.UUID, msgChan chan Message, filter func(message Message) bool) {
	cursor, err := rethink.Table(tableName).OrderBy(rethink.Desc("timestamp")).Limit(20).OrderBy(rethink.Asc("timestamp")).Run(rethinkService.connectionPool[id])
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

func (rethinkService *RethinkService) appendTopic(topic string) {
	for _, v := range rethinkService.topics {
		if v == topic {
			return
		}
	}
	rethinkService.topics = append(rethinkService.topics, topic)
	rethinkService.newTopicChan <- topic
}
