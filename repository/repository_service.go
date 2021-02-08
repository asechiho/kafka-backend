package repository

import (
	r "gopkg.in/rethinkdb/rethinkdb-go.v6"
	"log"
	"strings"
)

const dbName = "topics"

func New() *RepositoryService {
	// create DB if not exist
	var (
		session *r.Session
		cursor  *r.Cursor
		value   []byte
		err     error
	)
	session = connect()
	defer session.Close()

	cursor, err = r.DBList().Contains(dbName).Run(session)
	if err != nil {
		log.Print(err.Error())
	}

	value, _ = cursor.NextResponse()
	if string(value) != "true" {
		if _, err = r.DBCreate(dbName).Run(session); err != nil {
			log.Fatalf("Database was not created: %s", err.Error())
		}
	}

	return &RepositoryService{}
}

type Service interface {
	Insert(topicName string, message Message) (Message, error)
	List(topicName string) ([]Message, error)
}

type RepositoryService struct {
}

func (self *RepositoryService) Insert(topicName string, message Message) (int, error) {
	var (
		session    *r.Session
		wrResponse r.WriteResponse
		err        error
		//todo
		topic = strings.Replace(topicName, ".", "_", -1)
	)
	session = connect()
	defer session.Close()

	self.InitializeTable(topic, session)

	if wrResponse, err = r.DB(dbName).Table(topic).Insert(message).RunWrite(session); err != nil {
		log.Print(err.Error())
	}

	return wrResponse.Inserted, nil
}

func (self *RepositoryService) List(topicName string) ([]Message, error) {
	var (
		messages []Message
		session  *r.Session
		cursor   *r.Cursor
		err      error
		//todo
		topic = strings.Replace(topicName, ".", "_", -1)
	)
	session = connect()
	defer session.Close()

	if self.InitializeTable(topic, session) {
		return []Message{}, nil
	}

	if cursor, err = r.DB(dbName).Table(topic).GetAll().Run(session); err != nil {
		log.Print(err.Error())
		return nil, err
	}

	var row Message
	for cursor.Next(row) {
		messages = append(messages, row)
	}

	return messages, nil
}

func (self *RepositoryService) InitializeTable(tableName string, session *r.Session) bool {
	var (
		cursor *r.Cursor
		value  []byte
		err    error
	)

	if cursor, err = r.DB(dbName).TableList().Contains(tableName).Run(session); err != nil {
		log.Fatalf(err.Error())
	}

	value, _ = cursor.NextResponse()
	//todo: invest
	if string(value) != "true" {
		_, _ = r.DB(dbName).TableCreate(tableName).Run(session)
		return true
	}

	return false
}

func connect() *r.Session {
	var (
		session *r.Session
		err     error
	)

	if session, err = r.Connect(r.ConnectOpts{
		Address: "localhost:28015",
	}); err != nil {
		log.Fatalln(err.Error())
	}

	return session
}
