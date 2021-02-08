package repository

import "time"

type Message struct {
	Topic     string    `rethinkdb:"topic"`
	Headers   []byte    `rethinkdb:"headers"`
	Offset    int64     `rethinkdb:"offset"`
	Partition int       `rethinkdb:"partition"`
	Timestamp int64     `rethinkdb:"timestamp"`
	At        time.Time `rethinkdb:"at"`
	Size      int       `rethinkdb:"size"`
	Message   []byte    `rethinkdb:"message"`
}
