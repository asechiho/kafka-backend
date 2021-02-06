package main

import (
	topic "kafka-backned/http"
	"log"
	"net/http"
)

func main() {
	http.HandleFunc("/topics", topic.ListTopics)
	http.HandleFunc("/messages", topic.ReadMessages)

	//todo: add di
	log.Fatal(http.ListenAndServe(":8888", nil))
}
