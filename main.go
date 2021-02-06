package main

import (
	"github.com/goioc/di"
	topic "kafka-backned/http"
	"log"
	"net/http"
)

func main() {
	http.HandleFunc("/topics", di.GetInstance("httpService").(*topic.HttpService).ListTopics)
	http.HandleFunc("/messages", di.GetInstance("httpService").(*topic.HttpService).ReadMessages)
	log.Fatal(http.ListenAndServe(":8888", nil))
}
