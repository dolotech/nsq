package main

import (
	"net"
	"github.com/nsqio/go-nsq"
	"log"
)

func main() {
	config := nsq.NewConfig()
	laddr := "127.0.0.1"

	config.LocalAddr, _ = net.ResolveTCPAddr("tcp", laddr+":0")

	w, _ := nsq.NewProducer("127.0.0.1:4150", config)
	//w.SetLogger(nullLogger, LogLevelInfo)

	err := w.Publish("write_test", []byte("test"))
	if err != nil {
		log.Fatalf("should lazily connect - %s", err)
	}

	w.Stop()

	err = w.Publish("write_test", []byte("fail test"))
	if err != nsq.ErrStopped {
		log.Fatalf("should lazily connect - %s", err)
	}
}
