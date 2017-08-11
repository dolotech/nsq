package main

import (
	"github.com/nsqio/go-nsq"
	"log"
	"sync"
)
func (this *NSQHandler) HandleMessage(message *nsq.Message) error {
	log.Println("recv:", string(message.Body))
	return nil
}
type NSQHandler struct {
}
func main() {
	waiter := sync.WaitGroup{}
	waiter.Add(1)

	go func() {
		defer waiter.Done()

		consumer, err := nsq.NewConsumer("test", "ch1", nsq.NewConfig())
		if nil != err {
			log.Println(err)
			return
		}

		consumer.AddHandler(&NSQHandler{})
		err = consumer.ConnectToNSQD("127.0.0.1:4150")
		if nil != err {
			log.Println(err)
			return
		}

		select {}
	}()

	waiter.Wait()
}
