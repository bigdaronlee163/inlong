package main

import (
	"fmt"
	"github.com/apache/inlong/inlong-tubemq/tubemq-client-twins/tubemq-client-go/client"
	"github.com/apache/inlong/inlong-tubemq/tubemq-client-twins/tubemq-client-go/config"
	"github.com/apache/inlong/inlong-tubemq/tubemq-client-twins/tubemq-client-go/log"
	"github.com/apache/inlong/inlong-tubemq/tubemq-client-twins/tubemq-client-go/tdmsg"
)

func main() {

	cfg, err := config.ParseAddress("127.0.0.1:8715?topic=test_topic")
	if err != nil {
		log.Errorf("Failed to parse address", err.Error())
		panic(err)
	}
	p, err := client.NewProducer(cfg)
	if err != nil {
		log.Errorf("new consumer error %s", err.Error())
		panic(err)
	}
	// start := time.Now()
	msg := &tdmsg.Message{}
	msgRst, err := p.SendMessage(msg)
	if err != nil {
		return
	}
	fmt.Printf("%v\n", msgRst)
}
