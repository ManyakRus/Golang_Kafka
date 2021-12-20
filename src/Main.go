package main

import (
	"fmt"
	//"github.com/Shopify/sarama"
	"gopkg.in/Shopify/sarama.v1"
	"log"
	"os"
	"time"
	//"os/signal"
	//"sync"
)

var Address = []string{"localhost:9092"}

//var Address = []string{"localhost:9092"}

func main() {
	syncProducer(Address)

	topic := "test"
	worker, err := connectConsumer(Address)
	//worker, err := connectConsumer([]string{"localhost:9092", "localhost:9092"})
	if err != nil {
		panic(err)
	}
	// calling ConsumePartition. It will open one connection per broker
	// and share it for all partitions that live on it.
	consumer, err := worker.ConsumePartition(topic, 0, sarama.OffsetOldest)

	m := <-consumer.Messages()
	println(m)
}

func connectConsumer(brokersUrl []string) (sarama.Consumer, error) {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	// NewConsumer creates a new consumer using the given broker addresses and configuration
	conn, err := sarama.NewConsumer(brokersUrl, config)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

// синхронный режим сообщений
func syncProducer(address []string) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.Timeout = 5 * time.Second
	p, err := sarama.NewSyncProducer(address, config)
	if err != nil {
		log.Printf("sarama.NewSyncProducer err, message=%s \n", err)
		return
	}
	defer p.Close()
	topic := "test"
	srcValue := "sync: this is a message. index=%d"
	for i := 0; i < 10; i++ {
		value := fmt.Sprintf(srcValue, i)
		msg := &sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.ByteEncoder(value),
		}
		part, offset, err := p.SendMessage(msg)
		if err != nil {
			log.Printf("send message(%s) err=%s \n", value, err)
		} else {
			fmt.Fprintf(os.Stdout, value+"Успешно отправлено, раздел =% d, смещение =% d \n", part, offset)
		}
		time.Sleep(0 * time.Second)
	}
}
