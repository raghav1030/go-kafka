package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/IBM/sarama"
)

func main() {
	topic1 := "comments"
	topic2 := "test"

	workerURL := []string{"localhost:29092"}
	worker, err := connectWorker(workerURL)

	if err != nil {
		panic(err)
	}

	consumer1, err := worker.ConsumePartition(topic1, 0, sarama.OffsetOldest)
	
	if err != nil {
		panic(err)
	}
	
	consumer2, err := worker.ConsumePartition(topic2, 0, sarama.OffsetOldest)
	
	if err != nil {
		panic(err)
	}

	fmt.Println("consumer started")

	signchan := make(chan os.Signal, 1)
	signal.Notify(signchan, syscall.SIGINT, syscall.SIGTERM)

	doneChan := make(chan struct{})

	commentMessageCount := 0
	testMessageCount := 0
	go func() {
		for {
			select {
			case err := <-consumer1.Errors():
				fmt.Println(err)

			case err := <-consumer2.Errors():
				fmt.Println(err)

			case cmtMsg := <-consumer1.Messages():
				commentMessageCount++
				fmt.Printf("Received message Count: %d | Topic: %s | Message: %s\n ", commentMessageCount, string(cmtMsg.Topic), string(cmtMsg.Value))
			case testMsg := <-consumer2.Messages():
				testMessageCount++
				fmt.Printf("Received message Count: %d | Topic: %s | Message: %s\n ", testMessageCount, string(testMsg.Topic), string(testMsg.Value))
			case <-signchan:
				fmt.Println("Interruption detected")
				doneChan <- struct{}{}
			}
		}
	}()

	<-doneChan
	if err = worker.Close(); err != nil {
		panic(err)
	}

}

func connectWorker(workerURL []string) (sarama.Consumer, error) {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	conn, err := sarama.NewConsumer(workerURL, config)

	if err != nil {
		log.Fatal(err)
		return nil, err
	}

	return conn, nil
}
