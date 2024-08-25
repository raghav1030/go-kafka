package main

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/IBM/sarama"
	"github.com/gofiber/fiber/v3"
)

type Comment struct {
	Text string `form:"text" json:"text"`
}

func main() {
	app := fiber.New()
	api := app.Group("api/v1")

	api.Post("/comment", createComment)
	api.Post("/test", testKafka)
	app.Listen(":3000")
}

func createComment(ctx fiber.Ctx) error {
	cmt := new(Comment)

	if err := ctx.Bind().Body(cmt); err != nil {
		log.Println(err)
		return ctx.Status(400).JSON(&fiber.Map{
			"error":   err.Error(),
			"success": false,
		})
	}

	cmtInBytes, err := json.Marshal(cmt)
	if err != nil {
		log.Println(err)
		return ctx.Status(500).JSON(&fiber.Map{
			"error":   err.Error(),
			"success": false,
		})
	}

	err = PushCommentToQueue("comments", cmtInBytes)
	if err != nil {
		return ctx.Status(500).JSON(&fiber.Map{
			"error":   err.Error(),
			"success": false,
		})
	}

	return ctx.Status(200).JSON(&fiber.Map{
		"success": true,
		"message": "Comment pushed to queue successfully",
		"comment": cmt,
	})
}

func ConnectProducer(brokersUrl []string) (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5

	conn, err := sarama.NewSyncProducer(brokersUrl, config)
	if err != nil {
		return nil, err
	}

	return conn, nil
}

func PushCommentToQueue(topic string, message []byte) error {
	brokersUrl := []string{"localhost:29092"}

	producer, err := ConnectProducer(brokersUrl)
	if err != nil {
		log.Println(err)
		return err
	}
	defer producer.Close()

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(message),
	}

	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		log.Println(err)
		return err
	}

	fmt.Printf("Message is stored in topic(%s)/partition(%d)/offset(%d)\n", topic, partition, offset)
	return nil
}


func testKafka(ctx fiber.Ctx) error{
	cmt := new(Comment)

	if err := ctx.Bind().Body(cmt); err != nil {
		log.Println(err)
		return ctx.Status(400).JSON(&fiber.Map{
			"error":   err.Error(),
			"success": false,
		})
	}

	cmtInBytes, err := json.Marshal(cmt)
	if err != nil {
		log.Println(err)
		return ctx.Status(500).JSON(&fiber.Map{
			"error":   err.Error(),
			"success": false,
		})
	}

	err = PushCommentToQueue("test", cmtInBytes)
	if err != nil {
		return ctx.Status(500).JSON(&fiber.Map{
			"error":   err.Error(),
			"success": false,
		})
	}

	return ctx.Status(200).JSON(&fiber.Map{
		"success": true,
		"message": "Comment pushed to queue successfully",
		"comment": cmt,
	})
}