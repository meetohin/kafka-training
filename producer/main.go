package main

import (
	"fmt"
	"github.com/IBM/sarama"
	"log"
	"math/rand"
	"os"
	"time"
)

func main() {
	log.Println("Starting Producer!!!")

	kafkaBroker := os.Getenv("KAFKA_BROKER")
	if kafkaBroker == "" {
		kafkaBroker = "localhost:9092"
	}

	cfg := sarama.NewConfig()
	cfg.Producer.Return.Successes = true
	cfg.Producer.RequiredAcks = sarama.WaitForAll

	producer, err := sarama.NewSyncProducer([]string{kafkaBroker}, cfg)
	if err != nil {
		log.Fatalf("Create producer error: %v", err)
	}
	defer producer.Close()

	messages := []string{
		"Hello from producer",
		"Kafka is a power",
		"Fuck you bitch",
		"I really cool man",
		"Belgorod is a capital of the galaxy",
	}

	for i := 1; i < 6; i++ {
		messageText := messages[rand.Intn(5)]

		message := &sarama.ProducerMessage{
			Topic: "messages-topic",
			Key:   sarama.StringEncoder(fmt.Sprintf("key-%d", i)),
			Value: sarama.StringEncoder(messageText),
		}

		partition, offset, err := producer.SendMessage(message)
		if err != nil {
			log.Fatalf("Cannot send messge %d error: %v", i, err)
		} else {
			log.Printf("✅ Message %d send: partition=%d, offset=%d", i, partition, offset)
			log.Printf("   Text: %s", messageText)
		}

		time.Sleep(1 * time.Second)
	}

	log.Println("All messages has been send...")
	time.Sleep(2 * time.Second)
}
