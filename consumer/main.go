package main

import (
	"context"
	"github.com/IBM/sarama"
	"log"
	"os"
	"os/signal"
	"sync"
	"time"
)

type Consumer struct {
	ready chan bool
}

func main() {
	log.Println("Starting Consumer!!!")

	kafkaBroker := os.Getenv("KAFKA_BROKER")
	if kafkaBroker == "" {
		kafkaBroker = "localhost:9092"
	}

	cfg := sarama.NewConfig()
	cfg.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	cfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	cfg.Consumer.Group.Session.Timeout = 10 * time.Second
	cfg.Consumer.Group.Heartbeat.Interval = 3 * time.Second

	consumerGroup := "simple-consumer-group"
	client, err := sarama.NewConsumerGroup([]string{kafkaBroker}, consumerGroup, cfg)
	if err != nil {
		log.Fatalf("Creating consumer group error: %v", err)
	}

	defer client.Close()

	consumer := Consumer{
		ready: make(chan bool),
	}

	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer wg.Done()
		for {
			if err := client.Consume(ctx, []string{"messages-topic"}, &consumer); err != nil {
				log.Printf("Consumer error: %v", err)
				return
			}
			if ctx.Err() != nil {
				log.Printf("Context error: %v", err)
				return
			}
			consumer.ready = make(chan bool)
		}
	}()

	<-consumer.ready
	log.Printf("Consumer ready for recive message")

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, os.Interrupt)

	<-sigterm
	log.Printf("Consumer-service is stopping...")
	cancel()
	wg.Wait()
	log.Printf("Consumer-service has stopped.")
}

// Setup вызывается при запуске consumer
func (consumer *Consumer) Setup(sarama.ConsumerGroupSession) error {
	close(consumer.ready)
	return nil
}

// Cleanup вызывается при остановке consumer
func (consumer *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim обрабатывает сообщения
func (consumer *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case message := <-claim.Messages():
			if message == nil {
				return nil
			}

			// Выводим полученное сообщение в лог
			log.Printf("📨 Получено сообщение:")
			log.Printf("   🔑 Ключ: %s", string(message.Key))
			log.Printf("   📝 Текст: %s", string(message.Value))
			log.Printf("   📍 Топик: %s, Партиция: %d, Offset: %d",
				message.Topic, message.Partition, message.Offset)
			log.Printf("   ⏰ Время: %v", message.Timestamp)

			// Помечаем сообщение как обработанное
			session.MarkMessage(message, "")

		case <-session.Context().Done():
			return nil
		}
	}
}
