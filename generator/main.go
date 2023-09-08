package main

import (
	"log"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"time"

	"github.com/IBM/sarama"
)

func getEnvOrDefault(key string, defaultVal string) string {
	if val, ok := os.LookupEnv(key); ok {
		return val
	}
	return defaultVal
}

func main() {
	brokers := []string{getEnvOrDefault("KAFKA_BROKER", "kafkac:9092")}
	log.Printf("Kafka Broker List : %v\n", brokers)

	cardOwnersCountEnv := getEnvOrDefault("CARD_OWNERS_COUNT", "100")
	cardCountEnv := getEnvOrDefault("CARD_COUNT", "1000")
	seedEnv := getEnvOrDefault("SEED", "0")
	waitTimeMsEnv := getEnvOrDefault("WAIT_TIME_MS", "1000")

	cardOwnersCount, err := strconv.ParseInt(cardOwnersCountEnv, 10, 64)
	if err != nil {
		panic(err)
	}
	log.Printf("Card owners count: %d\n", cardOwnersCount)

	cardCount, err := strconv.ParseInt(cardCountEnv, 10, 64)
	if err != nil {
		panic(err)
	}
	log.Printf("Card count: %d\n", cardCount)

	waitTimeMs, err := strconv.ParseInt(waitTimeMsEnv, 10, 64)
	if err != nil {
		panic(err)
	}
	log.Printf("Wait time (ms): %d\n", waitTimeMs)

	seed, err := strconv.ParseInt(seedEnv, 10, 64)
	if err != nil {
		panic(err)
	}

	if seed != 0 {
		log.Printf("Seed: %d\n", seed)
	} else {
		log.Printf("Seed was not set or was set to zero, using rand/crypto")
	}

	config := sarama.NewConfig()

	config.Producer.RequiredAcks = sarama.WaitForAll

	config.Producer.Retry.Max = 5

	log.Println("Creating Kafka Producer...")

	producer, err := sarama.NewAsyncProducer(brokers, config)

	if err != nil {
		log.Fatal("Failed to create Kafka Producer : ", err)
	}

	topic := getEnvOrDefault("KAFKA_TOPIC", "transactions")

	deliveryReports := producer.Errors()

	signals := make(chan os.Signal, 1)

	signal.Notify(signals, os.Interrupt)

	wg := sync.WaitGroup{}

	wg.Add(1)

	go func() {
		defer wg.Done()
		for err := range deliveryReports {
			log.Printf("Failed to deliver message to Kafka: %v\n", err)
		}
	}()

	ts := NewTransactionSource(seed, cardOwnersCount, cardCount)

	go func() {
		for {
			waitTimeNs := waitTimeMs * 1_00_000
			<-time.After(time.Duration(waitTimeNs))
			transaction := ts.GetTranscations()
			messageValue, err := transaction.TOJSON()

			if err != nil {
				panic(err)
			}

			message := &sarama.ProducerMessage{
				Topic: topic,
				Value: sarama.StringEncoder(string(messageValue)),
			}

			producer.Input() <- message
			log.Printf("Message %s sent to Kafka topic: %s\n", string(messageValue), topic)
		}
	}()

	// Wait for OS signal for graceful shutdown
	<-signals

	// Close the producer and wait for delivery report goroutine to finish
	producer.AsyncClose()
	wg.Wait()

	log.Println("Kafka producer shut down gracefully")
}
