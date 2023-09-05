package main

import (
	"fmt"
	"log"
	"time"

	"github.com/IBM/sarama"
)

func main() {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer([]string{"localhost:9092"}, config)
	if err != nil {
		log.Fatalf("Error creating producer: %v", err)
	}
	defer producer.Close()

	topic := "purchases" // Ganti dengan nama topik Kafka yang Anda inginkan

	for {
		message := "hello world"
		producerMessage := &sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.StringEncoder(message),
		}

		partition, offset, err := producer.SendMessage(producerMessage)
		if err != nil {
			log.Fatalf("Error sending message: %v", err)
		}

		fmt.Printf("Message sent to partition %d at offset %d\n", partition, offset)

		time.Sleep(1 * time.Second) // Menunggu 1 detik sebelum mengirim pesan berikutnya
	}
}
