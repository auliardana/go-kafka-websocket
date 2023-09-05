package main

import (
	"fmt"
	"log"

	"github.com/IBM/sarama"
	"kafka-go-getting-started/websocket"
	
	"github.com/gin-gonic/gin"


)

func main() {
	r := gin.Default()
	
	// WebSocket endpoint
	r.GET("/ws", websocket.HandleWebSocket)
	// Inisialisasi WebSocket server di awal
	go websocket.StartWebSocketServer()

	config := sarama.NewConfig()
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	consumer, err := sarama.NewConsumer([]string{"localhost:9092"}, config)
	if err != nil {
		log.Fatalf("Error creating consumer: %v", err)
	}
	defer consumer.Close()

	partitionConsumer, err := consumer.ConsumePartition("purchases", 0, sarama.OffsetOldest)
	if err != nil {
		log.Fatalf("Error creating partition consumer: %v", err)
	}
	defer partitionConsumer.Close()

	r.Run(":9999")

	num := 0
	for {
		select {
		case msg := <-partitionConsumer.Messages():
			fmt.Printf("Received message %d: %s\n", num, string(msg.Value))
			num++
			websocket.BroadcastMessage(string(msg.Value))
			websocket.SendWebSocketUpdate("ini notifikasi")

		case err := <-partitionConsumer.Errors():
			fmt.Printf("Error: %v\n", err.Err)
		}
	}
}
