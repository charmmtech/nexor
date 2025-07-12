package main

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/shoot3rs/nexor"
	v1 "github.com/shoot3rs/nexor/gen/shooters/nexor/v1"
	"log"
	"strings"
	"time"
)

var (
	client = nexor.New("nats://localhost:4222")
)

func init() {
	client.Connect()
}

func main() {
	// Initialize the event bus
	js := client.JetStream()
	defer client.Close()

	// Create a stream (e.g. product.*)
	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "nexor_product_events",
		Subjects: []string{"nexor.product.*"},
	})
	if err != nil && !strings.Contains(err.Error(), "stream name already in use") {
		log.Fatalf("❌ Failed to create stream: %v", err)
	}

	for {
		// Get the current time
		currentTime := time.Now()
		// List of sample words
		words := []string{"Apple", "Banana", "Orange", "Mango", "Grape", "Peach", "Plum", "Cherry", "Lemon", "Lime"}
		// Generate a random word
		randomWord := words[currentTime.UnixNano()%int64(len(words))]

		// Create a ProductCreated event
		event := &v1.ProductCreated{
			Id:         uuid.NewString(),
			Name:       randomWord,
			SupplierId: uuid.NewString(),
			CreatedAt:  currentTime.UnixMilli(),
		}

		// Publish the event
		if err := client.Publish(context.Background(), "nexor.product.created", event); err != nil {
			log.Fatalf("Failed to publish event: %v", err)
		}

		fmt.Println("🚀 Event published successfully! 🚀")

		time.Sleep(time.Duration(5) * time.Second)
	}
}
