package main

import (
	"context"
	"time"

	"github.com/charmmtech/nexor"
	v1 "github.com/charmmtech/nexor/gen/charmmtech/nexor/v1"
	"github.com/charmmtech/nextools"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go/jetstream"
)

var (
	logger = nextools.NewLoggerBuilder().Build()
	client = nexor.New("nats://localhost:4222", logger)
)

func init() {
	client.Connect()
}

func main() {
	defer client.Close()

	ctx := context.Background()

	// Initialize the event bus
	_ = client.CreateStream(ctx, jetstream.StreamConfig{
		Name:        "SAMPLE_EVENTS",
		Description: "Sample events",
		Subjects:    []string{"sample.>"},
		MaxBytes:    1024 * 1024 * 1024,
	})

	for {
		// Get the current time
		currentTime := time.Now()
		// List of sample words
		words := []string{"Apple", "Banana", "Orange", "Mango", "Grape", "Peach", "Plum", "Cherry", "Lemon", "Lime"}
		subjects := []string{"sample.created", "sample.updated"}
		// Generate a random word
		randomWord := words[currentTime.UnixNano()%int64(len(words))]
		subject := subjects[currentTime.UnixNano()%int64(len(subjects))]

		// Create a ProductCreated event
		event := &v1.Event{
			Name: subject,
			Product: &v1.ProductCreated{
				Id:         uuid.NewString(),
				Name:       randomWord,
				SupplierId: uuid.NewString(),
				CreatedAt:  currentTime.UnixMilli(),
			},
		}

		// Publish the event
		if err := client.Publish(ctx, subject, event); err != nil {
			logger.Error("🚨 Failed to publish event: %v", nextools.Err(err))
		}

		logger.OK("🚀 Event published to subject: %s successfully! 🚀\n", nextools.F("subject", subject))

		time.Sleep(time.Duration(3) * time.Second)
	}
}
