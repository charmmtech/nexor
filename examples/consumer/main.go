package main

import (
	"context"
	"errors"

	"github.com/charmmtech/nexor"
	v1 "github.com/charmmtech/nexor/gen/charmmtech/nexor/v1"
	"github.com/charmmtech/nextools"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/proto"
)

var (
	logger = nextools.NewLoggerBuilder().Build()
	client = nexor.New("nats://localhost:4222", logger)
)

func init() {
	client.Connect()
}

func main() {
	// Initialize the event bus
	defer client.Close()

	ctx := context.Background()

	// Subscribe to the "product.created" event
	err := client.Subscribe(ctx, "sample.updated", "", "SAMPLE_EVENTS", func() proto.Message {
		return &v1.Event{} // Factory method to create a specific event type
	}, func(ctx context.Context, msg proto.Message, m jetstream.Msg) error {
		logger.Info("🔥 event received via subject:", nextools.F("subject", m.Subject()))

		// Type asserts the message to a specific event type
		event, ok := msg.(*v1.Event)
		if !ok {
			logger.Debug("👻 Received an unknown message type")
			return errors.New("unknown message type")
		}

		// Handle the event
		logger.Info("🔥 Event Created: %v", nextools.F("event", event.String()))

		if err := m.Ack(); err != nil {
			logger.Error("🚨 Failed to acknowledge message:", nextools.Err(err))
		}

		return nil
	})

	if err != nil {
		logger.Error("🚨 Failed to subscribe to sample.created event: %v", nextools.Err(err))
	}

	// Keep the main function running to receive events
	logger.OK("🚀 waiting for events...")
	select {}
}
