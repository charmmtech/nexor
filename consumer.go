// Package nexor provides a NATS client implementation with support for protobuf message handling.
// It simplifies the process of publishing and subscribing to messages using Protocol Buffers
// for message serialization and deserialization.
package nexor

import (
	"context"
	"errors"
	"time"

	"github.com/charmmtech/nextools"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/proto"
)

// ProtoHandler is a function type that defines the signature for handling protobuf messages.
// It processes a decoded protobuf message along with its NATS message context and returns an error if processing fails.
type ProtoHandler func(ctx context.Context, msg proto.Message, m jetstream.Msg) error

// Subscriber interface defines the contract for types that can handle protobuf message subscriptions.
// It requires implementations to provide both the protobuf message type and a handler function.
type Subscriber interface {
	// ProtoMessage returns a new instance of the protobuf message type that this consumer handles
	ProtoMessage() proto.Message
	// Handler processes the received protobuf message and its associated NATS message
	Handler(ctx context.Context, msg proto.Message, m jetstream.Msg) error
}

const (
	// DefaultAckWait is how long JetStream waits for an ack before redelivering a message.
	DefaultAckWait = 30 * time.Second
	// DefaultMaxDeliver caps how many times JetStream delivers a message to a consumer.
	// Without a cap, a message that is never acked or keeps failing is redelivered forever.
	DefaultMaxDeliver = 10
)

// redeliveryBackoff is the delay before a failed message is redelivered, indexed by delivery attempt.
var redeliveryBackoff = []time.Duration{
	5 * time.Second,
	15 * time.Second,
	30 * time.Second,
	time.Minute,
	2 * time.Minute,
	5 * time.Minute,
}

// redeliveryDelay returns how long to wait before redelivering a message that failed on its nth delivery.
func redeliveryDelay(numDelivered uint64) time.Duration {
	if numDelivered == 0 {
		numDelivered = 1
	}
	index := int(numDelivered) - 1
	if index >= len(redeliveryBackoff) {
		index = len(redeliveryBackoff) - 1
	}
	return redeliveryBackoff[index]
}

// Subscribe sets up a subscription to a NATS subject with protobuf message handling.
// It automatically decodes incoming messages using the provided protobuf message factory
// and processes them with the specified handler.
//
// Parameters:
//   - subject: The NATS subject to subscribe to
//   - stream: The stream name for the subscription (for JetStream persistence)
//   - durable: The durable name for the subscription (for JetStream persistence)
//   - factory: A function that creates new instances of the protobuf message type
//   - handler: A function that processes decoded protobuf messages
//   - opts: Optional subscription options that override the default settings
//
// Default behavior:
//   - Uses durable subscriptions for message persistence
//   - Acks the message when the handler returns nil (handlers may also ack themselves)
//   - Redelivers a failed message after an increasing delay, up to DefaultMaxDeliver deliveries
//   - Terminates messages that cannot be decoded, since redelivering them can never succeed
//   - Sets a 30-second acknowledgment timeout
//
// Returns:
//   - error: Returns an error if the subscription setup fails
func (n *nexor) Subscribe(
	ctx context.Context,
	subject string,
	stream string,
	durable string,
	factory func() proto.Message,
	handler ProtoHandler,
	opts ...jetstream.PullConsumeOpt,
) error {
	jetStream, err := n.js.Stream(ctx, stream)
	if err != nil {
		return err
	}

	consumer, err := jetStream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Name:          durable,
		Durable:       durable,
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       DefaultAckWait,
		MaxDeliver:    DefaultMaxDeliver,
		FilterSubject: subject,
	})
	if err != nil {
		return err
	}

	// Subscribe to the subject with the provided options
	_, err = consumer.Consume(func(m jetstream.Msg) {
		// Create a new instance of the protobuf message
		msg := factory()
		if err := proto.Unmarshal(m.Data(), msg); err != nil {
			n.logger.Error("🚨 nexor: dropping message that failed to decode: %v", nextools.F("subject", m.Subject()), nextools.Err(err))

			// A message that cannot be decoded will never succeed, so stop redelivering it.
			_ = m.Term()
			return
		}

		// Call the handler to process the message
		if err := handler(ctx, msg, m); err != nil {
			var numDelivered uint64
			if metadata, metadataErr := m.Metadata(); metadataErr == nil {
				numDelivered = metadata.NumDelivered
			}

			if numDelivered >= DefaultMaxDeliver {
				n.logger.Error("🚨 nexor: handler failed on final delivery, dropping message: %v", nextools.F("subject", m.Subject()), nextools.Err(err))
				_ = m.Term()
				return
			}

			if n.cfg.Debug {
				n.logger.Error("🚨 nexor: handler error: %v", nextools.F("subject", m.Subject()), nextools.Err(err))
			}

			// Redeliver after a delay instead of immediately, so a failing handler does not spin.
			if nakErr := m.NakWithDelay(redeliveryDelay(numDelivered)); nakErr != nil && !errors.Is(nakErr, jetstream.ErrMsgAlreadyAckd) {
				n.logger.Error("🚨 nexor: failed to nak message: %v", nextools.F("subject", m.Subject()), nextools.Err(nakErr))
			}
			return
		}

		// Ack on success so JetStream does not redeliver the message after AckWait.
		// Handlers that already acked are fine: the second ack is a no-op.
		if ackErr := m.Ack(); ackErr != nil && !errors.Is(ackErr, jetstream.ErrMsgAlreadyAckd) {
			n.logger.Error("🚨 nexor: failed to ack message: %v", nextools.F("subject", m.Subject()), nextools.Err(ackErr))
		}
	}, opts...)

	if err != nil {
		if n.cfg.Debug {
			n.logger.Error("❌ nexor: failed to subscribe to subject: %s: %v", nextools.F("subject", subject), nextools.Err(err))
		}
		return err
	}

	if n.cfg.Debug {
		n.logger.OK("🚀 nexor: successfully subscribed to subject: %s", nextools.F("subject", subject))
	}

	return err
}
