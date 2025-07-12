package nexor

import (
	"context"
	"google.golang.org/protobuf/proto"
	"log"

	"github.com/nats-io/nats.go"
)

// Publish publishes a protobuf message to the specified NATS subject.
// It marshals the protobuf message into bytes and publishes it using JetStream.
//
// Parameters:
//   - ctx: Context for the operation (currently unused)
//   - subject: The NATS subject to publish the message to
//   - msg: The protobuf message to be published
//   - opts: Optional publishing options for NATS
//
// Returns:
//   - error: Returns an error if marshaling fails or if publishing fails
func (n *nexor) Publish(ctx context.Context, subject string, msg proto.Message, opts ...nats.PubOpt) error {
	data, err := proto.Marshal(msg)
	if err != nil {
		if n.cfg.Debug {
			log.Printf("❌ nexor: failed to encode protobuf: %v", err)
		}

		return err
	}

	ack, err := n.js.Publish(subject, data, opts...)
	if err != nil {
		if n.cfg.Debug {
			log.Printf("❌ nexor: failed to publish message: %v", err)
		}

		return err
	}

	if n.cfg.Debug {
		log.Printf("🚀 nexor: published message on domain: %s", ack.Domain)
		log.Printf("🚀 nexor: published message on sequence: %d", ack.Sequence)
		log.Printf("🚀 nexor: published message on duplicate: %v", ack.Duplicate)
		log.Printf("🚀 nexor: published message on stream: %s", ack.Stream)
	}

	return err
}
