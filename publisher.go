package nexor

import (
	"context"

	"github.com/charmmtech/nextools"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/proto"
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
func (n *nexor) Publish(ctx context.Context, subject string, msg proto.Message, opts ...jetstream.PublishOpt) error {
	data, err := proto.Marshal(msg)
	if err != nil {
		if n.cfg.Debug {
			n.logger.Error("❌ nexor: failed to encode protobuf: %v", nextools.Err(err))
		}

		return err
	}

	ack, err := n.js.Publish(ctx, subject, data, opts...)
	if err != nil {
		if n.cfg.Debug {
			n.logger.Error("❌ nexor: failed to publish message: %v", nextools.Err(err))
		}

		return err
	}

	if n.cfg.Debug {
		n.logger.Info("🚀 nexor: published message on domain: %s", nextools.F("ack.Domain", ack.Domain))
		n.logger.Info("🚀 nexor: published message on sequence: %d", nextools.F("ack.Sequence", ack.Sequence))
		n.logger.Info("🚀 nexor: published message on duplicate: %v", nextools.F("ack.Duplicate", ack.Duplicate))
		n.logger.Info("🚀 nexor: published message on stream: %s", nextools.F("ack.Stream", ack.Stream))
	}

	return err
}
