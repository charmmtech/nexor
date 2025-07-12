package nexor

import (
	"context"
	"fmt"
	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"
	"log"
	"time"
)

// Request sends a protobuf message as a request and waits for a protobuf reply.
// - subject: The NATS subject to send the request to
// - req: The protobuf message to send
// - factory: A function that returns a new instance of the expected reply message
// - timeout: How long to wait for a response
func (n *nexor) Request(ctx context.Context, subject string, req proto.Message, factory func() proto.Message) (proto.Message, error) {
	data, err := proto.Marshal(req)
	if err != nil {
		if n.cfg.Debug {
			log.Printf("[👮] nexor: failed to marshal request: %v", err)
		}
		return nil, err
	}

	// Create a unique inbox
	inbox := nats.NewInbox()

	// Subscribe to the inbox for the reply
	sub, err := n.conn.SubscribeSync(inbox)
	if err != nil {
		if n.cfg.Debug {
			log.Printf("[👮] nexor: failed to subscribe to inbox: %v", err)
		}
		return nil, err
	}
	defer sub.Unsubscribe()

	// Publish with reply subject set to inbox
	if err := n.conn.PublishMsg(&nats.Msg{
		Subject: subject,
		Reply:   inbox,
		Data:    data,
	}); err != nil {
		if n.cfg.Debug {
			log.Printf("[👮] nexor: failed to publish request: %v", err)
		}
		return nil, err
	}

	// Wait for the reply with timeout from context
	timeout := 5 * time.Second // default
	if deadline, ok := ctx.Deadline(); ok {
		timeout = time.Until(deadline)
	}

	msg, err := sub.NextMsg(timeout)
	if err != nil {
		if n.cfg.Debug {
			log.Printf("[👮] nexor: failed to receive reply: %v", err)
		}
		return nil, err
	}

	reply := factory()
	if reply == nil {
		log.Println("[👮] nexor: factory returned nil, cannot unmarshal")
		return nil, fmt.Errorf("nexor: factory returned nil, cannot unmarshal")
	}

	if err := proto.Unmarshal(msg.Data, reply); err != nil {
		if n.cfg.Debug {
			log.Printf("[👮] nexor: failed to unmarshal response: %v", err)
		}
		return nil, err
	}

	return reply, nil
}

func (n *nexor) RequestBytes(ctx context.Context, subject string, data []byte) ([]byte, error) {
	msg, err := n.conn.RequestWithContext(ctx, subject, data)
	if err != nil {
		return nil, err
	}
	return msg.Data, nil
}
