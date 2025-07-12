package nexor

import (
	"context"
	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"
	"log"
)

type Replier interface {
	// ProtoMessage returns a new instance of the protobuf message type that this subscriber handles
	ProtoMessage() proto.Message
	// Handler processes the received protobuf message and its associated NATS message
	Handler(ctx context.Context, msg proto.Message) (proto.Message, error)
}

type ReplierRawClient interface {
	// Handler subscribes to a subject and handles incoming []byte requests,
	// responding with []byte replies.
	Handler(handler []byte) []byte
}

// Reply sets up a handler that receives protobuf request messages and responds with protobuf replies.
// - subject: Subject to listen for requests on
// - reqFactory: Function that returns a new instance of the request message type
// - handler: Function to handle the request and return a response
func (n *nexor) Reply(subject string, reqFactory func() proto.Message, handler func(context.Context, proto.Message) (proto.Message, error)) error {
	_, err := n.conn.Subscribe(subject, func(m *nats.Msg) {
		req := reqFactory()
		if err := proto.Unmarshal(m.Data, req); err != nil {
			if n.cfg.Debug {
				log.Printf("[👮] nexor: failed to unmarshal request: %v", err)
			}

			// Optionally send back an error protobuf (if you define an error proto type)
			return
		}

		resp, err := handler(context.Background(), req)
		if err != nil {
			if n.cfg.Debug {
				log.Printf("[👮] nexor: handler failed: %v", err)
			}

			// Optionally send back an error protobuf
			return
		}

		log.Printf("[👀] About to marshal response: %+v", resp)
		data, err := proto.Marshal(resp)
		//data, err := proto.Marshal(resp)
		//log.Printf("[👮] sending reply data: %s", hex.EncodeToString(data))
		if err != nil {
			if n.cfg.Debug {
				log.Printf("[👮] nexor: failed to marshal response: %v", err)
			}

			return
		}

		if err := m.Respond(data); err != nil {
			if n.cfg.Debug {
				log.Printf("[👮] nexor: failed to send response: %v", err)
			}
		}
	})

	if err != nil && n.cfg.Debug {
		log.Printf("[👮] nexor: failed to subscribe for reply on %s: %v", subject, err)
	}

	return err
}

func (n *nexor) ReplyBytes(subject string, handler func([]byte) []byte) error {
	_, err := n.conn.Subscribe(subject, func(m *nats.Msg) {
		resp := handler(m.Data)
		_ = m.Respond(resp)
	})

	return err
}
