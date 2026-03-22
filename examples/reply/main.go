package main

import (
	"context"
	"fmt"

	"github.com/charmmtech/nexor"
	v1 "github.com/charmmtech/nexor/gen/charmmtech/nexor/v1"
	"github.com/charmmtech/nextools"
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
	defer client.Close()

	_ = client.Reply("example.say.hello",
		func() proto.Message { return &v1.SayHelloRequest{} },
		func(ctx context.Context, req proto.Message) (proto.Message, error) {
			request := req.(*v1.SayHelloRequest)
			return &v1.SayHelloResponse{Message: fmt.Sprintf("Hello Reply %s", request.GetName())}, nil
		},
	)

	select {}
}
