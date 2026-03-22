package main

import (
	"context"
	"time"

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

	ctx := context.Background()
	resp, err := client.Request(ctx, "example.say.hello", &v1.SayHelloRequest{Name: "Joey"}, func() proto.Message { return &v1.SayHelloResponse{} }, 3*time.Second)
	if err != nil {
		logger.Error("🚨 response error:", nextools.Err(err))
	}

	response := resp.(*v1.SayHelloResponse)

	logger.OK("==== 📣 Response==== :", nextools.F("message", response.Message))
}
