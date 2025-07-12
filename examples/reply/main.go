package main

import (
	"github.com/shoot3rs/nexor"
)

var (
	client = nexor.New("nats://localhost:4222")
)

func init() {
	client.Connect()
}

func main() {
	//_ = client.Reply("nexor.request",
	//	func() proto.Message { return &v1.SayHelloResponse{} },
	//	func(ctx context.Context, req proto.Message) (proto.Message, error) {
	//		request := req.(*v1.SayHelloRequest)
	//		return &v1.SayHelloResponse{Message: fmt.Sprintf("Hello %s", request.GetName())}, nil
	//	},
	//)

	//_ = client.ReplyBytes("example.raw", func(bytes []byte) []byte {
	//	req := campaignv1.GetCampaignRequest{}
	//	if err := proto.Unmarshal(bytes, &req); err != nil {
	//		log.Fatalln("unmarshalling error: ", err)
	//	}
	//
	//	log.Println("[🔥] Received:", req.String())
	//
	//	// Create a simple JSON response as bytes
	//	pb, _ := proto.Marshal(&req)
	//
	//	log.Println("[📤] Sending response:", string(pb))
	//
	//	return pb
	//})

	select {}
}
