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
	defer client.Close()

	//ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	//defer cancel()
	//
	//req := &campaignv1.GetCampaignRequest{
	//	Id: "b316c95a-72c7-4ecd-89ff-fe1d47d357e7",
	//}
	//pb, err := proto.Marshal(req)
	//if err != nil {
	//	log.Fatalln("marshaling error: ", err)
	//}
	//
	//resp, err := client.RequestBytes(ctx, "example.raw", pb)
	//if err != nil {
	//	log.Fatalln("[🚨] response error:", err)
	//}
	//
	//response := &campaignv1.GetCampaignRequest{}
	//_ = proto.Unmarshal(resp, response)
	//
	//log.Println("==== 📣 Response ==== :\n", response)
}
