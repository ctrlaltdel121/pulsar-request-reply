package responder

import (
	"context"
	"fmt"
	"log"

	"github.com/fanatic/pulsar-request-reply/common"
	"github.com/fanatic/pulsar-request-reply/transport"
)

func HandleResponses(ctx context.Context, t transport.Connection, service string, handler func(body []byte) ([]byte, error)) {
	r, err := New(ctx, t, service, "shared")
	if err != nil {
		log.Fatalf("Could not instantiate requester: %v", err)
	}
	defer r.Close()

	for {
		// Consume request
		msg, err := r.consumer.ConsumeMsg(ctx)
		if err != nil {
			log.Fatal(err)
		} else {
			fmt.Printf("Consumed request : %v %v\n", msg.Key(), string(msg.Payload()))
		}

		reply, err := handler(msg.Payload())
		// TODO acks
		//if err != nil {
		//	r.consumer.Consumer().Nack(msg)
		//}
		//
		//r.consumer.Consumer().Ack(msg)

		// Produce response
		p, err := common.NewProducer(ctx, t, service+"."+msg.Headers()["replyTo"])
		if err != nil {
			log.Fatalf("Could not instantiate Pulsar producer: %v", err)
		}

		p.Produce(ctx, reply, nil)
		p.Close()
	}
}
