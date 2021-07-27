package responder

import (
	"context"
	"fmt"
	"log"

	"github.com/fanatic/pulsar-request-reply/common"
	"github.com/fanatic/pulsar-request-reply/transport"
)

type Responder struct {
	producer *common.Producer
	consumer *common.Consumer
}

func New(ctx context.Context, t transport.Connection, topic, subscriptionName string) (*Responder, error) {
	log.Printf("creating responder...")

	consumer, err := common.NewConsumer(ctx, t, topic, subscriptionName)
	if err != nil {
		return nil, fmt.Errorf("Could not instantiate Pulsar consumer: %v", err)
	}

	return &Responder{nil, consumer}, nil
}

func (r *Responder) Close() {
	r.consumer.Close()
}
