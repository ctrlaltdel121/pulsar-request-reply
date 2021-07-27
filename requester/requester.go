package requester

import (
	"context"
	"fmt"
	"log"

	"github.com/fanatic/pulsar-request-reply/common"
	"github.com/fanatic/pulsar-request-reply/transport"
)

type Requester struct {
	producer *common.Producer
	consumer *common.Consumer
}

func New(ctx context.Context, conn transport.Connection, topic, requestID string) (*Requester, error) {
	log.Printf("creating requester...")

	producer, err := common.NewProducer(ctx, conn, topic)
	if err != nil {
		return nil, fmt.Errorf("Could not instantiate Pulsar producer: %v", err)
	}

	consumer, err := common.NewConsumer(ctx, conn, topic+".reply-"+requestID, "reply-"+requestID)
	if err != nil {
		return nil, fmt.Errorf("Could not instantiate Pulsar consumer: %v", err)
	}

	return &Requester{producer, consumer}, nil
}

func (r *Requester) Close() {
	r.producer.Close()
	r.consumer.Close()
}
