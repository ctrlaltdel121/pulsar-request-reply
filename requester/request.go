package requester

import (
	"context"
	"fmt"
	"log"

	"github.com/fanatic/pulsar-request-reply/transport"
	"github.com/google/uuid"
)

func Request(ctx context.Context, conn transport.Connection, service string, payload []byte) ([]byte, error) {
	requestID := uuid.NewString()

	r, err := New(ctx, conn, service, requestID)
	if err != nil {
		log.Fatalf("Could not instantiate requester: %v", err)
	}
	defer r.Close()

	// Produce request (asynchronously)
	r.producer.Produce(ctx, payload, map[string]string{"replyTo": "reply-" + requestID})

	// Consume response
	msg, err := r.consumer.ConsumeMsg(ctx)
	if err != nil {
		log.Fatal(err)
	}
	// TODO acks
	// r.consumer.Consumer().Ack(msg)

	return msg.Payload(), nil
}

func AsyncRequest(ctx context.Context, conn transport.Connection, service string, payload []byte, reply chan []byte) error {
	requestID := uuid.NewString()

	r, err := New(ctx, conn, service, requestID)
	if err != nil {
		return fmt.Errorf("Could not instantiate requester: %w", err)
	}

	// Produce request (asynchronously)
	r.producer.Produce(ctx, payload, map[string]string{"replyTo": "reply-" + requestID})

	// Consume response
	go func() {
		msg, err := r.consumer.ConsumeMsg(ctx)
		if err != nil {
			log.Fatal(err)
		}

		// TODO acks
		//r.consumer.Consumer().Ack(msg)

		reply <- msg.Payload()

		r.Close()
	}()

	return nil
}
