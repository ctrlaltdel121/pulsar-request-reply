package common

import (
	"context"
	"fmt"
	"log"

	"github.com/fanatic/pulsar-request-reply/transport"
)

type Consumer struct {
	sess transport.Session
}

func NewConsumer(ctx context.Context, t transport.Connection, topic, subscriptionName string) (*Consumer, error) {
	log.Printf("creating consumer...")

	sess, err := transport.CreateSession(t, &transport.SessionOpts{Topic: topic, SubscriptionName: subscriptionName})
	if err != nil {
		return nil, err
	}
	return &Consumer{sess}, nil
}

func (c *Consumer) Close() {
	c.sess.Close()
}

//func (c *Consumer) Consumer() pulsar.Consumer {
//	return c.consumer
//}

func (c *Consumer) Consume(ctx context.Context) {
	// infinite loop to receive messages
	go func() {
		for {
			msg, err := transport.ConsumeMsg(ctx, c.sess)
			if err != nil {
				log.Fatal(err)
			} else {
				fmt.Printf("Consumed message : %v %v\n", msg.Key(), string(msg.Payload()))
			}

			// TODO ACKs
			//c.consumer.Ack(msg)
		}
	}()
}

func (c *Consumer) ConsumeMsg(ctx context.Context) (transport.Message, error) {
	return transport.ConsumeMsg(ctx, c.sess)
}
