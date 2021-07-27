package common

import (
	"context"

	"github.com/fanatic/pulsar-request-reply/transport"
)

type Producer struct {
	producer transport.Session
}

func NewProducer(ctx context.Context, t transport.Connection, topic string) (*Producer, error) {
	sess, err := transport.CreateSession(t, &transport.SessionOpts{
		Topic: topic,
	})
	if err != nil {
		return nil, err
	}
	return &Producer{sess}, nil
}

func (p *Producer) Close() {
	p.producer.Flush()
	p.producer.Close()
}

// to produce using transport, make an interface-compliant structure for outgoing messages
type myMessage struct {
	payload []byte
	headers map[string]string
	key     string
}

func (m *myMessage) Payload() []byte {
	return m.payload
}

func (m *myMessage) Headers() map[string]string {
	return m.headers
}

func (m *myMessage) Key() string {
	return m.key
}

func (p *Producer) Produce(ctx context.Context, payload []byte, properties map[string]string) {
	asyncMsg := myMessage{
		payload: payload,
		headers: properties,
		key:     "",
	}

	transport.ProduceMsg(ctx, p.producer, &asyncMsg, &transport.ProduceMsgOpts{Async: true})
}
