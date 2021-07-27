package transport

// This file contains the API surface for the transport library.

import (
	"context"
	"fmt"

	"github.com/apache/pulsar-client-go/pulsar"
)

// CORE API:

func Connect(c *ConnectOpts) (Connection, error) {
	switch c.Type {
	case "pulsar":
		client, err := pulsar.NewClient(pulsar.ClientOptions{
			URL: c.URL,
		})
		if err != nil {
			return nil, err
		}
		return &pulsarConn{client: client}, nil
	default:
		return nil, fmt.Errorf("Unknown type %s", c.Type)
	}
}

func CreateSession(c Connection, s *SessionOpts) (Session, error) {
	return c.createSession(s)
}

func ProduceMsg(ctx context.Context, s Session, m Message, o *ProduceMsgOpts) (MessageID, error) {
	return s.produceMsg(ctx, m, o)
}

func ConsumeMsg(ctx context.Context, s Session) (Message, error) {
	return s.consumeMsg(ctx)
}

func Consume(ctx context.Context, s Session) (<-chan Message, error) {
	return s.consume(ctx)
}

// INTERFACES:

// basic properties of a message
// User must create type implementing this interface to generate a message to produce.
type Message interface {
	Payload() []byte
	Key() string
	Headers() map[string]string
}

// connection object can be closed, but session creation is called through the API surface
type Connection interface {
	Close() error
	createSession(s *SessionOpts) (Session, error)
}

// session object can be  flushed and closed, but produce and consume is called through the API surface
type Session interface {
	produceMsg(ctx context.Context, m Message, o *ProduceMsgOpts) (MessageID, error)
	consumeMsg(ctx context.Context) (Message, error)
	consume(ctx context.Context) (<-chan Message, error)
	Close() error
	Flush() error
}

// STRUCTURES:

type SessionOpts struct {
	Topic            string
	SubscriptionName string
}

type MessageID []byte
type ProduceMsgOpts struct {
	Async bool
}

type ConnectOpts struct {
	Type string
	URL  string
}
