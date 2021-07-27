package transport

// This file contains the pulsar implementation of the transport API

import (
	"context"
	"encoding/hex"
	"fmt"

	"github.com/apache/pulsar-client-go/pulsar"
	log "github.com/sirupsen/logrus"
)

type pulsarSess struct {
	// session has link to underlying connection
	conn             *pulsarConn
	topic            string
	subscriptionName string

	// TODO lock on the below pointers
	// when consume is first called, a consumer is set up
	cons pulsar.Consumer
	// when produce is first called, a producer is set up
	prod pulsar.Producer
}

func (s *pulsarSess) produceMsg(ctx context.Context, m Message, o *ProduceMsgOpts) (MessageID, error) {
	if s.prod == nil {
		prod, err := s.conn.client.CreateProducer(pulsar.ProducerOptions{
			Topic: s.topic,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create consumer: %w", err)
		}
		s.prod = prod
	}

	var id pulsar.MessageID
	var err error
	if o.Async {
		// convert generic message to pulsar message
		s.prod.SendAsync(ctx, &pulsar.ProducerMessage{
			Payload:    m.Payload(),
			Key:        m.Key(),
			Properties: m.Headers(),
			// TODO: there are a lot of other options here - but how do we expose them without exposing pulsar?
		}, func(m pulsar.MessageID, _ *pulsar.ProducerMessage, e error) {
			if err != nil {
				// TODO: how do we get this back to the user? is async even a part of this API or is it higher level
				log.Infof("Got err %s from message %s", e.Error(), hex.EncodeToString(m.Serialize()))
			}
		})
		return nil, nil
	}

	// convert generic message to pulsar message
	id, err = s.prod.Send(ctx, &pulsar.ProducerMessage{
		Payload:    m.Payload(),
		Key:        m.Key(),
		Properties: m.Headers(),
		// TODO: there are a lot of other options here - but how do we expose them without exposing pulsar?
	})

	if err != nil {
		return nil, err
	}
	return id.Serialize(), err
}

// implements message interface
type pulsarMsg struct {
	msg pulsar.Message
}

func (m *pulsarMsg) Payload() []byte {
	return m.msg.Payload()
}

func (m *pulsarMsg) Key() string {
	return m.msg.Key()
}

func (m *pulsarMsg) Headers() map[string]string {
	return m.msg.Properties()
}

func (s *pulsarSess) consumeMsg(ctx context.Context) (Message, error) {
	if s.cons == nil {
		consumer, err := s.conn.client.Subscribe(pulsar.ConsumerOptions{
			Topic:            s.topic,
			SubscriptionName: s.subscriptionName,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create consumer: %w", err)
		}
		s.cons = consumer
	}

	msg, err := s.cons.Receive(ctx)
	if err != nil {
		return nil, err
	}
	return &pulsarMsg{msg: msg}, nil
}

func (s *pulsarSess) consume(ctx context.Context) (<-chan Message, error) {
	if s.cons == nil {
		consumer, err := s.conn.client.Subscribe(pulsar.ConsumerOptions{
			Topic:            s.topic,
			SubscriptionName: s.subscriptionName,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create consumer: %w", err)
		}
		s.cons = consumer
	}
	// TODO handle this gracefully
	ch := make(chan Message)
	go func() {
		for {
			ch <- &pulsarMsg{<-s.cons.Chan()}
		}
	}()
	return ch, nil
}

func (c *pulsarConn) createSession(s *SessionOpts) (Session, error) {
	return &pulsarSess{conn: c, topic: s.Topic, subscriptionName: s.SubscriptionName}, nil
}

type pulsarConn struct {
	client pulsar.Client
}

func (c *pulsarConn) Close() error {
	c.client.Close()
	return nil
}

func (s *pulsarSess) Close() error {
	if s.cons != nil {
		s.cons.Close()
	}
	if s.prod != nil {
		s.prod.Close()
	}
	return nil
}

// Flush flushes the pulsar producer to write any async produced messages
func (s *pulsarSess) Flush() error {
	if s.prod != nil {
		return s.prod.Flush()
	}
	return nil
}
