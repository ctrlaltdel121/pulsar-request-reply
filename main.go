package main

import (
	"context"
	"fmt"
	"log"

	"github.com/fanatic/pulsar-request-reply/requester"
	"github.com/fanatic/pulsar-request-reply/responder"
	"github.com/fanatic/pulsar-request-reply/timing"
	"github.com/fanatic/pulsar-request-reply/transport"
)

func main() {
	t, err := transport.Connect(&transport.ConnectOpts{Type: "pulsar", URL: "pulsar://localhost:6650"})
	if err != nil {
		log.Fatalf("Could not create transport: %s", err)
	}

	ctx := context.Background()

	// Setup Response Handler (server-side)
	go responder.HandleResponses(ctx, t, "echo-service", helloHandler)

	// Make our test requests (client-side)
	for i := 10; i >= 0; i-- {
		hello(ctx, t, "hello")
	}

	asyncHello(ctx, t, "hello")

	timing.Results()
}

func hello(ctx context.Context, conn transport.Connection, payload string) {
	defer timing.New("rtt").Start().Stop()

	reply, err := requester.Request(ctx, conn, "echo-service", []byte(payload))
	if err != nil {
		log.Fatalf("Request failed: %v", err)
	}
	log.Printf("Received reply: %s", reply)
}

func helloHandler(payload []byte) ([]byte, error) {
	defer timing.New("rtt").Start().Stop()

	return []byte(fmt.Sprintf("Received: %s", payload)), nil
}

func asyncHello(ctx context.Context, conn transport.Connection, payload string) {
	defer timing.New("async-rtt").Start().Stop()

	reply := make(chan []byte)
	n := 100

	for i := 0; i < n; i++ {
		err := requester.AsyncRequest(ctx, conn, "echo-service", []byte(payload), reply)
		if err != nil {
			log.Fatalf("Request failed: %v", err)
		}
	}

	for i := 0; i < n; i++ {
		log.Printf("Received reply: %s", <-reply)
	}
}
