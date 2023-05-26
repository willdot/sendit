package main

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/willdot/sendit/config"
)

const (
	body = `
	{
		"key1": "value1",
		"key2": "value2"
	}`

	nats_header = `
	{
		"header1": ["1", "2"],
		"header2": ["3", "4"]
	}
	`
	test_subject = "test-subject"

	nats_url = "localhost:4222"
)

type mockFileReader struct{}

func (fr *mockFileReader) ReadFile(filename string) ([]byte, error) {
	switch filename {
	case "body.json":
		return []byte(body), nil
	case "nats-headers.json":
		return []byte(nats_header), nil
	case "rabbit-headers.json":
		return []byte(rabbit_header), nil
	case "kafka-headers.json":
		return []byte(kafka_header), nil
	default:
		return nil, fmt.Errorf("invalid file name requested")
	}
}

func TestSendNats(t *testing.T) {
	cfg := &config.Config{
		Broker: config.NatsBroker,
		NatsCfg: &config.NatsConfig{
			Subject: test_subject,
		},
		URL:             nats_url,
		BodyFileName:    "body.json",
		HeadersFileName: "nats-headers.json",
		Repeat:          1,
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	natsSub := setupNats(t, ctx)

	err := send(cfg, &mockFileReader{})
	require.NoError(t, err)

	select {
	case msg := <-natsSub.msgs:
		assert.Equal(t, string(body), string(msg.Data))
		assertNatsHeadersMatch(t, nats_header, msg.Header)
	case <-ctx.Done():
		t.Fatalf("timed out waiting for messages")
	}
}

func TestSendNatsRepeat(t *testing.T) {
	cfg := &config.Config{
		Broker: config.NatsBroker,
		NatsCfg: &config.NatsConfig{
			Subject: test_subject,
		},
		URL:             nats_url,
		BodyFileName:    "body.json",
		HeadersFileName: "nats-headers.json",
		Repeat:          5,
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	natsSub := setupNats(t, ctx)

	err := send(cfg, &mockFileReader{})
	require.NoError(t, err)

	for i := 0; i < 5; i++ {
		select {
		case msg := <-natsSub.msgs:
			assert.Equal(t, string(body), string(msg.Data))
			assertNatsHeadersMatch(t, nats_header, msg.Header)
		case <-ctx.Done():
			t.Fatalf("timed out waiting for messages")
		}
	}
}

func assertNatsHeadersMatch(t *testing.T, expected string, actual nats.Header) {
	var expectedHeader nats.Header
	err := json.Unmarshal([]byte(expected), &expectedHeader)
	require.NoError(t, err)

	assert.Equal(t, expectedHeader, actual)
}

type natsSubscriber struct {
	msgs chan *nats.Msg
}

func setupNats(t *testing.T, ctx context.Context) natsSubscriber {
	nc, err := nats.Connect(nats_url)
	require.NoError(t, err)

	natsSub := natsSubscriber{
		msgs: make(chan *nats.Msg),
	}

	sub, err := nc.SubscribeSync(test_subject)
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = sub.Unsubscribe()
		nc.Close()
	})

	go func() {
		for {
			msg, _ := sub.NextMsgWithContext(ctx)
			natsSub.msgs <- msg
		}
	}()

	return natsSub
}
