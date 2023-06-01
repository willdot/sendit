package main

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/go-redis/redis/v8"
	"github.com/nats-io/nats.go"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/willdot/sendit/config"
)

func TestSendRabbit(t *testing.T) {
	consumer := setupRabbit(t)

	tt := map[string]struct {
		rabbitType  string
		destination string
	}{
		"exchange": {rabbitType: config.RabbitExchangeBroker, destination: test_exchange},
		"queue":    {rabbitType: config.RabbitQueueBroker, destination: test_queue},
	}

	for name, tc := range tt {
		t.Run(name, func(t *testing.T) {
			cfg := &config.Config{
				Broker: tc.rabbitType,
				RabbitCfg: &config.RabbitConfig{
					DestinationName: tc.destination,
				},
				URL:             rabbit_url,
				BodyFileName:    "body.json",
				HeadersFileName: "rabbit-headers.json",
				Repeat:          1,
			}

			err := send(cfg, mockFileReader)
			require.NoError(t, err)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			select {
			case <-ctx.Done():
				t.Fatalf("timed out waiting for response")
			case msg := <-consumer.msgs:
				assert.Equal(t, string(body), string(msg.Body))
				assertRabbitHeadersMatch(t, rabbit_header, msg.Headers)
			}

			err = checkNoMoreMessages[amqp.Delivery](consumer.msgs)
			require.NoError(t, err)
		})
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

	natsSub := setupNats(t)

	err := send(cfg, mockFileReader)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	select {
	case msg := <-natsSub.msgs:
		assert.Equal(t, string(body), string(msg.Data))
		assertNatsHeadersMatch(t, nats_header, msg.Header)
	case <-ctx.Done():
		t.Fatalf("timed out waiting for messages")
	}

	err = checkNoMoreMessages[*nats.Msg](natsSub.msgs)
	require.NoError(t, err)
}

func TestSendRedis(t *testing.T) {
	cfg := &config.Config{
		Broker: config.RedisBroker,
		RedisCfg: &config.RedisConfig{
			Channel: test_channel,
		},
		URL:          redis_url,
		BodyFileName: "body.json",
		Repeat:       1,
	}

	redisSub := setupRedis(t)

	err := send(cfg, mockFileReader)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	select {
	case msg := <-redisSub.msgs:
		assert.Equal(t, string(body), msg.Payload)
	case <-ctx.Done():
		t.Fatalf("timed out waiting for messages")
	}

	err = checkNoMoreMessages[*redis.Message](redisSub.msgs)
	require.NoError(t, err)
}

func TestSendGooglePubSub(t *testing.T) {
	cfg := &config.Config{
		Broker: config.GooglePubSubBroker,
		GooglePubSubCfg: &config.GooglePubSubConfig{
			Topic:       test_topic,
			ProjectID:   test_project_id,
			DisableAuth: true,
		},
		URL:             google_pub_sub_url,
		BodyFileName:    "body.json",
		HeadersFileName: "google-pub-sub-headers.json",
		Repeat:          1,
	}

	googlePubSub := setupGooglePubSub(t)

	err := send(cfg, mockFileReader)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	select {
	case msg := <-googlePubSub.msgs:
		assert.Equal(t, string(body), string(msg.Data))
		assertGooglePubSubHeadersMatch(t, google_pub_sub_header, msg.Attributes)
	case <-ctx.Done():
		t.Fatalf("timed out waiting for messages")
	}

	err = checkNoMoreMessages[*pubsub.Message](googlePubSub.msgs)
	require.NoError(t, err)
}

func checkNoMoreMessages[T any](c <-chan T) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*500)
	defer cancel()

	select {
	case <-ctx.Done():
		return nil
	case <-c:
		return errors.New("queue wasn't empty")
	}
}

func assertRabbitHeadersMatch(t *testing.T, expected string, actual map[string]interface{}) {
	var expectedHeader map[string]interface{}
	err := json.Unmarshal([]byte(expected), &expectedHeader)
	require.NoError(t, err)

	assert.Equal(t, expectedHeader, actual)
}

func assertNatsHeadersMatch(t *testing.T, expected string, actual nats.Header) {
	var expectedHeader nats.Header
	err := json.Unmarshal([]byte(expected), &expectedHeader)
	require.NoError(t, err)

	assert.Equal(t, expectedHeader, actual)
}

func assertGooglePubSubHeadersMatch(t *testing.T, expected string, actual map[string]string) {
	var expectedHeader map[string]string
	err := json.Unmarshal([]byte(expected), &expectedHeader)
	require.NoError(t, err)

	assert.EqualValues(t, expectedHeader, actual)
}
