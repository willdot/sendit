package main

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/willdot/sendit/config"
)

const (
	rabbit_header = `
	{
		"header1": ["1", "2"],
		"header2": ["3", "4"]
	}
	`
	test_queue    = "test-queue"
	test_exchange = "test-exchange"

	rabbit_url = "amqp://guest:guest@localhost:5672/"
)

func TestSendRabbitQueue(t *testing.T) {
	cfg := &config.Config{
		Broker: config.RabbitQueueBroker,
		RabbitCfg: &config.RabbitConfig{
			DestinationName: test_queue,
		},
		URL:             rabbit_url,
		BodyFileName:    "body.json",
		HeadersFileName: "rabbit-headers.json",
		Repeat:          1,
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	consumer := setupRabbit(t, ctx)

	err := send(cfg, &mockFileReader{})
	require.NoError(t, err)

	select {
	case msg := <-consumer.msgs:
		assert.Equal(t, string(body), string(msg.Body))
		assertRabbitHeadersMatch(t, rabbit_header, msg.Headers)
	case <-ctx.Done():
		t.Fatalf("timed out waiting for messages")
	}
}

func TestSendRabbitQueueRepeat(t *testing.T) {
	cfg := &config.Config{
		Broker: config.RabbitQueueBroker,
		RabbitCfg: &config.RabbitConfig{
			DestinationName: test_queue,
		},
		URL:             rabbit_url,
		BodyFileName:    "body.json",
		HeadersFileName: "rabbit-headers.json",
		Repeat:          5,
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	consumer := setupRabbit(t, ctx)

	err := send(cfg, &mockFileReader{})
	require.NoError(t, err)

	for i := 0; i < 5; i++ {
		select {
		case msg := <-consumer.msgs:
			assert.Equal(t, string(body), string(msg.Body))
			assertRabbitHeadersMatch(t, rabbit_header, msg.Headers)
		case <-ctx.Done():
			t.Fatalf("timed out waiting for messages")
		}
	}
}

func TestSendRabbitExchange(t *testing.T) {
	cfg := &config.Config{
		Broker: config.RabbitExchangeBroker,
		RabbitCfg: &config.RabbitConfig{
			DestinationName: test_exchange,
		},
		URL:             rabbit_url,
		BodyFileName:    "body.json",
		HeadersFileName: "rabbit-headers.json",
		Repeat:          1,
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	consumer := setupRabbit(t, ctx)

	err := send(cfg, &mockFileReader{})
	require.NoError(t, err)

	select {
	case msg := <-consumer.msgs:
		assert.Equal(t, string(body), string(msg.Body))
		assertRabbitHeadersMatch(t, rabbit_header, msg.Headers)
	case <-ctx.Done():
		t.Fatalf("timed out waiting for messages")
	}
}

func TestSendRabbitExchangeRepeat(t *testing.T) {
	cfg := &config.Config{
		Broker: config.RabbitExchangeBroker,
		RabbitCfg: &config.RabbitConfig{
			DestinationName: test_exchange,
		},
		URL:             rabbit_url,
		BodyFileName:    "body.json",
		HeadersFileName: "rabbit-headers.json",
		Repeat:          5,
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	consumer := setupRabbit(t, ctx)

	err := send(cfg, &mockFileReader{})
	require.NoError(t, err)

	for i := 0; i < 5; i++ {
		select {
		case msg := <-consumer.msgs:
			assert.Equal(t, string(body), string(msg.Body))
			assertRabbitHeadersMatch(t, rabbit_header, msg.Headers)
		case <-ctx.Done():
			t.Fatalf("timed out waiting for messages")
		}
	}
}

func assertRabbitHeadersMatch(t *testing.T, expected string, actual map[string]interface{}) {
	var expectedHeader map[string]interface{}
	err := json.Unmarshal([]byte(expected), &expectedHeader)
	require.NoError(t, err)

	assert.Equal(t, expectedHeader, actual)
}

type rabbitConsumer struct {
	msgs <-chan amqp.Delivery
}

func setupRabbit(t *testing.T, ctx context.Context) rabbitConsumer {
	conn, err := amqp.Dial(rabbit_url)
	require.NoError(t, err)

	ch, err := conn.Channel()
	require.NoError(t, err)

	t.Cleanup(func() {
		err := ch.ExchangeDelete(test_exchange, false, false)
		assert.NoError(t, err)

		_, err = ch.QueueDelete(test_queue, false, false, false)
		assert.NoError(t, err)

		ch.Close()
		conn.Close()
	})

	err = ch.ExchangeDeclare(test_exchange, "headers", false, true, false, false, nil)
	require.NoError(t, err)

	queue, err := ch.QueueDeclare(test_queue, false, false, false, false, nil)
	require.NoError(t, err)

	err = ch.QueueBind(queue.Name, test_queue, test_exchange, false, nil)
	require.NoError(t, err)

	msgs, err := ch.Consume(queue.Name, "", true, false, false, false, nil)
	require.NoError(t, err)

	consumer := rabbitConsumer{
		msgs: msgs,
	}

	return consumer
}
