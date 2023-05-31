package main

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/nats-io/nats.go"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	body = `{"key1": "value1","key2": "value2"}`

	rabbit_header = `{"header1": ["1", "2"],"header2": ["3", "4"]}`
	nats_header   = `{"header1": ["1", "2"],"header2": ["3", "4"]}`

	test_queue    = "test-queue"
	test_exchange = "test-exchange"
	test_subject  = "test-subject"
	test_channel  = "test-channel"

	rabbit_url = "amqp://guest:guest@localhost:5672/"
	nats_url   = "localhost:4222"
	redis_url  = "localhost:6379"
)

func mockFileReader(filename string) ([]byte, error) {
	switch filename {
	case "body.json":
		return []byte(body), nil
	case "nats-headers.json":
		return []byte(nats_header), nil
	case "rabbit-headers.json":
		return []byte(rabbit_header), nil
	default:
		return nil, fmt.Errorf("invalid file name requested")
	}
}

type natsSubscriber struct {
	msgs chan *nats.Msg
}

func setupNats(t *testing.T) natsSubscriber {
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
			msg, _ := sub.NextMsg(time.Second * 10)
			natsSub.msgs <- msg
		}
	}()

	return natsSub
}

type redisSubscriber struct {
	msgs chan *redis.Message
}

func setupRedis(t *testing.T) redisSubscriber {
	client := redis.NewClient(&redis.Options{
		Addr: redis_url,
	})

	res := client.Ping(context.Background())
	require.NoError(t, res.Err())

	redisSub := redisSubscriber{
		msgs: make(chan *redis.Message),
	}

	subscriber := client.Subscribe(context.Background(), test_channel)
	t.Cleanup(func() {
		_ = subscriber.Close()
		_ = client.Shutdown(context.Background())
	})

	go func() {
		for {
			msg, _ := subscriber.ReceiveMessage(context.Background())
			redisSub.msgs <- msg
		}
	}()

	return redisSub
}

type rabbitConsumer struct {
	msgs <-chan amqp.Delivery
}

func setupRabbit(t *testing.T) rabbitConsumer {
	var conn *amqp.Connection
	var err error
	// try to connect up to 10 times, waiting a second between each attempt
	for i := 0; i < 10; i++ {
		conn, err = amqp.Dial(rabbit_url)
		if err == nil {
			break
		}

		time.Sleep(time.Second)
	}
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
