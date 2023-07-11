package main

import (
	"context"
	"fmt"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/Shopify/sarama"
	"github.com/go-redis/redis/v8"
	"github.com/nats-io/nats.go"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
)

const (
	body = `{"key1": "value1","key2": "value2"}`

	rabbit_header         = `{"header1": ["1", "2"],"header2": ["3", "4"]}`
	nats_header           = `{"header1": ["1", "2"],"header2": ["3", "4"]}`
	google_pub_sub_header = `{"header1": "1"}`
	kafka_header          = `{"header1": "1", "header2": "2"}`

	test_queue        = "test-queue"
	test_exchange     = "test-exchange"
	test_subject      = "test-subject"
	test_channel      = "test-channel"
	test_topic        = "test-topic"
	test_subscription = "test-sub"
	test_project_id   = "project_id"

	rabbit_url         = "amqp://guest:guest@localhost:5672/"
	nats_url           = "localhost:4222"
	redis_url          = "localhost:6379"
	google_pub_sub_url = "localhost:8085"
	kafka_url          = "localhost:29092"
)

func mockFileReader(filename string) ([]byte, error) {
	switch filename {
	case "body.json":
		return []byte(body), nil
	case "nats-headers.json":
		return []byte(nats_header), nil
	case "rabbit-headers.json":
		return []byte(rabbit_header), nil
	case "google-pub-sub-headers.json":
		return []byte(google_pub_sub_header), nil
	case "kafka-headers.json":
		return []byte(kafka_header), nil
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

type googlePubSub struct {
	msgs chan *pubsub.Message
}

func setupGooglePubSub(t *testing.T) googlePubSub {
	t.Setenv("PUBSUB_EMULATOR_HOST", google_pub_sub_url)
	ctx := context.Background()

	client, err := pubsub.NewClient(ctx, test_project_id, option.WithoutAuthentication(), option.WithEndpoint(google_pub_sub_url))
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = client.Close()
	})

	// make sure topics and subscriptions are deleted first
	oldTopic := client.Topic(test_topic)
	if oldTopic != nil {
		_ = oldTopic.Delete(ctx)
	}
	oldSubscription := client.Subscription(test_subscription)
	if oldSubscription != nil {
		_ = oldSubscription.Delete(ctx)
	}

	topic, err := client.CreateTopic(ctx, test_topic)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = topic.Delete(ctx)
	})

	sub, err := client.CreateSubscription(ctx, test_subscription, pubsub.SubscriptionConfig{
		Topic: topic,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = sub.Delete(ctx)
	})

	pubSub := googlePubSub{
		msgs: make(chan *pubsub.Message),
	}

	go func() {
		err = sub.Receive(ctx, func(_ context.Context, msg *pubsub.Message) {
			msg.Ack()
			pubSub.msgs <- msg
		})

		require.NoError(t, err)
	}()

	return pubSub
}

type kafkaConsumer struct {
	msgs chan *sarama.ConsumerMessage
}

func setupKakfa(t *testing.T) kafkaConsumer {
	consumer, err := sarama.NewConsumer([]string{kafka_url}, sarama.NewConfig())
	require.NoError(t, err)

	t.Cleanup(func() {
		broker := sarama.NewBroker(kafka_url)
		err := broker.Open(sarama.NewConfig())
		require.NoError(t, err)
		_, err = broker.DeleteTopics(&sarama.DeleteTopicsRequest{
			Topics: []string{test_topic},
		})
		require.NoError(t, err)

		_ = broker.Close()
		_ = consumer.Close()
	})

	pc, err := consumer.ConsumePartition(test_topic, 0, sarama.OffsetOldest)
	require.NoError(t, err)

	kafkaConsumer := kafkaConsumer{
		msgs: make(chan *sarama.ConsumerMessage),
	}

	go func() {
		for {
			msg := <-pc.Messages()
			kafkaConsumer.msgs <- msg
		}
	}()

	return kafkaConsumer
}
