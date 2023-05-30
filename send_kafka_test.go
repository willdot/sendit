package main

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/willdot/sendit/config"
)

const (
	kafka_header = `
	{
		"header1": "1",
		"header2": "2"
	}
	`
	test_topic = "test-topic"

	kafka_url = "localhost:29092"
)

func TestSendKafka(t *testing.T) {
	cfg := &config.Config{
		Broker: config.KafkaBroker,
		KafkaCfg: &config.KafkaConfig{
			Topic: test_topic,
		},
		URL:             kafka_url,
		BodyFileName:    "body.json",
		HeadersFileName: "kafka-headers.json",
		Repeat:          1,
	}

	consumer := setupKafka(t, context.Background())

	err := send(cfg, &mockFileReader{})
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	select {
	case msg := <-consumer.pc.Messages():
		assert.Equal(t, string(body), string(msg.Value))
		assertKafkaHeadersMatch(t, kafka_header, msg.Headers)
	case <-ctx.Done():
		t.Fatalf("timed out waiting for messages")
	}
}

func TestSendKafkaRepeat(t *testing.T) {
	cfg := &config.Config{
		Broker: config.KafkaBroker,
		KafkaCfg: &config.KafkaConfig{
			Topic: test_topic,
		},
		URL:             kafka_url,
		BodyFileName:    "body.json",
		HeadersFileName: "kafka-headers.json",
		Repeat:          5,
	}

	consumer := setupKafka(t, context.Background())

	err := send(cfg, &mockFileReader{})
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	for i := 0; i < 5; i++ {
		select {
		case msg := <-consumer.pc.Messages():
			assert.Equal(t, string(body), string(msg.Value))
			assertKafkaHeadersMatch(t, kafka_header, msg.Headers)
		case <-ctx.Done():
			t.Fatalf("timed out waiting for messages")
		}
	}

	t.Fail()
}

func assertKafkaHeadersMatch(t *testing.T, expected string, actual []*sarama.RecordHeader) {
	var headers map[string]string
	err := json.Unmarshal([]byte(expected), &headers)
	require.NoError(t, err)

	var expectedHeader []*sarama.RecordHeader

	for k, v := range headers {
		expectedHeader = append(expectedHeader, &sarama.RecordHeader{
			Key:   []byte(k),
			Value: []byte(v),
		})
	}

	assert.ElementsMatch(t, expectedHeader, actual)
}

type kafkaConsumer struct {
	msgs chan *sarama.ConsumerMessage
	pc   sarama.PartitionConsumer
}

func setupKafka(t *testing.T, ctx context.Context) kafkaConsumer {
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

	// go func() {
	// 	for {
	// 		msg := <-pc.Messages()
	// 		kafkaConsumer.msgs <- msg
	// 	}
	// }()

	kafkaConsumer := kafkaConsumer{
		msgs: make(chan *sarama.ConsumerMessage),
		pc:   pc,
	}

	return kafkaConsumer
}
