package kafka

import (
	"encoding/json"

	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
	"github.com/willdot/sendit/config"
)

// KafkaPublisher is a publisher that can send messages to a Kafka server
type KafkaPublisher struct {
	conn sarama.SyncProducer
}

// NewKafkaPublisher will create a connection to a Kafka server. Shutdown on the returned publisher should be called
// to close the connection once finished
func NewKafkaPublisher(cfg *config.Config) (*KafkaPublisher, error) {
	kafkaConfig := sarama.NewConfig()
	kafkaConfig.Producer.Return.Successes = true
	conn, err := sarama.NewSyncProducer([]string{cfg.URL}, kafkaConfig)
	if err != nil {
		return nil, err
	}

	return &KafkaPublisher{
		conn: conn,
	}, nil
}

// Shutdown will close the Kafka connection
func (p *KafkaPublisher) Shutdown() {
	_ = p.conn.Close()
}

type Message struct {
	Body       []byte
	HeaderData []byte
}

// Publish will send the provided message
func (p *KafkaPublisher) Publish(destination string, msgs []Message) error {
	msgsToSend := make([]*sarama.ProducerMessage, 0, len(msgs))
	for _, msg := range msgs {
		headers, err := convertHeaders(msg.HeaderData)
		if err != nil {
			return err
		}

		msgsToSend = append(msgsToSend, &sarama.ProducerMessage{
			Topic:   destination,
			Value:   sarama.StringEncoder(msg.Body),
			Headers: headers,
		})
	}

	err := p.conn.SendMessages(msgsToSend)
	if err != nil {
		return errors.Wrap(err, "failed to send message")
	}

	return nil
}

func convertHeaders(headerData []byte) ([]sarama.RecordHeader, error) {
	if headerData == nil {
		return nil, nil
	}

	var headers map[string]string
	err := json.Unmarshal(headerData, &headers)
	if err != nil {
		return nil, errors.Wrap(err, "failed to convert header data")
	}

	var result []sarama.RecordHeader

	for k, v := range headers {
		result = append(result, sarama.RecordHeader{
			Key:   []byte(k),
			Value: []byte(v),
		})
	}

	return result, nil
}
