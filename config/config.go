package config

import (
	"errors"
)

const (
	RabbitQueueBroker    = "RabbitMQ - Queue"
	RabbitExchangeBroker = "RabbitMQ - Exchange"
	NatsBroker           = "NATs"
	KafkaBroker          = "Kafka"
)

// Config contains all of the configuration required to send messages to a broker
type Config struct {
	URL             string
	Broker          string
	BodyFileName    string
	HeadersFileName string
	Repeat          int
	RabbitCfg       *RabbitConfig
	NatsCfg         *NatsConfig
	KafkaCfg        *KafkaConfig
}

// NewConfig will create and validate configuration based on the provided flags
func NewConfig(brokerType string, flags flags) (*Config, error) {
	cfg := Config{
		Broker:          brokerType,
		BodyFileName:    flags.bodyFileName,
		HeadersFileName: flags.headersFileName,
		Repeat:          flags.repeat,
		URL:             flags.url,
	}

	if brokerType == RabbitExchangeBroker || brokerType == RabbitQueueBroker {
		cfg.RabbitCfg = &RabbitConfig{
			DestinationName: flags.destinationName,
		}
	}

	if brokerType == NatsBroker {
		cfg.NatsCfg = &NatsConfig{
			Subject: flags.subject,
		}
	}

	if brokerType == KafkaBroker {
		cfg.KafkaCfg = &KafkaConfig{
			Topic: flags.topic,
		}
	}

	err := cfg.validate()
	if err != nil {
		return nil, err
	}

	return &cfg, nil
}

func (c Config) validate() error {
	if c.Broker == RabbitExchangeBroker || c.Broker == RabbitQueueBroker {
		if err := c.RabbitCfg.validate(); err != nil {
			return err
		}
	}

	if c.Broker == NatsBroker {
		if err := c.NatsCfg.validate(); err != nil {
			return err
		}
	}

	if c.BodyFileName == "" {
		return errors.New("body flag must be provided")
	}

	return nil
}

// RabbitConfig contains config specifically for RabbitMQ
type RabbitConfig struct {
	DestinationName string
}

func (c RabbitConfig) validate() error {
	if c.DestinationName == "" {
		return errors.New("destination flag should be provided")
	}

	return nil
}

// NatsConfig contains config specifically for NATs
type NatsConfig struct {
	Subject string
}

func (c NatsConfig) validate() error {
	if c.Subject == "" {
		return errors.New("subject flag should be provided")
	}

	return nil
}

// KafkaConfig contains config specifically for Kafka
type KafkaConfig struct {
	Topic string
}

func (c KafkaConfig) validate() error {
	if c.Topic == "" {
		return errors.New("topic flag should be provided")
	}

	return nil
}

func defaultURL(broker string) string {
	switch broker {
	case RabbitQueueBroker, RabbitExchangeBroker:
		return "amqp://guest:guest@localhost:5672/"
	case NatsBroker:
		return "localhost:4222"
	case KafkaBroker:
		return "localhost:9092"
	default:
		return ""
	}
}
