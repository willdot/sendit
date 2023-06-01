package config

import (
	"errors"
)

const (
	RabbitQueueBroker    = "RabbitMQ - Queue"
	RabbitExchangeBroker = "RabbitMQ - Exchange"
	NatsBroker           = "NATs"
	RedisBroker          = "Redis"
	GooglePubSubBroker   = "Google Pub/Sub"
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
	RedisCfg        *RedisConfig
	GooglePubSubCfg *GooglePubSubConfig
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

	if brokerType == RedisBroker {
		cfg.RedisCfg = &RedisConfig{
			Channel: flags.channel,
		}
	}

	if brokerType == GooglePubSubBroker {
		cfg.GooglePubSubCfg = &GooglePubSubConfig{
			Topic:       flags.topic,
			DisableAuth: flags.disableAuth,
			ProjectID:   flags.projectID,
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

	if c.Broker == RedisBroker {
		if err := c.RedisCfg.validate(); err != nil {
			return err
		}
		if c.HeadersFileName != "" {
			return errors.New("headers not valid for Redis broker")
		}
	}

	if c.Broker == GooglePubSubBroker {
		if err := c.GooglePubSubCfg.validate(); err != nil {
			return err
		}
	}

	if c.BodyFileName == "" {
		return errors.New("body flag must be provided")
	}

	if c.Repeat < 1 {
		return errors.New("repeat flag must be >= 1")
	}

	return nil
}

// Destination will return where the message has been configured to be sent depending on the broker. Different brokers call
// this different things so the flags the users set are named after what a broker uses, and this will return that value
func (c Config) Destination() string {
	switch c.Broker {
	case RabbitExchangeBroker, RabbitQueueBroker:
		return c.RabbitCfg.DestinationName
	case NatsBroker:
		return c.NatsCfg.Subject
	case RedisBroker:
		return c.RedisCfg.Channel
	case GooglePubSubBroker:
		return c.GooglePubSubCfg.Topic
	default:
		return ""
	}
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

// RedisConfig contains config specifically for Redis
type RedisConfig struct {
	Channel string
}

func (c RedisConfig) validate() error {
	if c.Channel == "" {
		return errors.New("channel flag should be provided")
	}

	return nil
}

// GooglePubSubConfig contains config specifically for Google Pub/Sub
type GooglePubSubConfig struct {
	Topic       string
	ProjectID   string
	DisableAuth bool
}

func (c GooglePubSubConfig) validate() error {
	if c.Topic == "" {
		return errors.New("topic flag should be provided")
	}

	if c.ProjectID == "" {
		return errors.New("project_id flag should be provided")
	}

	return nil
}

func defaultURL(broker string) string {
	switch broker {
	case RabbitQueueBroker, RabbitExchangeBroker:
		return "amqp://guest:guest@localhost:5672/"
	case NatsBroker:
		return "localhost:4222"
	case RedisBroker:
		return "localhost:6379"
	case GooglePubSubBroker:
		// there is no default URL for google pub/sub
		return ""
	default:
		return ""
	}
}
