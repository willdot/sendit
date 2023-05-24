package rabbit

import (
	"encoding/json"
	"fmt"

	"github.com/pkg/errors"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/willdot/sendit/config"
)

// DestinationType is where the message will be sent, either directly to a queue or to an exchange
type DestinationType string

const (
	DestinationTypeQueue    DestinationType = "queue"
	DestinationTypeExchange DestinationType = "exchange"
)

// RabbitPublisher is a publisher that can send messages to a RabbitMQ server
type RabbitPublisher struct {
	conn            *amqp.Connection
	destinationType DestinationType
	url             string
}

// NewRabbitPublisher will create a connection to a RabbitMQ server. Shutdown on the returned publisher should be called
// to close the connection once finished
func NewRabbitPublisher(cfg *config.Config) (*RabbitPublisher, error) {
	// create connection
	conn, err := amqp.Dial(cfg.URL)
	if err != nil {
		return nil, errors.Wrap(err, "failed to open connection")
	}

	var destinationType DestinationType
	switch cfg.Broker {
	case config.RabbitExchangeBroker:
		destinationType = DestinationTypeExchange
	case config.RabbitQueueBroker:
		destinationType = DestinationTypeQueue
	default:
		return nil, fmt.Errorf("invalid destination type provided '%s'. Should be either Queue or Exchange")
	}

	return &RabbitPublisher{
		conn:            conn,
		destinationType: destinationType,
		url:             cfg.URL,
	}, nil
}

// Shutdown will close the RabbitMQ connection
func (r *RabbitPublisher) Shutdown() {
	r.conn.Close()
}

// Publish will send the provided message
func (r *RabbitPublisher) Publish(destinationName string, msgBody, headersData []byte) error {
	headers, err := convertHeaders(headersData)
	if err != nil {
		return err
	}

	// open a channel
	c, err := r.conn.Channel()
	if err != nil {
		return errors.Wrap(err, "failed to open channel")
	}
	defer c.Close()

	switch r.destinationType {
	case DestinationTypeExchange:
		return r.publishToExchange(c, destinationName, msgBody, headers)
	case DestinationTypeQueue:
		return r.publishToQueue(c, destinationName, msgBody, headers)
	default:
	}

	return nil
}

func convertHeaders(headerData []byte) (map[string]interface{}, error) {
	if headerData == nil {
		return nil, nil
	}

	var headers map[string]interface{}
	err := json.Unmarshal(headerData, &headers)
	if err != nil {
		return nil, errors.Wrap(err, "failed to convert header data")
	}

	return headers, nil
}

func (r *RabbitPublisher) publishToExchange(c *amqp.Channel, exchangeName string, msg []byte, headers map[string]interface{}) error {
	err := c.ExchangeDeclarePassive(exchangeName, "headers", false, false, false, false, nil)
	if err != nil {
		return errors.Wrap(err, "failed to declare exchange")
	}

	err = c.Publish(exchangeName, "", false, false, amqp.Publishing{
		Headers:     headers,
		ContentType: "application/json",
		Body:        msg,
	})

	if err != nil {
		return errors.Wrapf(err, "failed to publish message to exchange '%s': %s\n", exchangeName, err.Error())
	}

	return nil
}

func (r *RabbitPublisher) publishToQueue(c *amqp.Channel, queueName string, msg []byte, headers map[string]interface{}) error {
	queue, err := c.QueueDeclarePassive(queueName, false, false, false, false, nil)
	if err != nil {
		return errors.Wrap(err, "failed to declare queue")
	}

	err = c.Publish("", queueName, false, false, amqp.Publishing{
		Headers:     headers,
		ContentType: "application/json",
		Body:        msg,
	})

	if err != nil {
		return errors.Wrapf(err, "failed to publish message to queue '%s': %s", queue.Name, err.Error())
	}

	return nil
}
