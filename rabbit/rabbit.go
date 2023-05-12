package rabbit

import (
	"github.com/pkg/errors"
	amqp "github.com/rabbitmq/amqp091-go"
)

type DestinationType string

const (
	DestinationTypeQueue    DestinationType = "queue"
	DestinationTypeExchange DestinationType = "exchange"
)

type RabbitPublisher struct {
	connection      *amqp.Connection
	destinationType DestinationType
	url             string
}

func (d DestinationType) validate() error {
	if d == DestinationTypeQueue {
		return nil
	}

	if d == DestinationTypeExchange {
		return nil
	}

	return errors.New("invalid destination type")
}

func NewRabbitPublisher(url string, destinationType DestinationType) (*RabbitPublisher, error) {
	err := destinationType.validate()
	if err != nil {
		return nil, err
	}

	// create connection
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, errors.Wrap(err, "failed to open connection")
	}

	return &RabbitPublisher{
		connection:      conn,
		destinationType: destinationType,
	}, nil
}

// Shutdown will close the rabbit connection
func (r *RabbitPublisher) Shutdown() {
	r.connection.Close()
}

// Publish will send the provided message
func (r *RabbitPublisher) Publish(destinationName string, msg []byte, headers map[string]interface{}) error {
	// open a channel
	c, err := r.connection.Channel()
	if err != nil {
		return errors.Wrap(err, "failed to open channel")
	}
	defer c.Close()

	switch r.destinationType {
	case DestinationTypeExchange:
		return r.publishToExchange(c, destinationName, msg, headers)
	case DestinationTypeQueue:
		return r.publishToQueue(c, destinationName, msg, headers)
	default:
	}

	return nil
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
