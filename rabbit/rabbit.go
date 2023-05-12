package rabbit

import (
	"log"

	"github.com/pkg/errors"
	amqp "github.com/rabbitmq/amqp091-go"
)

type RabbitPublisher struct {
	connection      *amqp.Connection
	destinationType string
}

func NewRabbitPublisher(url string, destinationType string) (*RabbitPublisher, error) {
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

// Publish will send a given message onto a given queue or exchange
func (r *RabbitPublisher) Publish(destinationName string, msg []byte, headers map[string]interface{}) error {
	if r.destinationType == "queue" {
		return r.publishToQueue(destinationName, msg, headers)
	}

	return r.publishToExchange(destinationName, msg, headers)
}

func (r *RabbitPublisher) publishToExchange(exchangeName string, msg []byte, headers map[string]interface{}) error {
	// open a channel
	c, err := r.connection.Channel()
	if err != nil {
		return errors.Wrap(err, "failed to open channel")
	}
	defer c.Close()

	err = c.ExchangeDeclarePassive(exchangeName, "headers", false, false, false, false, nil)
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

func (r *RabbitPublisher) publishToQueue(queueName string, msg []byte, headers map[string]interface{}) error {
	// open a channel
	c, err := r.connection.Channel()
	if err != nil {
		log.Fatalf("failed to open channel: %s", err.Error())
	}
	defer c.Close()

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
