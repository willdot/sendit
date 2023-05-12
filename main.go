package main

import (
	"fmt"
	"log"

	"github.com/pkg/errors"

	"github.com/willdot/sendit/input"
	"github.com/willdot/sendit/nats"
	"github.com/willdot/sendit/rabbit"
)

type broker string

const (
	natsBroker   broker = "NATs"
	rabbitBroker broker = "RabbitMQ"
)

// these are here just to test at the moment before actual user input is implemented
var (
	destinationName = "test"
	msg             = []byte("hello there")

	rabbitURL = "amqp://guest:guest@localhost:5672/"
	natsURL   = "localhost:4222"
)

func main() {
	messageBrokers := []string{
		string(natsBroker),
		string(rabbitBroker),
	}

	selected, quit := input.PromptUserForSingleChoice(messageBrokers, "Select which message broker you wish to use")
	if quit {
		fmt.Println("you quit")
		return
	}

	fmt.Printf("you selected '%s'\n", selected)
	switch selected {
	case string(rabbitBroker):
		err := sendRabbit()
		if err != nil {
			log.Fatal(err)
		}
	case string(natsBroker):
		err := sendNats()
		if err != nil {
			log.Fatal(err)
		}
	default:
	}
}

func sendRabbit() error {
	choices := []string{
		string(rabbit.DestinationTypeExchange),
		string(rabbit.DestinationTypeQueue),
	}
	selected, quit := input.PromptUserForSingleChoice(choices, "please select if you wish to send to a queue or exchange")
	if quit {
		return nil
	}

	publisher, err := rabbit.NewRabbitPublisher(rabbitURL, rabbit.DestinationType(selected))
	if err != nil {
		return errors.Wrap(err, "failed to create new rabbit publisher")
	}
	defer publisher.Shutdown()

	err = publisher.Publish(destinationName, msg, map[string]interface{}{"header1": "value1"})
	if err != nil {
		return errors.Wrap(err, "failed to send message")
	}
	return nil
}

func sendNats() error {
	publisher, err := nats.NewNatsPublisher(natsURL)
	if err != nil {
		return errors.Wrap(err, "failed to create new nats publisher")
	}
	defer publisher.Shutdown()

	err = publisher.Publish(destinationName, msg)
	if err != nil {
		return errors.Wrap(err, "failed to send message")
	}

	return nil
}
