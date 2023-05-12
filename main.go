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
	selected, quit := input.PromptUserForSingleChoice([]string{"queue", "exchange"}, "please select if you wish to send to a queue or exchange")
	if quit {
		return nil
	}

	url := "amqp://guest:guest@localhost:5672/"

	publisher, err := rabbit.NewRabbitPublisher(url, selected)
	if err != nil {
		return errors.Wrap(err, "failed to create new rabbit publisher")
	}
	defer publisher.Shutdown()

	err = publisher.Publish("test", []byte("hello there"), map[string]interface{}{"header1": "value1"})
	if err != nil {
		return errors.Wrap(err, "failed to send message")
	}
	return nil
}

func sendNats() error {
	url := "localhost:4222"
	publisher, err := nats.NewNatsPublisher(url)
	if err != nil {
		return errors.Wrap(err, "failed to create new nats publisher")
	}
	defer publisher.Shutdown()

	err = publisher.Send()
	if err != nil {
		return errors.Wrap(err, "failed to send message")
	}

	return nil
}
