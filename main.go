package main

import (
	"fmt"
	"log"
	"os"

	"github.com/pkg/errors"

	"github.com/willdot/sendit/config"
	"github.com/willdot/sendit/nats"
	"github.com/willdot/sendit/rabbit"
	"github.com/willdot/sendit/ui"
)

func main() {
	messageBrokers := []string{
		config.NatsBroker,
		config.RabbitExchangeBroker,
		config.RabbitQueueBroker,
	}

	selectedBroker, quit := ui.PromptUserForSingleChoice(messageBrokers, "Select which message broker you wish to use")
	if quit {
		fmt.Println("you quit")
		return
	}

	flags := config.GetFlags(selectedBroker)

	cfg, err := config.NewConfig(selectedBroker, flags)
	if err != nil {
		log.Fatal(err)
	}

	err = send(cfg, &RealFileReader{})
	if err != nil {
		log.Fatal(err)
	}
}

type fileReader interface {
	ReadFile(filename string) ([]byte, error)
}

type RealFileReader struct{}

func (fr *RealFileReader) ReadFile(filename string) ([]byte, error) {
	return os.ReadFile(filename)
}

func send(cfg *config.Config, fr fileReader) error {
	bodyData, err := fr.ReadFile(cfg.BodyFileName)
	if err != nil {
		return errors.Wrap(err, "failed to read message body file")
	}

	var headersData []byte
	if cfg.HeadersFileName != "" {
		var err error
		headersData, err = fr.ReadFile(cfg.HeadersFileName)
		if err != nil {
			return errors.Wrap(err, "failed to read headers file")
		}
	}

	switch cfg.Broker {
	case config.RabbitExchangeBroker, config.RabbitQueueBroker:
		err := sendRabbit(cfg, bodyData, headersData)
		if err != nil {
			return errors.Wrap(err, "failed to send to RabbitMQ")
		}
	case config.NatsBroker:
		err := sendNats(cfg, bodyData, headersData)
		if err != nil {
			return errors.Wrap(err, "failed to send to NATs")
		}
	default:
	}
	return nil
}

func sendRabbit(cfg *config.Config, msgBody, headers []byte) error {
	publisher, err := rabbit.NewRabbitPublisher(cfg)
	if err != nil {
		return errors.Wrap(err, "failed to create new rabbit publisher")
	}
	defer publisher.Shutdown()

	for i := 0; i < cfg.Repeat; i++ {
		err = publisher.Publish(cfg.RabbitCfg.DestinationName, msgBody, headers)
		if err != nil {
			return errors.Wrap(err, "failed to send message")
		}
	}
	return nil
}

func sendNats(cfg *config.Config, msgBody, headers []byte) error {
	publisher, err := nats.NewNatsPublisher(cfg)
	if err != nil {
		return errors.Wrap(err, "failed to create new nats publisher")
	}
	defer publisher.Shutdown()
	for i := 0; i < cfg.Repeat; i++ {
		err = publisher.Publish(cfg.NatsCfg.Subject, msgBody, headers)
		if err != nil {
			return errors.Wrap(err, "failed to send message")
		}
	}
	return nil

}
