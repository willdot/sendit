package main

import (
	"fmt"
	"log"
	"os"

	"github.com/pkg/errors"

	"github.com/willdot/sendit/brokers"
	"github.com/willdot/sendit/config"
	"github.com/willdot/sendit/input"
	"github.com/willdot/sendit/service"
)

func main() {
	messageBrokers := []string{
		config.RabbitExchangeBroker,
		config.RabbitQueueBroker,
		config.NatsBroker,
		config.RedisBroker,
	}

	selectedBroker, quit := input.PromptUserForSingleChoice(messageBrokers, "Select which message broker you wish to use")
	if quit {
		fmt.Println("you quit")
		return
	}

	flags := config.GetFlags(selectedBroker)

	cfg, err := config.NewConfig(selectedBroker, flags)
	if err != nil {
		log.Fatal(err)
	}

	fileReader := func(filename string) ([]byte, error) {
		return os.ReadFile(filename)
	}

	err = send(cfg, fileReader)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Finished 🎉")
}

type readFileFunc func(filename string) ([]byte, error)

func send(cfg *config.Config, fr readFileFunc) error {
	bodyData, err := fr(cfg.BodyFileName)
	if err != nil {
		return errors.Wrap(err, "failed to read message body file")
	}

	var headersData []byte
	if cfg.HeadersFileName != "" {
		var err error
		headersData, err = fr(cfg.HeadersFileName)
		if err != nil {
			return errors.Wrap(err, "failed to read headers file")
		}
	}

	var publisher service.Publisher
	switch cfg.Broker {
	case config.RabbitExchangeBroker, config.RabbitQueueBroker:
		publisher, err = brokers.NewRabbitPublisher(cfg)
	case config.NatsBroker:
		publisher, err = brokers.NewNatsPublisher(cfg)
	case config.RedisBroker:
		publisher, err = brokers.NewRedisPublisher(cfg)
	default:
		return errors.New("invalid broker configured")
	}
	if err != nil {
		return err
	}

	service := service.New(publisher, cfg)
	defer service.Shutdown()

	err = service.Run(bodyData, headersData)
	if err != nil {
		return err
	}

	return nil
}
