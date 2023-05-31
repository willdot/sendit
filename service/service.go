package service

import (
	"github.com/pkg/errors"
	"github.com/willdot/sendit/config"
)

// Publisher defines the functions that a message broker must use
type Publisher interface {
	Send(dest string, message Message) error
	Shutdown()
}

// Service is capable of sending messages to a configured broker
type Service struct {
	publisher   Publisher
	repeat      int
	destination string
}

// New will configure a  new service that can send messages
func New(publisher Publisher, cfg *config.Config) *Service {
	return &Service{
		publisher:   publisher,
		repeat:      cfg.Repeat,
		destination: cfg.Destination(),
	}
}

// Shutdown will cleanly shutdown brokers
func (s *Service) Shutdown() {
	s.publisher.Shutdown()
}

// Message defines a data that can be sent
type Message struct {
	Body          []byte
	Headers       []byte
	messageNumber int // this is used to internally track the messages sent
}

// Run will send the configured number of messages to the broker
func (s *Service) Run(body, headers []byte) error {
	message := Message{
		Body:    body,
		Headers: headers,
	}
	for i := 0; i < s.repeat; i++ {
		// increment the message number so we can track it internally
		message.messageNumber = i

		err := s.publisher.Send(s.destination, message)
		if err != nil {
			return errors.Wrap(err, "error sending message")
		}
	}

	return nil
}
