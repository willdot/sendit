package nats

import (
	"github.com/nats-io/nats.go"
	"github.com/pkg/errors"
)

type NatsPublisher struct {
	natsConn *nats.Conn
}

func NewNatsPublisher(url string) (*NatsPublisher, error) {
	nc, err := nats.Connect(url)
	if err != nil {
		return nil, errors.Wrap(err, "failed to connect to nats")
	}

	return &NatsPublisher{
		natsConn: nc,
	}, nil
}

func (p *NatsPublisher) Shutdown() {
	p.natsConn.Close()
}

func (p *NatsPublisher) Send() error {
	err := p.natsConn.Publish("test", []byte("hello there"))
	if err != nil {
		return errors.Wrap(err, "failed to send message")
	}

	return nil
}
