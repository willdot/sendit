package nats

import (
	"encoding/json"

	"github.com/nats-io/nats.go"
	"github.com/pkg/errors"
	"github.com/willdot/sendit/config"
)

// NatsPublisher is a publisher that can send messages to a NATs server
type NatsPublisher struct {
	conn *nats.Conn
}

// NewNatsPublisher will create a connection to a NATs server. Shutdown on the returned publisher should be called
// to close the connection once finished
func NewNatsPublisher(cfg *config.Config) (*NatsPublisher, error) {
	nc, err := nats.Connect(cfg.URL)
	if err != nil {
		return nil, errors.Wrap(err, "failed to connect to nats")
	}

	return &NatsPublisher{
		conn: nc,
	}, nil
}

// Shutdown will close the NATs connection
func (p *NatsPublisher) Shutdown() {
	p.conn.Close()
}

// Publish will send the provided message
func (p *NatsPublisher) Publish(destination string, msgBody, headersData []byte) error {
	headers, err := convertHeaders(headersData)
	if err != nil {
		return err
	}
	err = p.conn.PublishMsg(&nats.Msg{
		Data:    msgBody,
		Subject: destination,
		Header:  headers,
	})
	if err != nil {
		return errors.Wrap(err, "failed to send message")
	}

	return nil
}

func convertHeaders(headerData []byte) (nats.Header, error) {
	if headerData == nil {
		return nil, nil
	}

	var headers nats.Header
	err := json.Unmarshal(headerData, &headers)
	if err != nil {
		return nil, errors.Wrap(err, "failed to convert header data")
	}

	return headers, nil
}
