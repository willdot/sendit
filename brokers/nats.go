package brokers

import (
	"encoding/json"

	"github.com/nats-io/nats.go"
	"github.com/pkg/errors"
	"github.com/willdot/sendit/config"
	"github.com/willdot/sendit/service"
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

// Send will send the provided message
func (p *NatsPublisher) Send(destination string, msg service.Message) error {
	headers, err := convertNatsHeaders(msg.Headers)
	if err != nil {
		return err
	}
	err = p.conn.PublishMsg(&nats.Msg{
		Data:    msg.Body,
		Subject: destination,
		Header:  headers,
	})
	if err != nil {
		return errors.Wrap(err, "failed to send message")
	}

	return nil
}

func convertNatsHeaders(headerData []byte) (nats.Header, error) {
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
