package brokers

import (
	"context"
	"encoding/json"

	"cloud.google.com/go/pubsub"
	"github.com/pkg/errors"
	"github.com/willdot/sendit/config"
	"github.com/willdot/sendit/service"
	"google.golang.org/api/option"
)

// GooglePubSubPublisher is a publisher that can send messages to a Google Pub/Sub server
type GooglePubSubPublisher struct {
	client *pubsub.Client
}

// NewGooglePubSubPublisher will create a connection to a Google Pub/Sub server. Shutdown on the returned publisher should be called
// to close the connection once finished
func NewGooglePubSubPublisher(cfg *config.Config) (*GooglePubSubPublisher, error) {
	options := make([]option.ClientOption, 0)
	if cfg.GooglePubSubCfg.DisableAuth {
		options = append(options, option.WithoutAuthentication())
	}

	if cfg.URL != "" {
		options = append(options, option.WithEndpoint(cfg.URL))
	}

	client, err := pubsub.NewClient(context.Background(), cfg.GooglePubSubCfg.ProjectID, options...)
	if err != nil {
		return nil, errors.Wrap(err, "failed to connect to Google Pub/Sub")
	}

	return &GooglePubSubPublisher{
		client: client,
	}, nil
}

// Shutdown will close the Google Pub/Sub connection
func (p *GooglePubSubPublisher) Shutdown() {
	_ = p.client.Close()
}

// Send will send the provided message
func (p *GooglePubSubPublisher) Send(destination string, msg service.Message) error {
	headers, err := convertGooglePubSubHeaders(msg.Headers)
	if err != nil {
		return err
	}
	t := p.client.Topic(destination)
	res := t.Publish(context.Background(), &pubsub.Message{
		Data:       msg.Body,
		Attributes: headers,
	})

	_, err = res.Get(context.Background())
	if err != nil {
		return errors.Wrap(err, "failed to publish message")
	}

	return nil
}

func convertGooglePubSubHeaders(headerData []byte) (map[string]string, error) {
	if headerData == nil {
		return nil, nil
	}

	var headers map[string]string
	err := json.Unmarshal(headerData, &headers)
	if err != nil {
		return nil, errors.Wrap(err, "failed to convert header data")
	}

	return headers, nil
}
