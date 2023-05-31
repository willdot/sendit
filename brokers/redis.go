package brokers

import (
	"context"

	"github.com/go-redis/redis/v8"
	"github.com/pkg/errors"
	"github.com/willdot/sendit/config"
	"github.com/willdot/sendit/service"
)

// RedisPublisher is a publisher that can send messages to a Redis server
type RedisPublisher struct {
	client *redis.Client
}

// NewRedisPublisher will create a connection to a Redis server. Shutdown on the returned publisher should be called
// to close the connection once finished
func NewRedisPublisher(cfg *config.Config) (*RedisPublisher, error) {
	client := redis.NewClient(&redis.Options{
		Addr: cfg.URL,
	})

	err := client.Ping(context.Background()).Err()
	if err != nil {
		return nil, errors.Wrap(err, "failed to connect to Redis")
	}

	return &RedisPublisher{
		client: client,
	}, nil
}

// Shutdown will close the Redis connection
func (p *RedisPublisher) Shutdown() {
	_ = p.client.Shutdown(context.Background())
}

// Send will send the provided message
func (p *RedisPublisher) Send(destination string, msg service.Message) error {
	err := p.client.Publish(context.Background(), destination, msg.Body).Err()
	if err != nil {
		return errors.Wrap(err, "failed to send message")
	}

	return nil
}
