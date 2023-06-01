package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func defaultFlags() flags {
	return flags{
		bodyFileName:    "body.json",
		destinationName: "destination",
		subject:         "subject",
		channel:         "channel",
		topic:           "topic",
		projectID:       "project-id",
		repeat:          1,
	}
}

func TestNoBodyFileNameFlagSet(t *testing.T) {
	fl := defaultFlags()
	fl.bodyFileName = ""

	_, err := NewConfig(RabbitExchangeBroker, fl)
	require.Error(t, err)
	assert.Equal(t, "body flag must be provided", err.Error())
}

func TestHeadersProvidedForRedis(t *testing.T) {
	fl := defaultFlags()
	fl.headersFileName = "headers.json"

	_, err := NewConfig(RedisBroker, fl)
	require.Error(t, err)
	assert.Equal(t, "headers not valid for Redis broker", err.Error())
}

func TestHeadersProvidedForGooglePubSub(t *testing.T) {
	fl := defaultFlags()
	fl.headersFileName = "headers.json"

	_, err := NewConfig(GooglePubSubBroker, fl)
	require.Error(t, err)
	assert.Equal(t, "headers not valid for Redis broker", err.Error())
}

func TestInvalidRepeat(t *testing.T) {
	fl := defaultFlags()
	fl.repeat = 0

	_, err := NewConfig(RedisBroker, fl)
	require.Error(t, err)
	assert.Equal(t, "repeat flag must be >= 1", err.Error())
}

func TestInvalidBrokerConfig(t *testing.T) {
	tt := map[string]struct {
		broker     string
		alterFlags func(fl *flags)
		expected   string
	}{
		"rabbit": {
			broker:   RabbitExchangeBroker,
			expected: "destination flag should be provided",
			alterFlags: func(fl *flags) {
				fl.destinationName = ""
			}},
		"nats": {
			broker:   NatsBroker,
			expected: "subject flag should be provided",
			alterFlags: func(fl *flags) {
				fl.subject = ""
			},
		},
		"redis": {
			broker:   RedisBroker,
			expected: "channel flag should be provided",
			alterFlags: func(fl *flags) {
				fl.channel = ""
			},
		},
		"google pub/sub - no topic": {
			broker:   GooglePubSubBroker,
			expected: "topic flag should be provided",
			alterFlags: func(fl *flags) {
				fl.topic = ""
			},
		},
		"google pub/sub - no project ID": {
			broker:   GooglePubSubBroker,
			expected: "project_id flag should be provided",
			alterFlags: func(fl *flags) {
				fl.projectID = ""
			},
		},
	}

	for name, tc := range tt {
		t.Run(name, func(t *testing.T) {
			fl := defaultFlags()
			tc.alterFlags(&fl)

			_, err := NewConfig(tc.broker, fl)
			require.Error(t, err)

			assert.Equal(t, tc.expected, err.Error())
		})
	}
}
