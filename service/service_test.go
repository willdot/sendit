package service

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/willdot/sendit/config"
)

type mockBroker struct {
	sentMessages []Message
	errorAt      int
}

func (m *mockBroker) Shutdown() {}

func (m *mockBroker) Send(dest string, message Message) error {
	if m.errorAt-1 == message.messageNumber {
		return errors.New("this failed")
	}

	m.sentMessages = append(m.sentMessages, message)
	return nil
}

func TestServiceSendsMessages(t *testing.T) {
	mockBroker := mockBroker{}
	cfg := config.Config{
		Repeat: 5,
	}
	body := []byte("body")
	header := []byte("headers")

	service := New(&mockBroker, &cfg)

	err := service.Run(body, header)
	require.NoError(t, err)

	require.Len(t, mockBroker.sentMessages, 5)

	expectedMessages := make([]Message, 0, 5)
	for i := 0; i < 5; i++ {
		expectedMessages = append(expectedMessages, Message{
			Body:          body,
			Headers:       header,
			messageNumber: i,
		})
	}

	assert.ElementsMatch(t, mockBroker.sentMessages, expectedMessages)
}

func TestServiceStopsAfterError(t *testing.T) {
	mockBroker := mockBroker{
		errorAt: 3,
	}
	cfg := config.Config{
		Repeat: 5,
	}
	body := []byte("body")
	header := []byte("headers")

	service := New(&mockBroker, &cfg)

	err := service.Run(body, header)
	require.Error(t, err)
	require.Len(t, mockBroker.sentMessages, 2)

	expectedMessages := make([]Message, 0, 2)
	for i := 0; i < 2; i++ {
		expectedMessages = append(expectedMessages, Message{
			Body:          body,
			Headers:       header,
			messageNumber: i,
		})
	}

	assert.ElementsMatch(t, mockBroker.sentMessages, expectedMessages)
}
