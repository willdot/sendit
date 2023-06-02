package brokers

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/pkg/errors"
	"github.com/willdot/sendit/config"
	"github.com/willdot/sendit/service"
)

// SqsPublisher is a publisher that can send messages to a SQS server
type SqsPublisher struct {
	session *session.Session
}

// NewSqsPublisher will create a connection to a Sqs server. Shutdown on the returned publisher should be called
// to close the connection once finished
func NewSqsPublisher(cfg *config.Config) (*SqsPublisher, error) {
	awsConfig := aws.Config{
		Region:   aws.String("us-west-2"),
		Endpoint: aws.String(cfg.URL),
	}
	if cfg.SqsConfig.DisableAuth {
		awsConfig.Credentials = credentials.NewStaticCredentials("fakeMyKeyId", "fakeSecretAccessKey", "")
	}

	sess, err := session.NewSessionWithOptions(session.Options{
		//Profile: "default",
		Config: awsConfig,
	})
	if err != nil {
		return nil, errors.Wrap(err, "failed to connect to SQS")
	}

	return &SqsPublisher{
		session: sess,
	}, nil
}

// Shutdown will close the SQS connection
func (p *SqsPublisher) Shutdown() {
	// SQS has no closing so NOOP
}

// Send will send the provided message
func (p *SqsPublisher) Send(destination string, msg service.Message) error {
	queueUrl, err := p.getQueueURL(destination)
	if err != nil {
		return errors.Wrap(err, "failed to get queue URL")
	}

	sqsClient := sqs.New(p.session)

	res, err := sqsClient.SendMessage(&sqs.SendMessageInput{
		QueueUrl:    queueUrl,
		MessageBody: aws.String(string(msg.Body)),
	})
	if err != nil {
		return errors.Wrap(err, "failed to send message")
	}

	fmt.Printf("sent message: %v\n", res.GoString())

	return nil
}

func (p *SqsPublisher) getQueueURL(queue string) (*string, error) {
	sqsClient := sqs.New(p.session)

	result, err := sqsClient.GetQueueUrl(&sqs.GetQueueUrlInput{
		QueueName: &queue,
	})

	if err != nil {
		return nil, err
	}

	return result.QueueUrl, nil
}
