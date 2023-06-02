package config

import (
	"flag"
)

type flags struct {
	bodyFileName    string
	headersFileName string
	repeat          int
	url             string
	destinationName string
	subject         string
	channel         string
	topic           string
	projectID       string
	disableAuth     bool
	queue           string
}

// GetFlags will parse the flags provided by the users input and return the results
func GetFlags(brokerType string) flags {
	bodyFileName := flag.String("body", "", "the file name of the body to send in the message")
	repeat := flag.Int("repeat", 1, "the number of times to send the message (optional - default of 1 will be used)")
	url := flag.String("url", defaultURL(brokerType), "the url of the broker you wish to send to (optional - the default for the broker will be used")
	headersFileName := flag.String("headers", "", "the file name of the header to send in the message (optional)")

	// rabbit flags
	var destinationName *string
	if brokerType == RabbitExchangeBroker || brokerType == RabbitQueueBroker {
		destinationName = flag.String("destination", "", "the queue or exchange to send the message to")
	}

	// nats flags
	var subject *string
	if brokerType == NatsBroker {
		subject = flag.String("subject", "", "the subject you wish to send the message to")
	}

	// redis flags
	var channel *string
	if brokerType == RedisBroker {
		channel = flag.String("channel", "", "the channel you wish to send the message to")
	}

	// google pub/sub flags
	var topic *string
	var projectID *string
	var disableAuth *bool
	if brokerType == GooglePubSubBroker {
		topic = flag.String("topic", "", "the topic you wish to send the message to")
		projectID = flag.String("project_id", "", "the project you wish to use")
		disableAuth = flag.Bool("disable_auth", false, "use this if using locally in emulation mode")
	}

	// sqs flags
	var queue *string
	if brokerType == SqsBroker {
		queue = flag.String("queue", "", "the queue you wish to send the message to")
		disableAuth = flag.Bool("disable_auth", false, "use this if using locally where auth isn't required")
	}

	flag.Parse()

	result := flags{
		bodyFileName: *bodyFileName,
		repeat:       *repeat,
		url:          *url,
	}

	if headersFileName != nil {
		result.headersFileName = *headersFileName
	}
	if destinationName != nil {
		result.destinationName = *destinationName
	}
	if subject != nil {
		result.subject = *subject
	}
	if channel != nil {
		result.channel = *channel
	}
	if topic != nil {
		result.topic = *topic
	}
	if projectID != nil {
		result.projectID = *projectID
	}
	if disableAuth != nil {
		result.disableAuth = *disableAuth
	}
	if queue != nil {
		result.queue = *queue
	}

	return result
}
