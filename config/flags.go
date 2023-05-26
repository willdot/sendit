package config

import "flag"

type flags struct {
	bodyFileName    string
	repeat          int
	url             string
	destinationName string
	headersFileName string
	subject         string
	topic           string
}

// GetFlags will parse the flags provided by the users input and return the results
func GetFlags(brokerType string) flags {
	bodyFileName := flag.String("body", "", "the file name of the body to send in the message")
	repeat := flag.Int("repeat", 1, "the number of times to send the message")
	url := flag.String("url", defaultURL(brokerType), "the url of the broker you wish to send to")
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

	// kafka flags
	var topic *string
	if brokerType == KafkaBroker {
		topic = flag.String("topic", "", "the topic you wish to send the message to")
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
	if topic != nil {
		result.topic = *topic
	}

	return result
}
