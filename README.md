# Sendit ✉️
A CLI tool for sending message(s) to different message brokers using bodies and headers from a file. 

![](https://github.com/willdot/sendit/blob/main/vhs/sendit.gif)

## Use cases 🤔

1. You want to send 100 messages to see how your services that consume the messages handle them. Perhaps you want to test idempotency or just see how your service handles the load.

1. You want to see how your service handles a specific message body or header really quickly without having to find where you have tests written that you can copy / alter.

1. You have a really complex microservice setup and you want to trigger some behaviour from a message, but don't want to go through the hassle of setting other things up to do it "the right way". (For example a cache is cleared somewhere when a message is consumed)

These are just examples of when I've wanted a tool that could do this.

## Installation 🛠️

``` sh
go install github.com/willdot/sendit@latest
```

## Useage 🧭
Basic useage. See different broker sections for broker specific details.

``` sh
sendit -body="body.json" -headers="headers.json" -url="localhost:1234" -repeat=3
```
* body (required) - the path to a file containing the data you wish to send as the body of the message
* headers(optional) - the path to a file containing the headers you wish to send with the message (each broker has it's own specification for how they should be provided)
* url(optional) - the url of the server to send the message to (each broker has a default which is the default for using locally)
* repeat(optional) - the number of time you wish to send the message (default is 1)

### RabbitMQ
You can either send directly to a queue OR to an exchange. You will be asked to select which option when you run the tool.

``` sh
sendit -body="body.json" -destination="test"
```
* destination(required) - the name of the queue or exchange to send the message to.

Headers should be in JSON format in a key / value format. eg:
``` json
{
    "header1" : "value1",
    "header2" : "value2"
}
```

### NATs
``` sh
sendit -body="body.json" -subject="test"
```
* subject(required) - the subject you wish to use for the message

Headers should be in JSON format in a key / array string format. eg:
``` json
{
    "header1" : ["value1", "value2"],
    "header2" : ["value3"]
}
```

### Redis
``` sh
sendit -body="body.json" -channel="test"
```
* channel(required) - the channel you wish to publish the message to

Note: Redis does not support headers.

## Contributing 🤝

Issues and PRs welcome.

To run the tests you will need to run `docker-compose up` to get the message broker servers running.